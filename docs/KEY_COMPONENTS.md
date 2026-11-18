# Key components

The ten pieces that do the real work, in pipeline order. For each one: what it does, how it works, and
why it matters. Paths are relative to `src/catalog_pipeline/`.

---

## 1. Drift detection: `ingest/drift.py` → `detect_drift(observed, contract)`
**Role:** gatekeeper for every incoming file.

- **Inputs:** the columns actually found in the file (`{name: type}`) and the vendor's YAML contract.
- **How:**
  1. Match the expected names (trimmed, case-insensitive).
  2. For any still missing, try the declared aliases.
  3. Classify each remaining column as ADDED, MISSING or TYPE_CHANGED (JSON only).
- **Output:** a list of drifts, a rename mapping (source column to canonical column) and a verdict:
  QUARANTINE if a *required* column is missing with no alias, otherwise OK.
- **Why it matters:** it's pure Python with no Spark, so every rule is unit-tested in milliseconds. It's
  also the reason a vendor renaming `title` to `name` doesn't break anything.

## 2. Bronze ingest: `ingest/bronze.py` → `ingest_batch(spark, config, contract, batch_id)`
**Role:** the first durable copy of the data.

- **How:**
  1. Skip if `ops.batch_log` already says SUCCESS. Reject a day older than the vendor's last success.
  2. Detect drift. If the verdict is QUARANTINE, log it and stop (other vendors continue).
  3. Otherwise read with an explicit all-string schema, rename to canonical names, and add lineage
     (`_batch_id`, `_source_file`, `_ingested_at`).
  4. Write with `replaceWhere batch_date = ...` into a table partitioned by `batch_date`.
- **Why it matters:** bronze is "what the vendor sent", so any later bug can be fixed by re-running silver,
  without asking vendors for files again.

## 3. Silver cleaning: `transform/silver.py` → `classify(df, aliases, config)`
**Role:** turns strings into trustworthy typed values.

- **How:** one pass computes everything:
  - `price_usd` (`parse_price` handles US and European formats with regex)
  - `qty_int` and `stock_status`
  - normalized title and tokens
  - GTIN-14
  - canonical brand (a **broadcast join** to the alias table)
  - `reason` (the first failing check, or null)
  - `rn` (a window ranking duplicates)
- **Then:** `to_offers` / `to_products` / `to_quarantine` slice the same frame, and `batch_counts` checks
  that `rows_in == valid + quarantined + deduped`. `pipeline/silver.py` (`silver_batch`) raises if it doesn't.
- **Why it matters:** every row is accounted for, and bad rows are kept with a reason, never dropped.

## 4. SCD2 change detection: `history/scd2.py` → `classify_changes` + `stage_merge`
**Role:** decides what changed since yesterday, without touching the table.

- **`classify_changes`:** left-joins today's offers to the current history rows and compares a
  `row_hash` (sha2 of price | stock_status). Each offer becomes one of:

  | Type | Meaning |
  |---|---|
  | NEW | never seen before |
  | CHANGED | hash differs |
  | UNCHANGED | hash is the same |
  | STALE | not newer than the current version, so this is a replay |

  Missing stock carries the last known value forward.
- **`stage_merge`:** builds the MERGE source. A CHANGED offer appears **twice**:
  - with `merge_key = offer_key`, which matches the current row, so the MERGE closes it
  - with `merge_key = NULL`, which never matches, so the MERGE inserts the new version
- **Why it matters:** this staged-source trick lets one atomic MERGE both close and open versions.

## 5. SCD2 write: `pipeline/history.py` → `history_batch` / `_merge_history`
**Role:** applies one batch to `gold.offer_history`, safely.

- **How:**
  1. Read the current rows **pinned to a table version** (`versionAsOf`), so recomputation during the
     MERGE can't see half-new data.
  2. Classify, then check that the counts reconcile.
  3. Run one MERGE: matched current rows get `valid_to` and `is_current=false`, and NULL-key rows are
     inserted as current.
  4. Log to `ops.history_log`.
- **Why it matters:** a re-run is skipped, a replay is STALE, and a crash commits nothing.
  `scd2.invariant_violations` proves the result: one current row per offer, no gaps, no overlaps.

## 6. Blocking: `match/candidates.py` → `block_key_passes` + `block_pairs`
**Role:** cuts about 23 billion possible comparisons down to about 400 thousand.

- **How:** each pass gives every record some block keys:
  - **Pass 1:** (brand, model-like token). Tokens with letters *and* digits, ≥4 characters, plus the
    model field. Built with `explode` and a higher-order `F.filter`, no UDF.
  - **Pass 2:** (brand, first two title words after removing the brand).

  `block_pairs` self-joins records sharing a key, keeps only cross-vendor pairs once
  (`a_key < b_key`), and **drops blocks larger than 50** (generic tokens).
- **Measured (E1, `docs/RESULTS.md`):** brand-only blocking would mean 262.6M pairs uncapped. With the
  cap it finds only 29% of true pairs. The final blocking finds 85% (92% with GTIN) from 403k pairs.

## 7. Scoring: `match/score.py` → `score_pairs`, `accepted`, `best_match_per_vendor`
**Role:** decides whether a candidate pair is the same product.

- **Score:** `0.35·token Jaccard + 0.25·Levenshtein ratio + 0.40·price closeness`.
- **Rules applied on top:**
  - **Vetoes:** different model numbers, or different GTINs, mean never a match.
  - **Price gate:** prices more than 15% apart never match.
  - **Threshold:** 0.65.
  - **Best match per vendor:** keep an edge only if each side is the other's best match within that
    vendor. This kills duplicate-listing confusion.
- **Combined with GTIN pairs (score 2.0)** by `combine_edges`.
- **Why it matters:** this is where precision is won. It went from 0.83 to 0.99 through the price gate,
  reweighting and best-match (D23, D30, D31).

## 8. Clustering: `match/cluster.py` → `label_propagation(nodes, edges, rounds)`
**Role:** turns pairwise matches into groups (A=B and B=C means A, B and C are one product).

- **How:** every record starts labelled with its own key. In each round it takes the minimum label
  among itself and its neighbours. After 3 rounds, records within 3 hops share a label (the master_id).
  `localCheckpoint()` after each round keeps the query plan small.
- **Limitation:** a record with no model number can bridge two products (documented in D25 and covered
  by a test).

## 9. Golden record and ER orchestration: `match/golden.py` + `pipeline/er.py` (`resolve_entities`)
**Role:** produces the final catalogue and measures it.

- **`build_xref`:** maps record to master_id (singletons get their own).
- **`build_dim`:** a window per master picks the most complete record (brand, category, model and
  gtin14 filled in), then breaks ties by vendor priority A > B > D > C, then by key.
- **`resolve_entities`** runs the whole chain on version-pinned inputs:
  1. prepare, then GTIN pairs, then blocking, then scoring, then clustering, then golden
  2. `localCheckpoint()` between steps (this fixed an out-of-memory error)
  3. overwrite xref and dim, then evaluate against ground truth (`evaluate.py`, pair counting with
     n(n-1)/2), then log to `ops.er_run_log`
  4. skip next time if the input versions and the **config hash** are unchanged
- **Result:** precision 0.9891, recall 0.9133, 99,878 masters.

## 10. Orchestration and idempotency: `pipeline/run.py` (`run_all`) + `ingest/ops.py`
**Role:** runs everything in the right order and makes re-runs harmless.

- **`run_all`:** discovers landed batches, oldest day first. For each vendor batch it runs bronze,
  then silver, then history, and a quarantined batch only stops *that* vendor's batch. The CLI then
  runs ER and reports.
- **`ops.py`:** every log write is `replaceWhere` on (vendor, batch), and the `*_done` checks let
  each stage skip finished work.
- **Why it matters:** `make run-all` twice gives identical tables. The integration tests check exactly
  this.

---

## Supporting pieces worth knowing
- **`spark.py` → `get_spark`:** the single place Spark is configured (shuffle partitions 8 instead of 200,
  4g heap, Delta extension).
- **`reports/reports.py`:**
  - `unpivot` turns the wide completeness counts into long rows
  - `lag()` finds price changes
  - a join through xref gives cross-vendor spread
- **`bench.py`:** fair benchmarking, with one fresh process and one empty lake per variant. The plan is
  inspected to prove which join strategy actually ran.
