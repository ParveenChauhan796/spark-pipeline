# DECISIONS.md

Design decisions that change how the pipeline behaves. This file is meant as a to-do list for
revisiting things once the prototype works: each entry says what was chosen, why, what the
alternative would be, and where to change it.

**Rules for adding entries (for every session):**
- Add an entry whenever a choice changes observable behaviour: what data is kept, rejected or
  merged; how a table is keyed, typed or laid out; what counts as a change, a match or an error;
  or what a re-run does. Pure code-style choices don't belong here.
- Number entries `D<n>` in order, and never renumber. If a decision is reversed, keep the old entry,
  mark it `Superseded by D<m>`, and add a new entry.
- `Status`: `Proposed` means not yet approved by the developer, and `Accepted` means approved.
  Record the date it was accepted.
- Keep it short: 3 to 6 lines per entry.

---

## M1: Data generation

### D1. Source dataset: Amazon Reviews 2023, Appliances category
- **Status:** Accepted 2026-10-26
- **Decision:** ~100k item-metadata records from one category. Barcodes, prices and stock are synthetic.
- **Why:** Model numbers and long titles suit fuzzy matching in M5. The dataset states no license,
  so it's used for private local learning only: data/ is never committed or shared.
- **Alternative:** Open Food Facts (explicit ODbL license, but short names and no model numbers).
- **Where:** `conf/pipeline.yaml` under `generate.source`.

### D2. The brand reference table is built from the golden catalogue
- **Status:** Accepted (M1/M3)
- **Decision:** `data/reference/brand_aliases.csv` comes from the same generator that produces the truth.
- **Why:** There's no real brand master available for synthetic vendors.
- **Effect:** ER evaluation in M5 is optimistic, because brand normalization "knows" the true aliases.
- **Alternative:** Build aliases only from observed vendor brands (frequency/edit distance), or hand-curate them.
- **Where:** `generate/`, `silver.brand_aliases_path`.

## M2: Bronze and drift

### D3. An ADDED column is logged on every batch where it appears
- **Status:** Accepted (M2)
- **Decision:** Vendor A's `rating` is logged ADDED on day 2 and again on day 3. Drift is checked
  per batch against the contract, with no memory of earlier batches.
- **Alternative:** Log it only the first time (needs a lookup into `ops.schema_drift_log`), or add it to the contract.
- **Where:** `ingest/drift.py`, `ingest/bronze.py`.

### D4. Stale batches are rejected per vendor
- **Status:** Accepted (from CLAUDE.md)
- **Decision:** A batch older than the vendor's last SUCCESS batch gets `REJECTED_STALE`.
  A quarantined day can't be replayed after a later day succeeds.
- **Alternative:** Support out-of-order loads (out of scope; it would need SCD2 to insert into the middle of history).
- **Where:** `ingest/bronze.py` (`ingest_batch`), `ops.last_success`.

### D5. Bronze writes use `replaceWhere` per (vendor, batch_date)
- **Status:** Accepted (M2)
- **Decision:** Re-ingesting a batch atomically replaces its rows, so it never appends twice.
- **Where:** `ingest/ops.py` (`replace_where`), `ingest/bronze.py`.

## M3: Silver

### D6. Ambiguous bare prices like `1.299` and `1,299`
- **Status:** Accepted (M3), known caveat
- **Decision:** Parsing follows fixed regex rules (see `transform/price.py`). A bare number with
  one separator and 3 decimals can't be disambiguated without knowing the vendor's locale.
- **Alternative:** A per-vendor `decimal_separator` in the contract YAML.
- **Where:** `transform/price.py`.

### D7. Within-batch duplicates: the latest row per key wins
- **Status:** Accepted (M3)
- **Decision:** `row_number()` per (vendor_id, vendor_sku) ordered by file position. Older duplicates are
  counted as `deduped`, not quarantined.
- **Where:** `transform/silver.py` (`classify`).

### D8. `silver.vendor_product` never moves backwards in time
- **Status:** Accepted (M3)
- **Decision:** The MERGE updates a product only if `s.last_batch_date >= t.last_batch_date`.
- **Where:** `pipeline.py` (`_upsert_products`).

### D9. Delta snapshotPartitions is left at the default (50) in production
- **Status:** Accepted (M3). Tests use 2. M7 measures the effect.
- **Where:** `tests/conftest.py`, `spark.py`.

## M4: SCD2 offer history

### D10. A missing stock_status means "unknown", not a change
- **Status:** Accepted 2026-11-17
- **Decision:** When a batch has no qty for an offer, its previous known stock_status is carried
  forward, so history doesn't flip `IN -> NULL -> IN` (about 2% of rows have no qty). A brand-new
  offer with no qty starts with NULL until a real value arrives.
- **Alternative:** Treat NULL as a real state (more versions, noisier change reports in M6).
- **Where:** `history/scd2.py` (`classify_changes`).

### D11. `valid_from` and `valid_to` are DATEs, using half-open intervals
- **Status:** Accepted 2026-11-17
- **Decision:** `valid_from` = batch date. A version is valid for `[valid_from, valid_to)`, and
  `valid_to` is NULL while the version is current.
- **Alternative:** Timestamps (needed if batches ever arrive more than once a day).
- **Where:** `history/scd2.py`.

### D12. Same offer and same day loaded again: the first load wins
- **Status:** Accepted 2026-11-17
- **Decision:** History only accepts a row whose batch date is strictly newer than the offer's
  current `valid_from`. Rows for a day already in history are counted as `stale` and ignored.
  This also makes replays and crash retries harmless. Correcting an already-loaded day isn't supported.
- **Alternative:** Allow corrections (delete that day's versions, reopen the previous version, then re-apply).
- **Where:** `history/scd2.py` (`classify_changes`).

### D13. `run_all` returns `BatchResult(bronze, silver, history)` per batch
- **Status:** Accepted 2026-11-17
- **Decision:** A dataclass replaces the old `(bronze, silver)` tuples. Later stages add fields here.
- **Where:** `pipeline.py`.

### D14. An offer that's absent from a later batch is unchanged, not delisted
- **Status:** Accepted 2026-11-17
- **Decision:** Days 2 and 3 are change feeds, so a missing offer keeps its current version open.
- **Alternative:** Delisting (out of scope; full-snapshot feeds would be needed to tell "absent" from "unchanged").
- **Where:** `history/scd2.py` (nothing is closed without a new version).

### D15. `gold.offer_history` is not partitioned
- **Status:** Accepted 2026-11-17
- **Decision:** About 330k narrow rows (a few MB). The MERGE looks up current rows by `offer_key`, which no
  partition column could prune, and partitioning would only create small files.
- **Scaling path:** Z-order or liquid clustering on `offer_key` first. Then split the table into current and
  archive tables. Use a coarse date partition only at terabyte scale. M7 may measure `OPTIMIZE ZORDER BY offer_key`.
- **Where:** `pipeline.py` (`history_batch`).

### D16. The history table also stores `vendor_id` and `vendor_sku`
- **Status:** Accepted 2026-11-17
- **Decision:** Besides the spec columns, these are kept so per-vendor reads and M6 joins don't
  have to split `offer_key`.
- **Where:** `history/scd2.py` (`HISTORY_COLUMNS`).

### D17. Unchanged rows are dropped while staging, before the MERGE
- **Status:** Accepted 2026-11-17
- **Decision:** The staged source contains only NEW and CHANGED offers, which keeps the MERGE small.
  The MERGE still has a `row_hash` differs condition as a safety net.
- **Where:** `history/scd2.py` (`stage_merge`), `pipeline.py` (`_merge_history`).

### D18. A crash retry records different log counts
- **Status:** Accepted 2026-11-17
- **Decision:** If the process dies after the MERGE but before `ops.history_log` is written, the
  retry finds the rows already applied. They are counted as `stale`, not `new`/`changed`. The history table
  is identical; only that batch's log counts differ from a clean run.
- **Alternative:** Read the MERGE's own metrics from `DESCRIBE HISTORY` on retry.
- **Where:** `pipeline.py` (`history_batch`).

## M5: Entity resolution

### D19. ER recomputes everything each run, from the latest silver values
- **Status:** Accepted 2026-11-17
- **Decision:** ER reads all of `silver.vendor_product` (217k records), plus each offer's current price
  from `gold.offer_history`. It overwrites `gold.product_xref` and `gold.dim_product` in full, and runs once
  after the batch loop rather than per batch.
- **Why:** At this size a full run takes about a minute, and incremental ER (matching only new records
  against existing masters) is a project of its own.
- **Alternative:** Incremental ER: block only new or changed records against the existing master records.
- **Where:** `pipeline.py` (`resolve_entities`).

### D20. Only cross-vendor pairs can match, GTIN included
- **Status:** Accepted 2026-11-17
- **Decision:** Two records from the same vendor are never linked directly. This assumes a vendor lists
  each product once. They can still end up in one cluster through other vendors.
- **Alternative:** Allow same-vendor edges to catch a vendor's own duplicate listings.
- **Where:** `match/candidates.py`.

### D21. Blocking pass 1 uses (brand, model-like token), and brandless records skip fuzzy matching
- **Status:** Accepted 2026-11-17
- **Decision:** A record's tokens are its normalized model plus any title token containing both letters
  and digits, at least `er.blocking.min_token_length` long. Vendor C has no model column, but its titles
  carry the model in about 56% of rows. Records with no brand (about 5%) only match through GTIN.
- **Alternative:** Block on the token alone, with brand as a veto (gains recall on brandless records,
  but blocks like `120v` get huge).
- **Where:** `match/candidates.py`, `er.blocking` in `conf/pipeline.yaml`.

### D22. Oversized blocks are dropped
- **Status:** Accepted 2026-11-17
- **Decision:** Blocks with more than `er.blocking.max_block_size` records are skipped, because they come
  from generic tokens and comparing every pair in them grows quadratically. The number dropped is logged
  in `ops.er_run_log`.
- **Where:** `match/candidates.py`.

### D23. Fuzzy score = weighted blend, tuned on the ground truth
- **Status:** Accepted 2026-11-17
- **Decision:** `w1*token_jaccard + w2*levenshtein_ratio + w3*price_closeness >= threshold`. Weights and
  threshold live in `er.scoring` and are tuned against `ground_truth` (there's no holdout, so the
  reported precision and recall are optimistic; see also D2). Tuned values: weights 0.35 / 0.25 / 0.40,
  threshold 0.65. The first guess (0.45 / 0.35 / 0.20, threshold 0.75) gave precision 0.832 and recall 0.807.
- **Alternative:** Hold out one vendor pair, or half the products, for evaluation.
- **Where:** `match/score.py`, `er.scoring`.

### D24. A different model number or GTIN vetoes a fuzzy match
- **Status:** Accepted 2026-11-17
- **Decision:** If both records have a model and the normalized models differ, or both have a `gtin14`
  and they differ, the pair is never a fuzzy match, whatever the score. This favours precision.
- **Why:** Profiling showed models are identical across vendors for the same product (0 of 46k differ).
- **Where:** `match/score.py`.

### D25. Clusters come from 3 rounds of label propagation
- **Status:** Accepted 2026-11-17
- **Decision:** Each record repeatedly takes the smallest record key among itself and its neighbours.
  Chains longer than 3 hops may stay split, and one bad edge merges two products. Clusters holding two
  records from the same vendor are counted in `ops.er_run_log` as a sign of that.
- **Known failure (tested):** a record with no model can bridge two records whose models differ (A-C
  and C-D pass, A-D is vetoed), so A and D end up in one cluster. See
  `test_known_limitation_a_record_without_model_can_chain_two_models`.
- **Alternative:** Iterate until nothing changes (exact connected components), or use GraphFrames
  (a new dependency, not approved). A cluster-level veto (refuse a merge that would put two different
  models or GTINs in one cluster) would fix the failure above.
- **Where:** `match/cluster.py`, `er.label_propagation_rounds`.

### D26. `master_id` = the smallest record key in the cluster
- **Status:** Accepted 2026-11-17
- **Decision:** `master_id` is the smallest `vendor_id|vendor_sku` in a cluster. It's deterministic for
  the same input, but it can change when a new record with a smaller key joins.
- **Alternative:** A persistent master-id mapping table that carries ids over between runs.
- **Where:** `match/cluster.py`.

### D27. Golden record: most complete record, then vendor priority
- **Status:** Accepted 2026-11-17
- **Decision:** Per master, the record with the most non-null fields among brand, category, model and
  `gtin14` wins. Ties go to vendor priority (`er.golden.vendor_priority`), then to the smallest key. The
  whole golden record is taken from that one record; nothing is mixed in from others.
- **Alternative:** Field-level survivorship (best value per attribute from any record in the cluster).
- **Where:** `match/golden.py`.

### D28. `product_xref.match_method` is the strongest way a record was linked
- **Status:** Accepted 2026-11-17
- **Decision:** `GTIN` if the record has any GTIN edge, else `FUZZY` if it has a fuzzy edge, else
  `SINGLETON`. Records linked only through clustering keep the method of their own edges.
- **Where:** `match/golden.py`.

### D29. An ER re-run is skipped when its inputs and config haven't changed
- **Status:** Accepted 2026-11-17
- **Decision:** `ops.er_run_log` records the Delta versions of `silver.vendor_product` and
  `gold.offer_history` that a run read, plus a sha256 of the `er` config section. If a logged run has
  the same three values, ER is skipped (`er --force` re-runs anyway). A re-run overwrites the gold tables
  with identical output, because everything is deterministic.
- **Where:** `pipeline.py` (`resolve_entities`), `ingest/ops.py`.

### D30. Prices too far apart never match
- **Status:** Accepted 2026-11-17 (added during tuning)
- **Decision:** A fuzzy pair needs `price_closeness >= er.scoring.min_price_closeness` (0.85). The source
  has many different products with identical titles and models (duplicate Amazon listings), and only
  price tells them apart. True pairs: 99% have closeness >= 0.81. Wrong pairs: median 0.16.
- **Effect:** Cluster precision went from 0.83 to 0.95 together with the reweighting in D23.
- **Caveat:** This relies on synthetic prices: vendor spread <= 10% plus up to 15% drift over three days. Real
  feeds with sales or bundles would need a looser gate.
- **Where:** `match/score.py` (`accepted`).

### D31. Keep only each record's best match per other vendor
- **Status:** Accepted 2026-11-17 (added during tuning)
- **Decision:** An edge survives only if it is the best-scoring edge for both of its records into the
  other's vendor. GTIN edges rank above any fuzzy edge, and ties go to the smaller key. This builds on D20
  (a vendor lists each product once).
- **Effect:** Cluster precision went from 0.952 to 0.989 with recall unchanged (0.914 -> 0.913), because it
  stops the chains of wrong edges that merged clusters.
- **Alternative:** Turn it off with `er.scoring.best_match_per_vendor: false`.
- **Where:** `match/score.py` (`best_match_per_vendor`, `combine_edges`).
