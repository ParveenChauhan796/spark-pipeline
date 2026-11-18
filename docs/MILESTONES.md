# MILESTONES.md

Build order for the multi-vendor product catalogue pipeline. Work one milestone at a time.
Budget: ~7.5 hours plus buffer, assuming Claude Code writes the boilerplate and the developer
spends time reading outputs and understanding them.

**Status legend:** `[ ]` not started, `[x]` done (tests green, summary delivered).

**Cut order if behind schedule:** M5c label propagation, then the second blocking pass, then the
stretch report. Never cut tests or measurements.

---

## M0: Setup (30 min)
- [x] Python 3.11+ and Java 17 verified (stop and report if not)
- [x] Compatible PySpark and delta-spark pair pinned (check Delta's compatibility table; default
      Spark 3.5.x with delta-spark 3.x) and reported with the reason
- [x] `pyproject.toml` with approved dependencies only (pyspark, delta-spark, pytest, pyyaml, requests)
- [x] `get_spark()` helper with Delta extension and catalog; `config.py`; `paths.py`; `conf/pipeline.yaml`
- [x] Shared pytest Spark fixture (local[2], shuffle.partitions=2, UI off, temp dir)
- [x] Smoke test: write a Delta table, append once, read back, assert 2 versions via `DESCRIBE HISTORY`
- [x] Makefile with `setup` and `test` targets; `.gitignore`

*Spark concept: SparkSession is the entry point. In local mode your laptop is the whole "cluster",
with threads acting as workers. The first Delta session downloads jars from Maven Central.*

**Done when:** `make test` is green.

---

## M1: Data generation (60 min)
**Dataset gate first (see CLAUDE.md), before any downloader code:**
- [x] Name the public dataset and the fields planned
- [x] Inspect a few real records; do not assume field names
- [x] Check and quote the license/terms; flag redistribution or non-research restrictions
- [x] If anything fails, propose a fallback and WAIT for the developer's choice

Default candidate: Amazon Reviews 2023 item metadata (Hugging Face), one category, ~100k records
(stream the first ~100k lines rather than downloading everything). Fallback: Open Food Facts.
Barcodes, prices and stock are synthetic either way.

**Generation (plain seeded Python, standard library only; Spark is for the pipeline):**
- [x] Golden catalogue: assign `true_product_id`, synthesize GTINs for ~60% of products, plus
      brand, price and stock
- [x] 4 vendor feeds x 3 daily batches, each vendor with a different schema:
  - Vendors A and B: **CSV**. Vendors C and D: **JSON**.
- [x] Day 1 is a full snapshot; days 2-3 are change feeds (changed and new items, plus some
      unchanged rows re-sent)
- [x] Injected mess: typos, case and whitespace, brand aliases, word-order shuffles, brand baked
      into the title, locale-formatted prices (`"$1,299.00"`, `"1.299,00"`), missing values,
      null SKUs, in-file duplicates
- [x] Price and stock changes on ~10% of offers per batch
- [x] Injected drift:
  - Day 2: vendor A (CSV) adds a new column (ADDED)
  - Day 3: vendor B (CSV) renames `title` to `name`; the alias is **declared in B's contract**
    (RENAMED, batch processes normally)
  - Day 3: vendor C (JSON) changes `price` from number to string (TYPE_CHANGED)
- [x] Separate `ground_truth` file mapping (`vendor_id`, `vendor_sku`) to `true_product_id`
- [x] Deterministic generation (seeded); a tiny-n test passes

**Done when:** files exist under `data/landing/<vendor>/<YYYY-MM-DD>/`, generation is
deterministic, and the tiny-n test passes.

---

## M2: Bronze ingest and drift detection (60 min)
- [x] Vendor contract YAMLs in `conf/vendors/`: format, read options, expected columns. Per column:
      source name, canonical name, type, required (yes/no), optional `aliases`
- [x] Contract loader fails if one alias appears on more than one column
- [x] Drift detector as a **pure function**: (observed schema, contract) -> (drift list, rename
      mapping, batch verdict). Alias matching is case-insensitive and trimmed
  - ADDED (not in contract, not an alias): log, ignore downstream
  - RENAMED (alias matches, expected name absent): log RENAMED, map to canonical column, process normally
  - Both expected name and alias present: expected name wins, alias logged as ADDED
  - MISSING optional (no alias match): null + warn
  - MISSING required (no alias match): **quarantine the batch**
  - TYPE_CHANGED: **JSON vendors only**; log + warn. CSV has no type drift (bad values are caught
    in silver)
- [x] Reads: CSV all strings. JSON may infer a schema ONLY to feed the drift detector; that inferred
      schema never defines a table
- [x] Bronze write: one Delta table per vendor; contract columns under canonical names, all scalars
      cast to string (nested JSON structs stay nested); lineage columns `_vendor_id`, `_batch_id`
      (format `YYYY-MM-DD`), `_source_file`, `_ingested_at`; **`batch_date` = `to_date(_batch_id)`**;
      **partitioned by `batch_date`**
- [x] Quarantined batch = not written to bronze, logged in `ops.batch_log` with status `QUARANTINED`,
      files stay in landing, other vendors continue
- [x] `ops.batch_log` and `ops.schema_drift_log` (ADDED | MISSING | RENAMED | TYPE_CHANGED)
- [x] Idempotency: skip batches already `SUCCESS`; per vendor, reject batches older than that
      vendor's last `SUCCESS` batch

*Spark concept: lazy evaluation. Transformations (select, filter, join) only build a plan; nothing
runs until an action (count, write, show). Also why CSVs are read as all strings: schema inference
costs an extra pass and guesses wrong on messy feeds.*
*Spark concept: on-disk partitioning. `partitionBy("batch_date")` creates one folder per date; a
query filtering on one date reads only that folder.*

**Tests:** drift detector unit tests for ADDED, declared RENAMED, MISSING optional, TYPE_CHANGED
(JSON), and **unknown rename of a required column -> batch quarantined**.

**Done when:** running days 1-3 produces at least these drift rows: day 2 A = ADDED, day 3 B =
RENAMED, day 3 C = TYPE_CHANGED; and the unknown-rename test passes. (An unknown column that is
still present in a later batch is logged ADDED again, so A's `rating` also appears on day 3.)

---

## M3: Silver normalization (45 min)
- [x] Parse prices with `regexp_replace`/`when` (no UDFs), handling `"$1,299.00"` and `"1.299,00"`
- [x] Normalize titles (`title_norm`, `title_tokens`), brands (via alias table), categories
- [x] Pad barcodes to `gtin14`
- [x] Derive `stock_status`: qty 0 -> OUT, <=5 -> LOW, else IN
- [x] Route bad rows to `silver.quarantine` with a `reason` (null SKU, unparseable or non-positive
      price, other parse failures). This is where CSV type problems surface
- [x] Dedupe within a batch (latest row per key)
- [x] Write `silver.vendor_product` (latest values, key `vendor_id` + `vendor_sku`) and
      `silver.vendor_offer` (one row per offer per batch)

*Spark concept: built-ins vs UDFs. A Python UDF ships rows one at a time to a Python process and is
opaque to Spark's optimizer. Built-in functions stay inside the JVM and are optimized.*
*Spark concept: window functions. They compute a value per row using related rows without collapsing
them (unlike `groupBy`). `row_number() over (partition by key order by ts desc)` followed by keeping
row 1 means "latest row per key".*
*Spark concept: joins and partitions. Spark splits each DataFrame into partitions processed in
parallel. A join normally shuffles (redistributes rows between partitions by key), which is the
expensive step. If one side is tiny, like the brand alias table, a broadcast join copies it to every
worker and skips the shuffle.*

**Done when:** `rows_in == valid + quarantined + deduped` for every batch, and price-parser tests pass.

---

## M4: SCD2 with incremental loads (75 min)
- [x] `gold.offer_history`: `offer_key` (`vendor_id|vendor_sku`), `price`, `stock_status`,
      `row_hash`, `valid_from`, `valid_to` (NULL while current, half-open interval), `is_current`
- [x] Change detection via `row_hash` (sha2 over price + stock_status)
- [x] Delta `MERGE` with a staged source: each changed row appears twice, once with the real key
      (matches and closes the old version) and once with a NULL key (never matches, so inserts the
      new version). New offers don't match and insert. Unchanged rows match but fail the
      hash-differs condition, so nothing happens
- [x] Process batches in date order; skip batches already `SUCCESS` in `ops.batch_log`

*Spark concept: SCD2 (slowly changing dimension type 2) keeps history by closing the old row and
inserting a new version instead of overwriting. Delta's `MERGE` is an atomic upsert: matching rows
are updated and the rest inserted in one transaction. Parquet files cannot be updated in place,
which is why this project uses Delta.*

**Tests:** new, changed, unchanged, and replayed-batch scenarios. Invariants: exactly one current
row per `offer_key`, no overlapping intervals, replaying a batch is a no-op.

**Done when:** all tests pass. Bonus: inspect `DESCRIBE HISTORY` to see Delta's versions.

---

## M5: Entity resolution (75 min)
*Spark concept: a self-join compares a DataFrame to itself. Comparing all ~240k records pairwise is
~30 billion pairs, which is why blocking is needed: only compare records that share a cheap key.*

- [x] **5a: Tier 1.** Exact match on `gtin14` produces edges with `match_method='GTIN'`
- [x] **5b: Tier 2.** Blocked fuzzy matching:
  - Blocking pass 1: (brand, model-number token). Pass 2 (cut if behind): (brand, first title tokens)
  - Cross-vendor pairs only, using `a.key < b.key` to avoid duplicate pairs
  - Score with a weighted blend of token Jaccard (`array_intersect`/`array_union`), `levenshtein`
    ratio, and price closeness. Built-ins only. Tune keys and weights against ground truth
- [x] **5c: Cluster.** Fixed 3-round label propagation: each round, every record adopts the smallest
      key among itself and its matched neighbours (join plus `groupBy min`). Approximate when
      clusters have more than one record per vendor; evaluation shows how much
- [x] **5d: Golden record.** Window function picks the most complete record per master, with a
      vendor priority tiebreak. Write `gold.product_xref` and `gold.dim_product`
- [x] **5e: Evaluate.** Pairwise precision and recall vs `ground_truth`, logged to `ops.er_run_log`
      (records, candidate pairs, edges, masters, precision, recall, seconds). Target precision >= 0.95

**Tests:** exact match, typo positive, different-model negative, 3-record chain.

**Done when:** metrics are logged and tests pass.
**If behind:** keep 5a, 5b with a single blocking pass, 5d and 5e; skip 5c.

---

## M6: Reports (30 min)
- [x] `gold.rpt_attribute_completeness`: `count(col)/count(*)` generated for all attribute columns,
      per vendor and for the merged catalogue
- [x] `gold.rpt_price_changes_by_brand`: `lag()` over `offer_history` per `offer_key` to get the
      previous price, join to xref and master brand, aggregate per brand per day (changes, ups,
      downs, average % change)
- [x] Stretch: `rpt_price_spread` (cross-vendor min/max price per master product)

**Done when:** tests on a tiny fixture match hand-computed values.

---

## M7: Optimize and measure (45 min)
*Spark concepts: `explain()` shows the physical plan (look for Exchange = shuffle and
BroadcastHashJoin). The Spark UI at `localhost:4040` shows stages, shuffle sizes and skew.
`spark.sql.shuffle.partitions` defaults to 200, far too many for local small data. AQE (adaptive
query execution) can adjust plans at runtime.*

- [x] **E1:** brand-only blocking vs final blocking. Report candidate pairs, pair recall, runtime
- [x] **E2:** shuffle partitions 200 vs ~8, plus broadcast on the alias table, for a full run
- [x] `make bench` writes before/after tables and machine specs to `docs/RESULTS.md`

**Done when:** before/after tables are filled in.

---

## M8: README (30 min)
- [ ] Mermaid architecture diagram
- [ ] Quickstart
- [ ] Results table (headline numbers below)
- [ ] Sample reports
- [ ] Limitations section (see below)

**Done when:** `make setup && make data && make run-all && make test` works from a fresh clone.

---

## Headline numbers to report
Rows processed per stage and rows/sec; records collapsed into masters (e.g. ~240k vendor records to
~100k masters); ER precision and recall; candidate-pair reduction; runtime before/after
optimization; machine specs.

## Known limitations (document in README)
- Single currency (USD); no FX
- No delisted-product handling; no out-of-order or late batches
- A quarantined day cannot be replayed after a later day succeeds for that vendor
- Master IDs are rebuilt each run (hash of anchor key) and can shift as new records arrive;
  production would persist the crosswalk and match new records against existing masters first
- Ground truth is optimistic: the noise was written by us, so metrics measure detection of our own
  noise types
- Local-mode numbers are laptop numbers; no cluster-scalability claims
- Stock history tracks status (IN/LOW/OUT), not raw quantity

## Out of scope
Streaming, orchestration (Airflow), Docker/cloud, ML/embedding matching, multi-currency/FX,
delisted-product handling, out-of-order batches, dashboards, Great Expectations,
image/description processing.
