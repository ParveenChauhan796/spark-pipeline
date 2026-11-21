# CLAUDE.md

## Project
Multi-vendor product catalogue pipeline: PySpark + Delta Lake. One-day learning project.
Ingest messy vendor feeds (CSV/JSON) -> detect schema drift -> normalize -> SCD2 price/stock
history with incremental loads -> cross-vendor entity resolution -> reporting tables.
Public + synthetic data only.

## About the developer
Limited Spark experience. The first time you use a Spark concept (window function, join
strategy/broadcast, partitioning/shuffle, MERGE, AQE, caching), add a 2-4 line docstring or
comment saying what it does and why it's used here. After each milestone, give a short
"what Spark actually did" summary (point at explain() output or the Spark UI where relevant).
When something breaks, explain the cause in plain terms BEFORE fixing it.

## Hard rules
- DataFrame API and Spark SQL only. No Python UDFs or pandas UDFs. If one seems unavoidable, stop and ask.
- No inferSchema. Use explicit schemas, or all-strings for raw reads.
  Single exception: JSON reads in ingest/ may infer a schema ONLY to get the observed schema for
  drift detection. That inferred schema never defines a table.
- No .collect() / .toPandas() on anything but tiny results (reports for printing, tests).
- Transform functions are pure: `def f(df: DataFrame, ...) -> DataFrame`. I/O lives only in ingest/ and the
  pipeline/ package (one orchestration module per stage; no I/O in transform/, history/, match/, reports/).
  The standalone tools generate/ (writes the synthetic feeds) and bench.py (benchmark lakes, docs/RESULTS.md)
  do their own I/O and are never imported by pipeline code.
- No hardcoded paths or thresholds: use conf/pipeline.yaml, conf/vendors/*.yaml, and paths.py.
- Every batch run is idempotent. Re-running a batch must not change results.
- Never commit data/. Seeded randomness everywhere (generation must be deterministic).
- Dependencies: approved without asking: pyspark and delta-spark (pinned, compatible pair), pytest,
  pyyaml, requests. Ask before adding ANYTHING else (including pandas, pyarrow, rapidfuzz, Faker, linters).
  Generators use the standard library only.
- Do not add anything from "Out of scope" without asking first.

## Dataset gate (before writing any downloader code)
1. Tell me which public product dataset and which fields you plan to use.
2. Inspect a few real records first. Do not assume field names.
3. Check and quote the dataset's license/terms. Flag anything restricting redistribution or non-research use.
4. If it doesn't work (access, license, missing fields), propose a fallback and WAIT for my choice.
Default candidate: Amazon Reviews 2023 item metadata (Hugging Face), one category, ~100k records.
Fallback candidate: Open Food Facts (real barcodes). Barcodes, prices, stock are synthetic either way.

## Stack and commands
Python 3.11+, PySpark and delta-spark (pinned, compatible pair; verify against Delta's compatibility
table in M0 and report the pair chosen), Java 17, pytest, pyyaml, requests.
Note: the first Delta session downloads Delta jars from Maven Central (needs internet; cached afterwards).
- `make setup`    create venv, install deps
- `make data`     download source slice and generate vendor feeds + ground truth
- `make run-all`  run batches in date order through the whole pipeline
- `make reports-show` print the top rows of each report table
- `make test`     run pytest
- `make bench`    run optimization experiments and write docs/RESULTS.md
(Add each target only when its milestone lands; don't stub targets that do nothing.)

## Layout
src/catalog_pipeline/{generate,ingest,transform,history,match,reports}, pipeline/ (per-stage orchestration + CLI), bench.py,
conf/, tests/{unit,integration}, docs/RESULTS.md. Lake layers under data/lake: bronze, silver, gold, ops.

## Vendors and formats
- Vendors A and B: CSV (all columns read as strings). Vendors C and D: JSON.
- Each vendor has a YAML contract in conf/vendors/: format, read options, and expected columns.
  Per column: source name, canonical name, type, required (yes/no), and optional `aliases`
  (other known source names for the same column).
- A contract is invalid (fail at load) if one alias appears on more than one column.

## Data model (summary)
- bronze.<vendor>: contract columns under their CANONICAL names, all scalars cast to string
  (nested JSON structs stay nested) + _vendor_id, _batch_id (format YYYY-MM-DD), _source_file,
  _ingested_at, batch_date (= to_date(_batch_id)). Partitioned by batch_date.
- silver.vendor_product (latest values, key vendor_id+vendor_sku), silver.vendor_offer (per batch),
  silver.quarantine (row-level, with reason)
- gold.offer_history (SCD2): offer_key, price, stock_status, row_hash, valid_from, valid_to
  (NULL when current, half-open), is_current
- gold.product_xref, gold.dim_product, gold.rpt_attribute_completeness, gold.rpt_price_changes_by_brand
- ops.batch_log (status SUCCESS | QUARANTINED), ops.schema_drift_log
  (ADDED | MISSING | RENAMED | TYPE_CHANGED), ops.er_run_log

## Key behaviours
- Drift detection is a pure function: (observed schema, contract) -> (drift list, rename mapping, batch verdict).
  Alias matching is case-insensitive and trimmed.
  - ADDED column (not in contract, not an alias): log, ignore downstream.
  - RENAMED: source column matches a declared alias and the expected source name is absent ->
    log RENAMED, map to the canonical column, process the batch normally.
    If BOTH the expected name and an alias are present: the expected name wins, the alias is logged as ADDED.
  - MISSING optional (no alias match): fill with null + warn.
  - MISSING required (no alias match): quarantine the whole batch. Quarantined batch = NOT written to
    bronze, logged in batch_log as QUARANTINED, files stay in landing, other vendors continue.
  - TYPE_CHANGED: JSON vendors ONLY (observed types vs contract types). Log + warn; bronze still
    stores the value as string. CSV has no type drift: bad values surface as parse failures in silver
    and go to silver.quarantine with a reason.
- Generated data: day 2 vendor A (CSV) adds a column; day 3 vendor B (CSV) renames `title` -> `name`
  (alias declared in B's contract, so the batch processes normally); day 3 vendor C (JSON) changes
  `price` type (number -> string).
- Price parsing handles "$1,299.00" and "1.299,00" with regexp/when, no UDFs. Single currency (USD) only.
- stock_status: qty 0 -> OUT, <=5 -> LOW, else IN. SCD2 tracks price + stock_status only.
- SCD2 via Delta MERGE with a staged source (real key to close old version, NULL key to insert new
  version). Process batches in date order. Skip batches already SUCCESS in batch_log. Per vendor,
  reject batches older than that vendor's last SUCCESS batch (known limitation: a quarantined day
  cannot be replayed after a later day succeeds).
- ER: tier 1 exact on gtin14; tier 2 blocked fuzzy (token Jaccard + levenshtein ratio + price closeness),
  cross-vendor pairs only; cluster with 3-round label propagation; golden record via window function.
  Optimize for precision (target >= 0.95).
- Evaluate ER against ground_truth and log to ops.er_run_log.

## Testing
- Tiny hand-built fixtures (a few rows each). Shared session-scoped Spark fixture: master local[2],
  shuffle.partitions=2, UI disabled.
- Required tests: price parser; drift detector (pure function) covering ADDED, declared RENAMED,
  MISSING optional, TYPE_CHANGED (JSON), and UNKNOWN RENAME of a required column -> batch quarantined;
  silver counts reconcile; SCD2 (new / changed / unchanged / replayed batch + invariants: one current
  row per key, no overlapping intervals); ER (exact, typo positive, different-model negative,
  3-record chain); report values hand-computed.
- One small end-to-end test on generated data (tiny n).
- Run `make test` before declaring any milestone done.

## Workflow
Work one milestone at a time, in order. Before each, state your plan in 5-10 lines, list the files
you will create, and wait for a go-ahead before writing code.
M0 setup | M1 data generation (dataset gate first) | M2 bronze + drift | M3 silver normalize |
M4 SCD2 incremental | M5 entity resolution (5a GTIN, 5b fuzzy, 5c cluster, 5d golden, 5e evaluate) |
M6 reports | M7 optimize + measure | M8 README + diagram.
If behind schedule, cut in this order: M5c label propagation, second blocking pass, stretch report.
Never cut tests or measurements.

## Measurements (must end up in README and docs/RESULTS.md)
Rows processed per stage and rows/sec; ER precision/recall; candidate pairs before/after blocking
change; full-run runtime before/after shuffle-partition and broadcast tuning; machine specs.

## Out of scope
Streaming, orchestration (Airflow), Docker/cloud, ML/embedding matching, multi-currency/FX,
delisted-product handling, out-of-order batches, dashboards, Great Expectations,
image/description processing.
