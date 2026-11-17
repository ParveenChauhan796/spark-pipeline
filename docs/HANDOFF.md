# Handoff: state of the project for a new session

Written 2026-10-30, updated 2026-11-17 after M4. Read `CLAUDE.md` (the contract), `docs/MILESTONES.md` (build order) and `docs/DECISIONS.md` (behaviour-changing decisions, D1-D18 so far) first. This file says where the work stands and what is not obvious from the code.

## 1. Where we are

- **M0 to M4 are done and ticked** in `docs/MILESTONES.md`. `make test` last gave **99 passed** in about 7.5 min. The M4 integration tests each run 3 days end to end, which is what made the suite slower.
- **M5 (entity resolution, 5a-5e) is next and NOT started.** Per the workflow, present a 5-10 line plan, a file list and the proposed decisions (as `Proposed` entries in `docs/DECISIONS.md`), then wait for "go".
- After that: M6 reports, M7 optimize and measure, M8 README.

## 2. About the user and how to work with them

- Limited Spark experience. At first use of a Spark concept (window, broadcast, MERGE, partitioning/shuffle, AQE, caching) add a 2-4 line docstring or comment saying what it does and why. After each milestone give a short "what Spark actually did" summary. When something breaks, explain the cause in plain terms BEFORE fixing it.
- One milestone at a time: 5-10 line plan, file list, wait for go-ahead. Run the full `make test` before declaring a milestone done, tick it in MILESTONES.md, then stop and wait.
- Hard rules (full text in CLAUDE.md): DataFrame API / Spark SQL only (no UDFs); no inferSchema (only JSON reads in ingest/ for drift); no `.collect()`/`.toPandas()` except tiny results; transforms pure, I/O only in ingest/ and pipeline.py; no hardcoded paths or thresholds (conf/*.yaml, paths.py); every batch run idempotent; seeded randomness; **only pyspark, delta-spark, pytest, pyyaml, requests**. Ask before adding anything else. Add Makefile targets only when their milestone lands. Nothing from "Out of scope" without asking.
- Use they/them for the user unless told otherwise.
- **Record every behaviour-changing decision in `docs/DECISIONS.md` (user request, 2026-11-17).** The user wants a working prototype first, then to revisit these decisions and change the system. So whenever a plan proposes a choice that affects what data is kept, rejected or merged, how a table is keyed, typed or laid out, what counts as a change, match or error, or what a re-run does, add a `D<n>` entry. Each entry gives the decision, why, the alternative, and where in the code to change it. Add it as `Proposed` with the plan, and flip it to `Accepted <date>` on go-ahead. Never renumber; supersede instead. Rules are at the top of that file. Also list a milestone's new decision IDs in its summary to the user.

## 3. Environment

- Project root `/home/parveen/learning/pipeline` (not a git repo). Python 3.11.14 (uv), PySpark 3.5.9 + delta-spark 3.3.3, Java 17 pinned per-project in the Makefile.
- `make setup | data | run-all | test`. Tests use local[2], shuffle partitions 2, UI off.
- Gotchas (also in memory dir `~/.claude/projects/-home-parveen-learning-pipeline/memory/`):
  - `PYSPARK_PYTHON` must equal the driver interpreter (set via `os.environ.setdefault` in `spark.py`), otherwise workers fail with PYTHON_VERSION_MISMATCH.
  - The Makefile resolves the uv interpreter symlink with `readlink -f` when making the venv.
  - Delta reads use 50 tasks by default (`spark.databricks.delta.snapshotPartitions`). It is set to 2 in tests only (`extra_conf` in `tests/conftest.py`). Production keeps the default so M7 can measure it.
  - First Delta session downloads jars (needs internet, cached in `~/.ivy2`).
- Data: `data/source` (273 MB), `data/landing` (54 MB), `data/lake` is derived (safe to `rm -rf data/lake && make run-all`). Real silver totals: 277,197 offers, 217,384 products, 4,219 quarantine rows. A second `run-all` skips everything (~1 s per batch).

## 4. What exists (by area)

- `conf/pipeline.yaml`: sections `spark`, `lake`, `generate`, `ingest` (contracts_dir, landing_dir, bronze_table_prefix, `ops_tables` batch_log / schema_drift_log / silver_log / history_log), `normalization`, `silver` (brand_aliases_path, stock thresholds, gtin lengths, `tables` vendor_product / vendor_offer / quarantine), `history` (`tables.offer_history`). YAML anchors share paths.
- `conf/vendors/{a,b,c,d}.yaml`: contracts (A,B CSV; C flat JSON; D nested JSON). Drift injected by the generator: A adds `rating` (day 2, still present day 3, so logged ADDED twice), B renames title to `name` (day 3), C sends price as string (day 3).
- `src/catalog_pipeline/`: `spark.py` (`get_spark`), `config.py`, `paths.py` (`table_path(layer, table, config)`), `generate/` (stdlib only, deterministic), `ingest/` (`contracts.py`, `drift.py` pure detector, `bronze.py`, `ops.py` logs and helpers), `transform/` (`price`, `text`, `gtin`, `stock`, `brand`, `silver`), `history/scd2.py` (pure SCD2: `classify_changes`, `stage_merge`, `row_hash`, `invariant_violations`), `pipeline.py`.
- `pipeline.py`: `discover_batches`, `load_brand_aliases`, `_upsert_products` (Delta MERGE with `s.last_batch_date >= t.last_batch_date` guard), `silver_batch`, `history_batch` (reads current rows pinned with `versionAsOf`, classifies, one MERGE per vendor batch, logs), `run_all(spark, config)` returns a list of `BatchResult(bronze, silver, history)`; CLI `run-all`. Statuses: DONE, ALREADY_DONE, NOT_INGESTED (silver), NOT_READY (history). **M5 hooks in after the batch loop** (ER runs over the whole silver.vendor_product, not per batch).
- Bronze: partitioned by `batch_date`, written with `replaceWhere` (atomic delete+insert by predicate = idempotent). Has `_row_id = monotonically_increasing_id()`.
- Silver: `vendor_offer` (vendor_id, vendor_sku, batch_date, price decimal(12,2), qty, stock_status; one row per offer per batch, partitioned by batch_date), `vendor_product`, `quarantine` (reasons NULL_SKU, MISSING_TITLE, MISSING_PRICE, PRICE_UNPARSEABLE, PRICE_NON_POSITIVE, QTY_UNPARSEABLE).
- Gold: `offer_history` (SCD2, unpartitioned; columns in `scd2.HISTORY_SCHEMA`: spec columns plus vendor_id, vendor_sku). Real lake: 250,282 rows, 217,384 current (= vendor_product count), 32,898 closed versions, 0 invariant violations, 13 table versions (an empty create plus 12 MERGEs).
- `ops.batch_log` (SUCCESS | QUARANTINED), `ops.silver_log` (per-batch counts; reconciliation `rows_in = valid + quarantined + deduped` is enforced), `ops.history_log` (rows_in, new, changed, unchanged, stale, table_version; reconciliation `rows_in = new + changed + unchanged + stale` is enforced).
- Tests (99): `tests/unit/` (smoke_delta, generate, contracts, drift, price, text, gtin, stock, brand, silver_transform, scd2) and `tests/integration/` (`test_bronze_ingest.py`, `test_silver.py`, `test_offer_history.py`, which reuses the `env` fixture from test_silver). `test_silver.py` imports helpers (`A_HEADER`, `C_ROWS`, `D1`, `D2`, `D_ROWS`, `put_csv`, `put_jsonl`) from `test_bronze_ingest.py`. Integration `env` fixtures use a tmp lake root, tmp landing and a tmp brand-aliases file (deep-copied config).

## 5. Notes from M4 worth carrying forward

- Design and decisions: `docs/DECISIONS.md` D10-D18 (accepted 2026-11-17).
- **Small files (for M7):** each MERGE adds 4-8 files (one per shuffle partition), so the few-MB history table ends up with dozens of small files. Good M7 material: compare file counts before and after `OPTIMIZE` (and `ZORDER BY offer_key`, see D15) using `DESCRIBE DETAIL` / `DESCRIBE HISTORY` metrics.
- Real `make run-all` with only the history step new: about 1.5 min for 12 batches (day-1 MERGEs about 5-20 s, change days about 6 s). A full re-run skips everything at about 1 s per batch.
- 3,260 current offers have NULL stock_status: new offers whose qty has never been known (D10).

## 6. Known caveats to carry into the README

- Brand reference table is built from the golden catalogue (optimistic leakage in ER evaluation).
- Ambiguous bare price forms like `1.299` and `1,299`.
- A JSON column that is null in every row might be inferred as string and flagged TYPE_CHANGED (untested).
- A quarantined day cannot be replayed after a later day succeeds for that vendor (accepted limitation from CLAUDE.md).
- Delta snapshotPartitions is 50 in production; left for M7 to measure.
- SCD2: corrections to an already-loaded day are ignored (D12); absent offers are never closed (D14); a crash retry logs different counts (D18).

## 7. Next steps for the new session

1. Read `docs/DECISIONS.md`. Then plan M5 (start with 5a GTIN exact match and 5b blocked fuzzy matching; precision target >= 0.95). Put its decisions (blocking keys, similarity weights and thresholds in `conf/pipeline.yaml`, tie-breaks for the golden record, how ER reruns stay idempotent) into DECISIONS.md as `Proposed`, show the plan, and wait for "go".
2. On go: implement, run the full `make test`, give the "what Spark actually did" summary (blocking, shuffle and broadcast in `explain()`), flip the decisions to `Accepted`, tick MILESTONES.md, update this file, then stop.
