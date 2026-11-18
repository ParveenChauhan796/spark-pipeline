# Repository guide

What every folder and file is for, and the design principles the code follows. For the *why* behind
individual behaviours, see `docs/DECISIONS.md` (D1, D2, ...).

## The flow in one picture

```
data/landing (vendor files, per day)
   │  ingest/      contracts + drift detection
   ▼
bronze  (as delivered, all strings, + lineage)         ops.batch_log, ops.schema_drift_log
   │  transform/   parse, clean, canonicalize
   ▼
silver  (vendor_product, vendor_offer, quarantine)     ops.silver_log
   │  history/     SCD2 MERGE
   ▼
gold.offer_history (price/stock timeline)              ops.history_log
   │  match/       GTIN + blocked fuzzy matching, clustering, golden record
   ▼
gold.product_xref, gold.dim_product                    ops.er_run_log
   │  reports/
   ▼
gold.rpt_attribute_completeness, rpt_price_changes_by_brand, rpt_price_spread
```

The `pipeline/` package drives all of this. `bench.py` times it.

## Top level

| Path | Why it exists |
|---|---|
| `CLAUDE.md` | The project brief and hard rules (no UDFs, no inferSchema, config-driven, idempotent, test rules) |
| `Makefile` | The only commands you need: `setup`, `data`, `run-all`, `reports-show`, `test`, `bench`. Pins Java 17 for this project only (Linux JVM dir or macOS `java_home`) |
| `pyproject.toml` | Package metadata and pinned dependencies (pyspark 3.5.9 + delta-spark 3.3.3, a compatible pair). `pip install -e .` makes `catalog_pipeline` importable |
| `.gitignore` | Keeps `data/` (lake, feeds, source download), the venv, IDE files and bundles out of git |
| `conf/` | All settings. Nothing tunable is hard-coded in Python |
| `src/catalog_pipeline/` | The code, as one installable package |
| `tests/` | Unit tests (pure functions, tiny DataFrames) and integration tests (real Delta tables in temp dirs) |
| `docs/` | Plans, decisions, results and guides (this file) |
| `data/` (not in git) | `source/` (downloaded Amazon slice), `landing/` (generated vendor feeds), `reference/` (brand alias table), `ground_truth/` (answer key for ER), `lake/` (bronze/silver/gold/ops Delta tables), `bench/` (benchmark lakes) |

## `conf/`

| File | Purpose |
|---|---|
| `pipeline.yaml` | One section per stage: `spark`, `lake`, `generate`, `ingest`, `normalization`, `silver`, `history`, `er`, `reports`, `bench`. Thresholds carry a comment pointing at their decision (e.g. `# D30`) |
| `vendors/a.yaml` ... `d.yaml` | One **contract** per vendor: file format, read options, and for each column its source name, canonical name, type, whether it's required, and known `aliases`. Drift detection compares every incoming file against this |

Why YAML contracts instead of code: adding a vendor or a known rename is a config change, reviewable by
someone who doesn't read Spark.

## `src/catalog_pipeline/`

### Shared plumbing
| File | Purpose |
|---|---|
| `config.py` | `load_config()` reads `conf/pipeline.yaml`. `PROJECT_ROOT` anchors relative paths |
| `paths.py` | Turns config into lake paths: `layer_path`, `table_path`, and named helpers for the tables several modules share (`silver_table_path`, `offer_history_path`, `er_table_path`). The only place that knows the lake layout |
| `spark.py` | `get_spark()`: one SparkSession factory with the Delta extension, shuffle partitions, driver memory, UI on/off, and `extra_conf` passthrough. Tests and the bench override it, nothing else builds sessions |

### `generate/`: synthetic vendor feeds (standard library only, seeded)
| File | Purpose |
|---|---|
| `__main__.py` | `python -m catalog_pipeline.generate` (= `make data`): download, then generate |
| `source.py` | Downloads the Amazon Reviews 2023 *Appliances* metadata slice and streams its JSON lines |
| `catalogue.py` | Builds the clean "truth" catalogue: cleaned titles, brands, models, categories, and synthetic valid GTINs (with check digits) |
| `vendors.py` | Simulates 4 vendors × 3 days: different schemas, coverage, price spreads; day 1 full snapshot, days 2-3 change feeds; injects the scripted drift (A adds a column, B renames `title` to `name`, C sends price as a string) |
| `mess.py` | Mess injection: typos, word shuffles, whitespace noise, truncation, brand aliases, US vs European price formats |
| `brands.py` | Writes the brand alias reference table. Its `brand_key` is the Python twin of the Spark one, and a test checks they agree |
| `writer.py` | Writes CSV/JSONL feeds and `ground_truth.csv` with deterministic bytes for a given seed |

### `ingest/`: landing to bronze, plus the ops logs
| File | Purpose |
|---|---|
| `contracts.py` | Loads and validates vendor contracts (fails if one alias is claimed by two columns) |
| `drift.py` | `detect_drift(observed, contract)`: a **pure function** with no Spark and no I/O, returning drifts, a rename mapping and a verdict (OK / QUARANTINE) |
| `bronze.py` | `ingest_batch`: skip if already done, reject stale days, observe the schema, detect drift, then quarantine or write bronze (all strings, canonical names, lineage columns, partitioned by `batch_date`) |
| `ops.py` | The five ops logs. Every write is a `replaceWhere` on (vendor, batch), so logging twice never duplicates rows. Also the "already done?" checks that make every stage skippable |

### `transform/`: pure column expressions and DataFrame-to-DataFrame functions
| File | Purpose |
|---|---|
| `price.py` | `parse_price`: `$1,299.00` and `1.299,00` to `decimal(12,2)` with regex and `when`, no UDF |
| `stock.py` | `parse_qty`, and `stock_status` (0 = OUT, ≤5 = LOW, else IN) |
| `gtin.py` | Barcodes to GTIN-14 (left-padded digits, otherwise null) |
| `text.py` | Title normalization and tokens, category normalization |
| `brand.py` | `brand_key` and `canonicalize_brand` (alias table join, broadcast by default) |
| `silver.py` | `classify` (clean and flag every row once), then `to_offers`, `to_products`, `to_quarantine` and `batch_counts` cut that one frame into the silver outputs |

### `history/`
| File | Purpose |
|---|---|
| `scd2.py` | `classify_changes` (NEW/CHANGED/UNCHANGED/STALE), `stage_merge` (the two-rows-per-change MERGE source), `invariant_violations` (the timeline must have no gaps, no overlaps and exactly one current row) |

### `match/`: entity resolution
| File | Purpose |
|---|---|
| `candidates.py` | `prepare` (one row per vendor record with normalized fields and current price), `gtin_pairs` (tier 1), the block-key functions and `block_key_passes`, and `block_pairs` (tier 2 candidates, with oversized blocks dropped) |
| `score.py` | Similarities (token Jaccard, Levenshtein ratio, price closeness), vetoes (different model or GTIN), `accepted`, `best_match_per_vendor`, `combine_edges` |
| `cluster.py` | `label_propagation`: 3 rounds of "take the smallest neighbour label" |
| `golden.py` | `build_xref` (record to master), `build_dim` (the most complete record wins, then vendor priority), `same_vendor_clusters` (a quality signal) |
| `evaluate.py` | Pairwise precision and recall against ground truth, counted with n(n-1)/2 instead of building the pairs |

### `reports/`
| File | Purpose |
|---|---|
| `reports.py` | `attribute_completeness` (unpivot), `price_changes` (lag window), `price_changes_by_brand`, `price_spread` |

### Benchmark
| File | Purpose |
|---|---|
| `bench.py` | `make bench`: runs E2 variants in fresh processes on their own lakes, runs E1 blocking, and writes `docs/RESULTS.md` |

### `pipeline/`: the orchestrator (all lake I/O outside `ingest/`)
One module per stage. Each reads its inputs, calls the pure functions, writes the results and logs them.

| File | Purpose |
|---|---|
| `outcomes.py` | Statuses (`DONE`, `ALREADY_DONE`, `NOT_INGESTED`, `NOT_READY`) and the result records `SilverOutcome`, `HistoryOutcome`, `BatchResult` and `ErOutcome` |
| `silver.py` | `load_brand_aliases`, and `silver_batch` (bronze to silver for one vendor batch, using the `_upsert_products` MERGE) |
| `history.py` | `history_batch` (version-pinned read, classify, one SCD2 MERGE, log) and `table_version` |
| `run.py` | `discover_batches`, and `run_all` (every landed batch through bronze, silver and history, oldest day first) |
| `er.py` | `resolve_entities` (the full ER chain, metrics and the skip check) and `load_ground_truth` |
| `reports.py` | `build_reports` (overwrites the three report tables) |
| `cli.py` and `__main__.py` | `python -m catalog_pipeline.pipeline {run-all, er [--force], reports, show}` (`show` prints the top rows of each report, for `make reports-show`) |
| `__init__.py` | Re-exports the public functions, so callers write `from catalog_pipeline import pipeline; pipeline.run_all(...)` |

## `tests/`
| Path | What it covers |
|---|---|
| `conftest.py` | One session-wide SparkSession (`local[2]`, 2 shuffle partitions, no UI). Starting a JVM per test would be far too slow |
| `unit/test_price.py`, `test_stock.py`, `test_gtin.py`, `test_text.py`, `test_brand.py` | Column expressions on tiny inputs |
| `unit/test_contracts.py`, `test_drift.py` | Contract validation, plus every drift case including an unknown rename of a required column leading to quarantine |
| `unit/test_generate.py` | Determinism (same seed, same bytes) and the scripted drift |
| `unit/test_silver_transform.py` | Classification, dedup, quarantine reasons, count reconciliation |
| `unit/test_scd2.py` | Change classification, staging and invariants |
| `unit/test_match.py` | GTIN, typo positive, different-model negative, the 3-record chain, and a known-limitation test |
| `unit/test_reports.py` | Report values computed by hand |
| `unit/test_bench.py` | Variant configs, the cross-vendor pair formula, which join operator is used |
| `unit/test_smoke_delta.py` | Delta works at all (write, append, read, table history) |
| `integration/test_bronze_ingest.py`, `test_silver.py`, `test_offer_history.py` | Real Delta tables in temp dirs: idempotent re-runs, quarantine, stale-batch rejection |
| `integration/test_er.py` | End to end on tiny generated data: batches, then ER, then reports, plus the ER skip/re-run logic |

## `docs/`
| File | Purpose |
|---|---|
| `OVERVIEW.md` | The use case and pain points for non-technical readers |
| `DOMAIN.md` | Domain model: bounded contexts, context map, what each context owns and guarantees, glossary |
| `REPO_GUIDE.md` | This file |
| `KEY_COMPONENTS.md` | The handful of functions that do the real work, and how they work |
| `DECISIONS.md` | Numbered decision log (D1...): what was decided, why, the alternative, where in the code |
| `MILESTONES.md` | The build plan, M0-M8, with checkboxes |
| `HANDOFF.md` | State of the project for the next working session |
| `RESULTS.md` | Generated by `make bench`: machine, timings, blocking experiment |
| `GIT.md` | Local git workflow rules |

## Design principles, and where to see them

| Principle | How it shows up here |
|---|---|
| **Functional core, imperative shell** | `transform/`, `history/scd2.py`, `match/`, `reports/` and `ingest/drift.py` are pure: DataFrames (or dicts) in, DataFrames out, no reads or writes. All I/O sits in `ingest/` and the `pipeline/` package. Pure parts are tested with tiny in-memory frames, with no lake needed |
| **Single responsibility** | One module per concern (price parsing, drift, SCD2, scoring, clustering...), and one orchestration module per stage in `pipeline/`. `paths.py` alone knows the lake layout, and `spark.py` alone builds sessions |
| **Configuration over code** | Every path, threshold, weight and table name lives in `conf/`. Code reads `config[...]` (no silent defaults), so a missing setting fails loudly |
| **Explicit contracts at the boundary** | Vendor YAML contracts plus drift detection. Explicit schemas everywhere, and `inferSchema` only to *observe* JSON types for drift, never to define a table |
| **Idempotency** | Each stage checks its ops log and skips finished work. Writes use `replaceWhere`, MERGE or overwrite, never blind appends. ER skips if its input versions and config hash are unchanged |
| **Atomicity** | Every Delta write commits fully or not at all. One batch's SCD2 changes are one MERGE |
| **Fail loudly, never silently drop** | Count reconciliation raises on mismatch. Bad rows go to quarantine with a reason. Missing required columns quarantine the batch and are logged |
| **Determinism** | Seeded generation, deterministic tie-breaks (vendor priority, then key), and ER reads pinned to table versions |
| **Observability** | Five ops logs, ER metrics per run, bench results, and a printed per-batch summary |
| **DRY (don't repeat yourself)** | One `brand_key` in each language, with a test tying them together. The blocking passes are defined once (`candidates.block_key_passes`) and used by both ER and the bench |
| **Encapsulation** | Underscore-prefixed helpers stay inside their module. Anything shared across modules is public and documented (`paths.offer_history_path`, etc.) |
| **Built-ins over UDFs** | Only Spark built-in functions, so the optimizer sees everything and no rows go through Python |
| **Documented decisions** | Every behaviour-changing choice is in `DECISIONS.md` with its alternative, so it can be revisited without archaeology |

### Where the code fell short, and what was fixed (M7)
- **Private helpers used across modules:** `bench.py` reached into `pipeline._silver_path` /
  `_history_path`. These table locations now live in `paths.py` as public functions, used by both.
- **Duplicated blocking setup:** `bench.py` rebuilt the two blocking passes by hand, a copy of the
  code inside `resolve_entities`. Both now call `candidates.block_key_passes(records, blocking_cfg)`, so
  a change to blocking can't make the benchmark measure something other than the pipeline.

- **One 400-line orchestrator:** `pipeline.py` held every stage's I/O plus the CLI. It's now the
  `pipeline/` package, one module per stage. The code moved without changes, and `__init__.py` keeps
  the old import names working. The CLAUDE.md rule now reads "I/O lives only in ingest/ and the
  pipeline/ package", with the standalone tools `generate/` and `bench.py` named as the only other places.

### Known trade-offs, kept on purpose
- **`config` is a plain dict, not typed dataclasses:** a typo in a key surfaces as a `KeyError` at
  first use rather than at load time. That's acceptable for a project of this size.
