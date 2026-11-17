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
