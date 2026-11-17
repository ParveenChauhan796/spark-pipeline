"""Pipeline CLI and stage orchestration: `python -m catalog_pipeline.pipeline run-all`.

I/O lives here and in ingest/. The functions in transform/ stay pure.
"""
import argparse
import time
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from catalog_pipeline.config import load_config
from catalog_pipeline.history import scd2
from catalog_pipeline.ingest import ops
from catalog_pipeline.ingest.bronze import SKIPPED, SUCCESS, BatchOutcome, bronze_path, ingest_batch
from catalog_pipeline.ingest.contracts import Contract, load_contracts
from catalog_pipeline.paths import resolve, table_path
from catalog_pipeline.spark import get_spark
from catalog_pipeline.transform import silver as silver_tf

DONE = "DONE"
ALREADY_DONE = "ALREADY_DONE"
NOT_INGESTED = "NOT_INGESTED"
NOT_READY = "NOT_READY"  # history: silver has not processed this batch


@dataclass(frozen=True)
class SilverOutcome:
    vendor_id: str
    batch_id: str
    status: str  # DONE | ALREADY_DONE | NOT_INGESTED
    counts: dict[str, int] | None = None
    reasons: dict[str, int] | None = None


@dataclass(frozen=True)
class HistoryOutcome:
    vendor_id: str
    batch_id: str
    status: str  # DONE | ALREADY_DONE | NOT_READY
    counts: dict[str, int] | None = None  # rows_in, NEW, CHANGED, UNCHANGED, STALE
    table_version: int | None = None


@dataclass(frozen=True)
class BatchResult:
    """What every stage did with one vendor batch."""
    bronze: BatchOutcome
    silver: SilverOutcome
    history: HistoryOutcome


def discover_batches(config: dict, vendor_ids: list[str]) -> list[tuple[str, str]]:
    """(batch_id, vendor_id) for every batch folder in landing, in date order."""
    landing = resolve(config["ingest"]["landing_dir"])
    found = []
    for vendor_id in vendor_ids:
        vendor_dir = landing / vendor_id
        if vendor_dir.is_dir():
            found += [(d.name, vendor_id) for d in vendor_dir.iterdir() if d.is_dir()]
    return sorted(found)


def load_brand_aliases(spark: SparkSession, config: dict) -> DataFrame:
    """The brand reference table (brand_key, canonical_brand), read with an explicit schema."""
    path = resolve(config["silver"]["brand_aliases_path"])
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .schema("brand_key string, canonical_brand string")
        .load(str(path))
    )


def _silver_path(config: dict, name: str):
    return table_path("silver", config["silver"]["tables"][name], config)


def _upsert_products(spark: SparkSession, path, batch_products: DataFrame) -> None:
    """Update silver.vendor_product with this batch's latest values.

    MERGE is Delta's atomic upsert: rows whose key already exists are updated, the rest inserted,
    in one transaction (plain Parquet files cannot be changed in place, which is why this project
    uses Delta). The `>=` condition means an older batch can never overwrite newer values, and
    re-running the same batch rewrites identical values, so replays change nothing.
    """
    if not DeltaTable.isDeltaTable(spark, str(path)):
        batch_products.write.format("delta").save(str(path))
        return
    (
        DeltaTable.forPath(spark, str(path))
        .alias("t")
        .merge(batch_products.alias("s"), "t.vendor_id = s.vendor_id AND t.vendor_sku = s.vendor_sku")
        .whenMatchedUpdateAll(condition="s.last_batch_date >= t.last_batch_date")
        .whenNotMatchedInsertAll()
        .execute()
    )


def silver_batch(
    spark: SparkSession, config: dict, contract: Contract, batch_id: str, aliases: DataFrame
) -> SilverOutcome:
    """Bronze -> silver for one vendor batch. Safe to repeat: a finished batch is skipped, and every
    write replaces or merges rather than appends."""
    vendor_id = contract.vendor_id
    ops.check_batch_id(batch_id)
    if ops.batch_status(spark, config, vendor_id, batch_id) != SUCCESS:
        return SilverOutcome(vendor_id, batch_id, NOT_INGESTED)
    if ops.silver_done(spark, config, vendor_id, batch_id):
        return SilverOutcome(vendor_id, batch_id, ALREADY_DONE)

    batch_date = F.to_date(F.lit(batch_id))
    bronze = spark.read.format("delta").load(str(bronze_path(contract, config))).filter(F.col("batch_date") == batch_date)

    # Caching: the classified frame feeds the counts and three writes. persist() keeps it in
    # memory after the first pass so those later steps do not recompute it from the bronze files.
    classified = silver_tf.classify(bronze, aliases, config).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        counts = silver_tf.batch_counts(classified)
        if counts["rows_in"] != counts["valid"] + counts["quarantined"] + counts["deduped"]:
            raise RuntimeError(f"silver counts do not reconcile for {vendor_id} {batch_id}: {counts}")

        predicate = f"batch_date = DATE'{batch_id}' AND vendor_id = '{vendor_id}'"
        ops.replace_where(silver_tf.to_offers(classified), _silver_path(config, "vendor_offer"), predicate, "batch_date")
        ops.replace_where(silver_tf.to_quarantine(classified), _silver_path(config, "quarantine"), predicate, "batch_date")
        _upsert_products(spark, _silver_path(config, "vendor_product"), silver_tf.to_products(classified))

        reasons = {
            r["reason"]: r["n"]
            for r in classified.filter(~F.col("is_valid")).groupBy("reason").agg(F.count(F.lit(1)).alias("n")).collect()
        }  # a handful of rows: one per reason
        ops.record_silver(spark, config, vendor_id, batch_id, counts)
    finally:
        classified.unpersist()
    return SilverOutcome(vendor_id, batch_id, DONE, counts, reasons)


def _history_path(config: dict):
    return table_path("gold", config["history"]["tables"]["offer_history"], config)


def _history_table(spark: SparkSession, path) -> DeltaTable:
    """gold.offer_history, created empty with its explicit schema on first use, so every batch
    (including the first) goes through the same MERGE."""
    if not DeltaTable.isDeltaTable(spark, str(path)):
        spark.createDataFrame([], scd2.HISTORY_SCHEMA).write.format("delta").save(str(path))
    return DeltaTable.forPath(spark, str(path))


def _version(table: DeltaTable) -> int:
    return table.history(1).select("version").first()[0]  # one row: the latest commit


def _merge_history(table: DeltaTable, staged: DataFrame) -> None:
    """Apply one batch's staged changes in a single atomic MERGE (see scd2.stage_merge):
    rows whose merge_key matches a current row close it; rows with a NULL key insert a new version.
    If the job dies halfway, Delta commits nothing, so history is never half-updated."""
    inserted = {c: f"s.{c}" for c in scd2.HISTORY_COLUMNS if c not in ("valid_to", "is_current")}
    (
        table.alias("t")
        .merge(staged.alias("s"), "t.offer_key = s.merge_key AND t.is_current")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",  # safety net: staging already dropped unchanged rows
            set={"valid_to": "s.valid_from", "is_current": "false"},
        )
        .whenNotMatchedInsert(values={**inserted, "valid_to": "CAST(NULL AS DATE)", "is_current": "true"})
        .execute()
    )


def history_batch(spark: SparkSession, config: dict, vendor_id: str, batch_id: str) -> HistoryOutcome:
    """silver.vendor_offer -> gold.offer_history (SCD2) for one vendor batch. Safe to repeat: a
    logged batch is skipped, and rows not newer than an offer's current version are ignored (STALE)."""
    ops.check_batch_id(batch_id)
    if not ops.silver_done(spark, config, vendor_id, batch_id):
        return HistoryOutcome(vendor_id, batch_id, NOT_READY)
    if ops.history_done(spark, config, vendor_id, batch_id):
        return HistoryOutcome(vendor_id, batch_id, ALREADY_DONE)

    path = _history_path(config)
    table = _history_table(spark, path)
    # Time travel: read the current rows as of a fixed table version. The MERGE below changes the
    # table; pinning the version means any recomputation of `classified` still sees the old state.
    current = (
        spark.read.format("delta").option("versionAsOf", _version(table)).load(str(path))
        .filter(F.col("is_current") & (F.col("vendor_id") == vendor_id))
    )
    offers = (
        spark.read.format("delta").load(str(_silver_path(config, "vendor_offer")))
        .filter((F.col("vendor_id") == vendor_id) & (F.col("batch_date") == F.to_date(F.lit(batch_id))))
    )

    classified = scd2.classify_changes(offers, current).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        by_type = {r["change_type"]: r["n"] for r in classified.groupBy("change_type").agg(F.count(F.lit(1)).alias("n")).collect()}
        counts = {"rows_in": offers.count(), **{t: by_type.get(t, 0) for t in scd2.CHANGE_TYPES}}
        if counts["rows_in"] != sum(counts[t] for t in scd2.CHANGE_TYPES):
            # More rows out than in would mean an offer had two current versions.
            raise RuntimeError(f"history counts do not reconcile for {vendor_id} {batch_id}: {counts}")
        if counts[scd2.NEW] + counts[scd2.CHANGED]:
            _merge_history(table, scd2.stage_merge(classified))
        version = _version(table)
        ops.record_history(spark, config, vendor_id, batch_id, counts, version)
    finally:
        classified.unpersist()
    return HistoryOutcome(vendor_id, batch_id, DONE, counts, version)


def run_all(spark: SparkSession, config: dict) -> list[BatchResult]:
    """Run every landed batch through bronze, silver and history, oldest day first. One vendor's
    quarantined batch never stops the others."""
    contracts = load_contracts(resolve(config["ingest"]["contracts_dir"]))
    aliases = load_brand_aliases(spark, config)
    results = []
    for batch_id, vendor_id in discover_batches(config, sorted(contracts)):
        start = time.perf_counter()
        bronze_outcome = ingest_batch(spark, config, contracts[vendor_id], batch_id)
        silver_outcome = (
            silver_batch(spark, config, contracts[vendor_id], batch_id, aliases)
            if bronze_outcome.outcome in (SUCCESS, SKIPPED)
            else SilverOutcome(vendor_id, batch_id, NOT_INGESTED)
        )
        history_outcome = (
            history_batch(spark, config, vendor_id, batch_id)
            if silver_outcome.status in (DONE, ALREADY_DONE)
            else HistoryOutcome(vendor_id, batch_id, NOT_READY)
        )
        result = BatchResult(bronze_outcome, silver_outcome, history_outcome)
        results.append(result)
        print(_describe(result, time.perf_counter() - start))
    return results


def _describe(result: BatchResult, seconds: float) -> str:
    b, s, h = result.bronze, result.silver, result.history
    drift = ", ".join(f"{d.drift_type}({d.source_column})" for d in b.drifts) or "-"
    bronze = f"bronze {b.outcome}" + (f" rows={b.rows}" if b.rows is not None else f" ({b.reason})")
    silver = f"silver {s.status}"
    if s.counts:
        c = s.counts
        silver += f" in={c['rows_in']} valid={c['valid']} quarantined={c['quarantined']} deduped={c['deduped']}"
    history = f"history {h.status}"
    if h.counts:
        c = h.counts
        history += f" new={c['NEW']} changed={c['CHANGED']} unchanged={c['UNCHANGED']} stale={c['STALE']} v{h.table_version}"
    return f"vendor {b.vendor_id} {b.batch_id}: {bronze}; drift: {drift}; {silver}; {history} [{seconds:.1f}s]"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="catalog_pipeline.pipeline")
    parser.add_argument("command", choices=["run-all"])
    parser.parse_args(argv)

    config = load_config()
    spark = get_spark(config)
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_all(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
