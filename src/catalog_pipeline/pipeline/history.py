"""silver.vendor_offer -> gold.offer_history (SCD2) for one vendor batch (the change logic is history/scd2.py)."""
from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from catalog_pipeline.history import scd2
from catalog_pipeline.ingest import ops
from catalog_pipeline.paths import offer_history_path, silver_table_path
from catalog_pipeline.pipeline.outcomes import ALREADY_DONE, DONE, NOT_READY, HistoryOutcome


def _history_table(spark: SparkSession, path) -> DeltaTable:
    """gold.offer_history, created empty with its explicit schema on first use, so every batch
    (including the first) goes through the same MERGE."""
    if not DeltaTable.isDeltaTable(spark, str(path)):
        spark.createDataFrame([], scd2.HISTORY_SCHEMA).write.format("delta").save(str(path))
    return DeltaTable.forPath(spark, str(path))


def table_version(table: DeltaTable) -> int:
    """Latest commit version of a Delta table."""
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

    path = offer_history_path(config)
    table = _history_table(spark, path)
    # Time travel: read the current rows as of a fixed table version. The MERGE below changes the
    # table; pinning the version means any recomputation of `classified` still sees the old state.
    current = (
        spark.read.format("delta").option("versionAsOf", table_version(table)).load(str(path))
        .filter(F.col("is_current") & (F.col("vendor_id") == vendor_id))
    )
    offers = (
        spark.read.format("delta").load(str(silver_table_path("vendor_offer", config)))
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
        version = table_version(table)
        ops.record_history(spark, config, vendor_id, batch_id, counts, version)
    finally:
        classified.unpersist()
    return HistoryOutcome(vendor_id, batch_id, DONE, counts, version)
