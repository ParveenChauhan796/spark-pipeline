"""ops.batch_log, ops.schema_drift_log, ops.silver_log and ops.history_log: small Delta tables recording what each batch did.

All logs keep exactly one set of rows per (vendor_id, batch_id): recording a batch again
replaces its earlier rows instead of appending, so re-running a batch never changes the logs.
"""
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

from catalog_pipeline.ingest.drift import Drift
from catalog_pipeline.paths import table_path

SUCCESS = "SUCCESS"
QUARANTINED = "QUARANTINED"

BATCH_LOG_SCHEMA = StructType(
    [
        StructField("vendor_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("status", StringType(), False),  # SUCCESS | QUARANTINED
        StructField("rows_ingested", LongType(), True),
        StructField("reason", StringType(), True),
        StructField("source_files", StringType(), True),
        StructField("processed_at", TimestampType(), False),
    ]
)

DRIFT_LOG_SCHEMA = StructType(
    [
        StructField("vendor_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("drift_type", StringType(), False),  # ADDED | MISSING | RENAMED | TYPE_CHANGED
        StructField("source_column", StringType(), False),
        StructField("canonical_column", StringType(), True),
        StructField("is_required", BooleanType(), True),
        StructField("expected_type", StringType(), True),
        StructField("observed_type", StringType(), True),
        StructField("detected_at", TimestampType(), False),
    ]
)

SILVER_LOG_SCHEMA = StructType(
    [
        StructField("vendor_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("rows_in", LongType(), False),
        StructField("valid", LongType(), False),  # rows written to silver (latest row per key)
        StructField("quarantined", LongType(), False),
        StructField("deduped", LongType(), False),  # older duplicate rows dropped
        StructField("processed_at", TimestampType(), False),
    ]
)

HISTORY_LOG_SCHEMA = StructType(
    [
        StructField("vendor_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("rows_in", LongType(), False),  # silver.vendor_offer rows of this batch
        StructField("new", LongType(), False),
        StructField("changed", LongType(), False),
        StructField("unchanged", LongType(), False),
        StructField("stale", LongType(), False),  # already in history (replay or crash retry)
        StructField("table_version", LongType(), True),  # gold.offer_history version after this batch
        StructField("processed_at", TimestampType(), False),
    ]
)

_BATCH_ID = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def check_batch_id(batch_id: str) -> str:
    """Batch ids are YYYY-MM-DD; they end up inside SQL predicates, so validate them."""
    if not _BATCH_ID.match(batch_id):
        raise ValueError(f"batch id must look like YYYY-MM-DD, got {batch_id!r}")
    return batch_id


def log_path(name: str, config: dict) -> Path:
    return table_path("ops", config["ingest"]["ops_tables"][name], config)


def replace_where(df: DataFrame, path: Path, predicate: str, partition_by: str | None = None) -> None:
    """Delta `replaceWhere`: atomically delete the rows matching `predicate` and write `df` instead.

    This is what makes writes safe to repeat: a retry replaces the earlier attempt's rows rather
    than adding a second copy. Delta checks that every new row satisfies the predicate. If the
    table does not exist yet, it is created.
    """
    writer = df.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(str(path))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def record_batch(
    spark: SparkSession,
    config: dict,
    vendor_id: str,
    batch_id: str,
    status: str,
    rows_ingested: int | None,
    reason: str | None,
    source_files: Iterable[str],
) -> None:
    check_batch_id(batch_id)
    row = (vendor_id, batch_id, status, rows_ingested, reason, ",".join(source_files), _now())
    df = spark.createDataFrame([row], BATCH_LOG_SCHEMA)
    replace_where(df, log_path("batch_log", config), f"vendor_id = '{vendor_id}' AND batch_id = '{batch_id}'")


def record_drifts(spark: SparkSession, config: dict, vendor_id: str, batch_id: str, drifts: Iterable[Drift]) -> None:
    check_batch_id(batch_id)
    now = _now()
    rows = [
        (vendor_id, batch_id, d.drift_type, d.source_column, d.canonical_column, d.is_required,
         d.expected_type, d.observed_type, now)
        for d in drifts
    ]
    df = spark.createDataFrame(rows, DRIFT_LOG_SCHEMA)
    replace_where(df, log_path("schema_drift_log", config), f"vendor_id = '{vendor_id}' AND batch_id = '{batch_id}'")


def read_batch_log(spark: SparkSession, config: dict) -> DataFrame | None:
    """The batch log, or None if no batch has been recorded yet."""
    path = str(log_path("batch_log", config))
    return spark.read.format("delta").load(path) if DeltaTable.isDeltaTable(spark, path) else None


def batch_status(spark: SparkSession, config: dict, vendor_id: str, batch_id: str) -> str | None:
    """Recorded status of one batch (SUCCESS | QUARANTINED), or None if never recorded."""
    log = read_batch_log(spark, config)
    if log is None:
        return None
    rows = log.filter((F.col("vendor_id") == vendor_id) & (F.col("batch_id") == batch_id)).collect()  # <= 1 row
    return rows[0]["status"] if rows else None


def last_success(spark: SparkSession, config: dict, vendor_id: str) -> str | None:
    """Latest batch_id (YYYY-MM-DD sorts correctly as text) with status SUCCESS for this vendor."""
    log = read_batch_log(spark, config)
    if log is None:
        return None
    ok = log.filter((F.col("vendor_id") == vendor_id) & (F.col("status") == SUCCESS))
    return ok.agg(F.max("batch_id")).collect()[0][0]  # one scalar


def record_silver(spark: SparkSession, config: dict, vendor_id: str, batch_id: str, counts: dict[str, int]) -> None:
    check_batch_id(batch_id)
    row = (vendor_id, batch_id, counts["rows_in"], counts["valid"], counts["quarantined"], counts["deduped"], _now())
    df = spark.createDataFrame([row], SILVER_LOG_SCHEMA)
    replace_where(df, log_path("silver_log", config), f"vendor_id = '{vendor_id}' AND batch_id = '{batch_id}'")


def silver_done(spark: SparkSession, config: dict, vendor_id: str, batch_id: str) -> bool:
    """True if silver already processed this batch (its counts row exists)."""
    return _logged(spark, config, "silver_log", vendor_id, batch_id)


def record_history(
    spark: SparkSession, config: dict, vendor_id: str, batch_id: str, counts: dict[str, int], table_version: int | None
) -> None:
    check_batch_id(batch_id)
    row = (vendor_id, batch_id, counts["rows_in"], counts["NEW"], counts["CHANGED"], counts["UNCHANGED"],
           counts["STALE"], table_version, _now())
    df = spark.createDataFrame([row], HISTORY_LOG_SCHEMA)
    replace_where(df, log_path("history_log", config), f"vendor_id = '{vendor_id}' AND batch_id = '{batch_id}'")


def history_done(spark: SparkSession, config: dict, vendor_id: str, batch_id: str) -> bool:
    """True if the SCD2 history step already processed this batch."""
    return _logged(spark, config, "history_log", vendor_id, batch_id)


def _logged(spark: SparkSession, config: dict, name: str, vendor_id: str, batch_id: str) -> bool:
    path = str(log_path(name, config))
    if not DeltaTable.isDeltaTable(spark, path):
        return False
    log = spark.read.format("delta").load(path)
    return log.filter((F.col("vendor_id") == vendor_id) & (F.col("batch_id") == batch_id)).limit(1).count() > 0
