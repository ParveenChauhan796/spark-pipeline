"""Bronze ingest: one vendor batch from landing -> bronze.<vendor> Delta table.

Bronze keeps the vendor's data as delivered: contract columns under canonical names, scalars as
strings, nested JSON structs left nested, plus lineage columns. Cleaning starts in silver.
"""
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    NumericType,
    StringType,
    StructField,
    StructType,
)

from catalog_pipeline.ingest import ops
from catalog_pipeline.ingest.contracts import Column, Contract
from catalog_pipeline.ingest.drift import QUARANTINE, Drift, detect_drift
from catalog_pipeline.paths import resolve, table_path

# Outcomes of ingest_batch. SUCCESS and QUARANTINED are logged in ops.batch_log; the other two mean
# nothing was done and nothing was logged.
SUCCESS = ops.SUCCESS
QUARANTINED = ops.QUARANTINED
SKIPPED = "SKIPPED"  # already ingested successfully
REJECTED_STALE = "REJECTED_STALE"  # older than this vendor's last successful batch


@dataclass(frozen=True)
class BatchOutcome:
    vendor_id: str
    batch_id: str
    outcome: str
    rows: int | None = None
    drifts: tuple[Drift, ...] = ()
    reason: str | None = None


def bronze_path(contract: Contract, config: dict) -> Path:
    name = f"{config['ingest']['bronze_table_prefix']}{contract.vendor_id.lower()}"
    return table_path("bronze", name, config)


def batch_files(config: dict, vendor_id: str, batch_id: str) -> list[Path]:
    batch_dir = resolve(config["ingest"]["landing_dir"]) / vendor_id / batch_id
    files = sorted(p for p in batch_dir.glob("*") if p.is_file()) if batch_dir.is_dir() else []
    if not files:
        raise FileNotFoundError(f"no files in {batch_dir}")
    return files


def type_category(dtype: DataType) -> str:
    """Collapse a Spark type to the coarse categories contracts use."""
    if isinstance(dtype, StringType):
        return "string"
    if isinstance(dtype, NumericType):
        return "number"
    if isinstance(dtype, StructType):
        return "struct"
    if isinstance(dtype, ArrayType):
        return "array"
    if isinstance(dtype, BooleanType):
        return "boolean"
    return "other"


def _spark_type(col: Column) -> DataType:
    """Bronze type of a contract column: string, or a struct whose members are all strings."""
    if col.type == "struct":
        return StructType([StructField(name, StringType(), True) for name, _ in col.fields])
    return StringType()


def _reader(spark: SparkSession, contract: Contract, schema: StructType | None = None):
    reader = spark.read.format(contract.format)
    for key, value in contract.read_options.items():
        reader = reader.option(key, value)
    return reader.schema(schema) if schema is not None else reader


def observed_schema(spark: SparkSession, contract: Contract, files: list[Path]) -> dict[str, str]:
    """Column name -> type category, as found in the batch files.

    CSV: no schema is given and inferSchema is off, so every column is a string; Spark only reads
    the header line. JSON: this is the one place a schema is inferred (it costs a full pass over the
    files). The result feeds drift detection only; it never defines a table.
    """
    df = _reader(spark, contract).load([str(f) for f in files])
    return {f.name: type_category(f.dataType) for f in df.schema.fields}


def _read_bronze_frame(
    spark: SparkSession, contract: Contract, files: list[Path], rename_mapping: dict[str, str], batch_id: str
) -> DataFrame:
    """Read the batch with an explicit all-strings schema and project it onto canonical names.

    Spark is lazy: nothing here reads data. `read`, `select` and `withColumn` only build a plan; the
    work happens when the write below (an action) runs.
    """
    observed_by_canonical = {canonical: observed for observed, canonical in rename_mapping.items()}
    schema = None
    if contract.format == "json":
        schema = StructType(
            [StructField(observed_by_canonical[c.canonical], _spark_type(c), True)
             for c in contract.columns if c.canonical in observed_by_canonical]
        )
    raw = _reader(spark, contract, schema).load([str(f) for f in files])

    columns: list[SparkColumn] = []
    for col in contract.columns:
        dtype = _spark_type(col)
        observed = observed_by_canonical.get(col.canonical)
        if observed is None:  # optional column absent from this batch: fill with nulls
            expr = F.lit(None).cast(dtype)
        elif col.type == "struct":
            expr = F.col(f"`{observed}`")
        else:
            expr = F.col(f"`{observed}`").cast("string")
        columns.append(expr.alias(col.canonical))

    return raw.select(
        *columns,
        F.lit(contract.vendor_id).alias("_vendor_id"),
        F.lit(batch_id).alias("_batch_id"),
        F.input_file_name().alias("_source_file"),  # which landing file each row came from
        F.current_timestamp().alias("_ingested_at"),
        # Increasing id in file order (one file per batch): silver uses it to find the latest of
        # several rows with the same key. Stored, so it never changes after the write.
        F.monotonically_increasing_id().alias("_row_id"),
        F.to_date(F.lit(batch_id)).alias("batch_date"),
    )


def ingest_batch(spark: SparkSession, config: dict, contract: Contract, batch_id: str) -> BatchOutcome:
    """Ingest one vendor batch into bronze. Safe to call again: finished batches are skipped."""
    vendor_id = contract.vendor_id
    ops.check_batch_id(batch_id)

    if ops.batch_status(spark, config, vendor_id, batch_id) == SUCCESS:
        return BatchOutcome(vendor_id, batch_id, SKIPPED, reason="already ingested")
    last_ok = ops.last_success(spark, config, vendor_id)
    if last_ok is not None and batch_id < last_ok:
        return BatchOutcome(vendor_id, batch_id, REJECTED_STALE, reason=f"older than last SUCCESS batch {last_ok}")

    files = batch_files(config, vendor_id, batch_id)
    file_names = [f.name for f in files]
    result = detect_drift(observed_schema(spark, contract, files), contract)
    ops.record_drifts(spark, config, vendor_id, batch_id, result.drifts)

    if result.verdict == QUARANTINE:
        reason = "; ".join(result.quarantine_reasons)
        ops.record_batch(spark, config, vendor_id, batch_id, QUARANTINED, None, reason, file_names)
        return BatchOutcome(vendor_id, batch_id, QUARANTINED, None, result.drifts, reason)

    df = _read_bronze_frame(spark, contract, files, dict(result.rename_mapping), batch_id)

    # On-disk partitioning: partitionBy("batch_date") makes one folder per date, so reading one
    # batch later touches only its folder. replaceWhere makes this write repeatable (see ops).
    path = bronze_path(contract, config)
    ops.replace_where(df, path, f"batch_date = DATE'{batch_id}'", partition_by="batch_date")

    rows = spark.read.format("delta").load(str(path)).filter(F.col("batch_date") == F.to_date(F.lit(batch_id))).count()
    ops.record_batch(spark, config, vendor_id, batch_id, SUCCESS, rows, None, file_names)
    return BatchOutcome(vendor_id, batch_id, SUCCESS, rows, result.drifts)
