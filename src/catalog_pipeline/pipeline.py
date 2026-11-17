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
from catalog_pipeline.ingest import ops
from catalog_pipeline.ingest.bronze import SKIPPED, SUCCESS, BatchOutcome, bronze_path, ingest_batch
from catalog_pipeline.ingest.contracts import Contract, load_contracts
from catalog_pipeline.paths import resolve, table_path
from catalog_pipeline.spark import get_spark
from catalog_pipeline.transform import silver as silver_tf

DONE = "DONE"
ALREADY_DONE = "ALREADY_DONE"
NOT_INGESTED = "NOT_INGESTED"


@dataclass(frozen=True)
class SilverOutcome:
    vendor_id: str
    batch_id: str
    status: str  # DONE | ALREADY_DONE | NOT_INGESTED
    counts: dict[str, int] | None = None
    reasons: dict[str, int] | None = None


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


def run_all(spark: SparkSession, config: dict) -> list[tuple[BatchOutcome, SilverOutcome]]:
    """Run every landed batch through bronze and silver, oldest day first. One vendor's quarantined
    batch never stops the others."""
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
        results.append((bronze_outcome, silver_outcome))
        print(_describe(bronze_outcome, silver_outcome, time.perf_counter() - start))
    return results


def _describe(b: BatchOutcome, s: SilverOutcome, seconds: float) -> str:
    drift = ", ".join(f"{d.drift_type}({d.source_column})" for d in b.drifts) or "-"
    bronze = f"bronze {b.outcome}" + (f" rows={b.rows}" if b.rows is not None else f" ({b.reason})")
    silver = f"silver {s.status}"
    if s.counts:
        c = s.counts
        silver += f" in={c['rows_in']} valid={c['valid']} quarantined={c['quarantined']} deduped={c['deduped']}"
    return f"vendor {b.vendor_id} {b.batch_id}: {bronze}; drift: {drift}; {silver} [{seconds:.1f}s]"


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
