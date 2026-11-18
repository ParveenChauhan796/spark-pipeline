"""Bronze -> silver for one vendor batch (I/O and logging; the cleaning itself is transform/silver.py)."""
from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from catalog_pipeline.ingest import ops
from catalog_pipeline.ingest.bronze import SUCCESS, bronze_path
from catalog_pipeline.ingest.contracts import Contract
from catalog_pipeline.paths import resolve, silver_table_path
from catalog_pipeline.pipeline.outcomes import ALREADY_DONE, DONE, NOT_INGESTED, SilverOutcome
from catalog_pipeline.transform import silver as silver_tf


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
        ops.replace_where(silver_tf.to_offers(classified), silver_table_path("vendor_offer", config), predicate, "batch_date")
        ops.replace_where(silver_tf.to_quarantine(classified), silver_table_path("quarantine", config), predicate, "batch_date")
        _upsert_products(spark, silver_table_path("vendor_product", config), silver_tf.to_products(classified))

        reasons = {
            r["reason"]: r["n"]
            for r in classified.filter(~F.col("is_valid")).groupBy("reason").agg(F.count(F.lit(1)).alias("n")).collect()
        }  # a handful of rows: one per reason
        ops.record_silver(spark, config, vendor_id, batch_id, counts)
    finally:
        classified.unpersist()
    return SilverOutcome(vendor_id, batch_id, DONE, counts, reasons)
