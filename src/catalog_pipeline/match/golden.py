"""gold.product_xref and gold.dim_product: map every vendor record to a master and pick its golden record."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from catalog_pipeline.match.score import FUZZY, GTIN

SINGLETON = "SINGLETON"
_COMPLETENESS_COLUMNS = ["brand", "category", "model", "gtin14"]


def build_xref(records: DataFrame, labels: DataFrame, edges: DataFrame) -> DataFrame:
    """vendor_id, vendor_sku, record_key, master_id, match_method (GTIN > FUZZY > SINGLETON).

    edges: a_key, b_key, method (GTIN | FUZZY), the final edges that clustering used.
    """
    ends = edges.select(F.col("a_key").alias("record_key"), "method").unionByName(
        edges.select(F.col("b_key").alias("record_key"), "method")
    )
    methods = ends.groupBy("record_key").agg(
        F.max((F.col("method") == GTIN).cast("int")).alias("has_gtin"),
        F.max((F.col("method") == FUZZY).cast("int")).alias("has_fuzzy"),
    )
    method = F.when(F.col("has_gtin") == 1, GTIN).when(F.col("has_fuzzy") == 1, FUZZY).otherwise(SINGLETON)
    return (
        records.select("record_key", "vendor_id", "vendor_sku")
        .join(labels, "record_key")
        .join(methods, "record_key", "left")
        .select("vendor_id", "vendor_sku", "record_key", "master_id", method.alias("match_method"))
    )


def build_dim(records: DataFrame, xref: DataFrame, vendor_priority: list[str]) -> DataFrame:
    """One golden record per master: the most complete record, then vendor priority, then smallest key."""
    completeness = sum(F.col(c).isNotNull().cast("int") for c in _COMPLETENESS_COLUMNS)
    priority = F.coalesce(
        F.element_at(F.create_map(*[F.lit(x) for i, v in enumerate(vendor_priority) for x in (v, i)]), F.col("vendor_id")),
        F.lit(len(vendor_priority)),  # vendors missing from the list come last
    )
    members = records.join(xref.select("record_key", "master_id"), "record_key")
    best_first = Window.partitionBy("master_id").orderBy(completeness.desc(), priority.asc(), F.col("record_key").asc())
    whole_cluster = Window.partitionBy("master_id")
    return (
        members.withColumn("rn", F.row_number().over(best_first))
        .withColumn("n_records", F.count(F.lit(1)).over(whole_cluster))
        .withColumn("n_vendors", F.size(F.collect_set("vendor_id").over(whole_cluster)))
        .filter(F.col("rn") == 1)
        .select(
            "master_id",
            "title",
            "brand",
            "category",
            "model",
            "gtin14",
            F.col("record_key").alias("golden_record_key"),
            "n_records",
            "n_vendors",
        )
    )


def same_vendor_clusters(xref: DataFrame) -> DataFrame:
    """Masters holding two or more records of one vendor: a sign of a wrong merge (D25)."""
    return (
        xref.groupBy("master_id", "vendor_id").agg(F.count(F.lit(1)).alias("n"))
        .filter(F.col("n") > 1).select("master_id").distinct()
    )
