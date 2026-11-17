"""Pure report builders: attribute completeness, price changes by brand, cross-vendor price spread."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CATALOGUE = "CATALOGUE"  # completeness scope of the merged catalogue (gold.dim_product)
UNKNOWN_BRAND = "UNKNOWN"


def _completeness(df: DataFrame, scope, columns: list[str]) -> DataFrame:
    """scope, attribute, non_null, total, completeness for one input. count(col) skips nulls, so
    count(col) / count(*) is the share of rows that have a value."""
    wide = df.groupBy(scope.alias("scope")).agg(
        F.count(F.lit(1)).alias("total"), *[F.count(c).alias(c) for c in columns]
    )
    # unpivot turns the one-column-per-attribute row into one row per attribute (wide -> long), so
    # the report keeps the same shape whatever the column list is.
    long = wide.unpivot(["scope", "total"], columns, "attribute", "non_null")
    return long.select(
        "scope", "attribute", "non_null", "total", (F.col("non_null") / F.col("total")).alias("completeness")
    )


def attribute_completeness(vendor_products: DataFrame, dim_product: DataFrame, columns: list[str]) -> DataFrame:
    """Per vendor (silver.vendor_product) and for the merged catalogue (gold.dim_product). D32."""
    return _completeness(vendor_products, F.col("vendor_id"), columns).unionByName(
        _completeness(dim_product, F.lit(CATALOGUE), columns)
    )


def price_changes(history: DataFrame) -> DataFrame:
    """One row per price change: offer_key, change_date, old_price, new_price, pct_change. D33.

    lag() reads the previous version of the same offer (window per offer_key, oldest first). Versions
    whose price did not move (stock-only changes) and each offer's first version are not changes.
    """
    by_offer = Window.partitionBy("offer_key").orderBy("valid_from")
    with_prev = history.withColumn("old_price", F.lag("price").over(by_offer))
    return with_prev.filter(F.col("old_price").isNotNull() & (F.col("price") != F.col("old_price"))).select(
        "offer_key",
        F.col("valid_from").alias("change_date"),
        "old_price",
        F.col("price").alias("new_price"),
        ((F.col("price") - F.col("old_price")) / F.col("old_price") * 100).cast("double").alias("pct_change"),
    )


def price_changes_by_brand(changes: DataFrame, xref: DataFrame, dim_product: DataFrame) -> DataFrame:
    """brand, change_date, changes, increases, decreases, avg_pct_change (unweighted mean). D34, D35."""
    brand = F.coalesce(F.col("brand"), F.lit(UNKNOWN_BRAND)).alias("brand")
    with_brand = changes.join(xref.select(F.col("record_key").alias("offer_key"), "master_id"), "offer_key", "left").join(
        dim_product.select("master_id", "brand"), "master_id", "left"
    )
    return with_brand.groupBy(brand, "change_date").agg(
        F.count(F.lit(1)).alias("changes"),
        F.sum((F.col("pct_change") > 0).cast("int")).alias("increases"),
        F.sum((F.col("pct_change") < 0).cast("int")).alias("decreases"),
        F.avg("pct_change").alias("avg_pct_change"),
    )


def price_spread(history: DataFrame, xref: DataFrame, min_vendors: int) -> DataFrame:
    """Current cross-vendor price range per master: master_id, n_vendors, n_offers, min_price,
    max_price, spread_pct = (max - min) / min * 100. Only masters sold by `min_vendors`+ vendors. D37."""
    current = history.filter(F.col("is_current")).select("offer_key", "vendor_id", "price")
    offers = current.join(xref.select(F.col("record_key").alias("offer_key"), "master_id"), "offer_key")
    return (
        offers.groupBy("master_id")
        .agg(
            F.countDistinct("vendor_id").alias("n_vendors"),
            F.count(F.lit(1)).alias("n_offers"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
        )
        .filter(F.col("n_vendors") >= min_vendors)
        .withColumn("spread_pct", ((F.col("max_price") - F.col("min_price")) / F.col("min_price") * 100).cast("double"))
    )
