"""ER inputs and candidate pairs: GTIN edges (tier 1) and blocked pairs for fuzzy scoring (tier 2).

Comparing every record with every other is ~23 billion pairs for 217k records. Blocking only
compares records that share a cheap key, here (brand, model-like token) and (brand, title prefix).
All pairs are cross-vendor and listed once, as (a_key, b_key) with a_key < b_key.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

_NON_ALNUM = r"[^a-z0-9]"


def model_norm(model: Column) -> Column:
    """Lowercase letters and digits only ("WR-55X10942" -> "wr55x10942"); null when nothing is left."""
    s = F.regexp_replace(F.lower(model), _NON_ALNUM, "")
    return F.when(s == "", None).otherwise(s)


def prepare(products: DataFrame, current_prices: DataFrame) -> DataFrame:
    """One row per vendor record with everything matching needs.

    products:       silver.vendor_product
    current_prices: offer_key, price (the is_current rows of gold.offer_history)
    """
    key = F.concat_ws("|", "vendor_id", "vendor_sku")
    return (
        products.withColumn("record_key", key)
        .join(current_prices.select(F.col("offer_key").alias("record_key"), "price"), "record_key", "left")
        .select(
            "record_key",
            "vendor_id",
            "vendor_sku",
            "title",
            "title_norm",
            "title_tokens",
            "brand",
            F.lower("brand").alias("brand_key"),
            "category",
            "model",
            model_norm(F.col("model")).alias("model_norm"),
            "gtin14",
            "price",
        )
    )


def _ordered_pairs(left: DataFrame, right: DataFrame, on: list[str]) -> DataFrame:
    """Self-join on `on`: cross-vendor pairs, each listed once (a_key < b_key)."""
    a = left.select(*on, F.col("record_key").alias("a_key"), F.col("vendor_id").alias("a_vendor"))
    b = right.select(*on, F.col("record_key").alias("b_key"), F.col("vendor_id").alias("b_vendor"))
    return (
        a.join(b, on)
        .filter((F.col("a_vendor") != F.col("b_vendor")) & (F.col("a_key") < F.col("b_key")))
        .select("a_key", "b_key")
    )


def gtin_pairs(records: DataFrame) -> DataFrame:
    """Tier 1: records with the same gtin14 are the same product."""
    with_gtin = records.filter(F.col("gtin14").isNotNull())
    return _ordered_pairs(with_gtin, with_gtin, ["gtin14"]).distinct()


def model_token_keys(records: DataFrame, min_token_length: int) -> DataFrame:
    """Pass 1 block keys: (brand_key, token) for the normalized model and every title token that has
    both letters and digits and is at least `min_token_length` long. Brandless records get no keys."""
    # F.filter with a lambda is Spark's built-in higher-order function: the lambda only builds a
    # column expression once, and the filtering itself runs inside the JVM (it is not a Python UDF).
    model_like = F.filter(
        "title_tokens",
        lambda t: t.rlike("[a-z]") & t.rlike("[0-9]") & (F.length(t) >= min_token_length),
    )
    model = F.when(F.col("model_norm").isNotNull(), F.array("model_norm")).otherwise(F.array().cast("array<string>"))
    tokens = F.array_distinct(F.concat(model_like, model))
    # explode turns one row with an array into one row per element: one row per (record, block key).
    return records.filter(F.col("brand_key").isNotNull()).select(
        "record_key", "vendor_id", "brand_key", F.explode(tokens).alias("block_token")
    )


def title_prefix_keys(records: DataFrame, n_tokens: int) -> DataFrame:
    """Pass 2 block keys: (brand_key, first `n_tokens` title words once the brand's own words are removed)."""
    brand_words = F.split(F.trim(F.regexp_replace("brand_key", r"[^\p{L}\p{N}]+", " ")), " ")
    prefix = F.concat_ws(" ", F.slice(F.array_except("title_tokens", brand_words), 1, n_tokens))
    return (
        records.filter(F.col("brand_key").isNotNull())
        .select("record_key", "vendor_id", "brand_key", prefix.alias("block_token"))
        .filter(F.col("block_token") != "")
    )


def block_pairs(keys: DataFrame, max_block_size: int) -> tuple[DataFrame, DataFrame]:
    """Candidate pairs from block keys, plus the oversized blocks that were skipped.

    Returns (pairs, dropped_blocks). A pair sharing several keys is listed once.
    """
    block = ["brand_key", "block_token"]
    keys = keys.dropDuplicates(["record_key", *block])
    sizes = keys.groupBy(*block).agg(F.count(F.lit(1)).alias("block_size"))
    kept = keys.join(sizes.filter(F.col("block_size").between(2, max_block_size)), block).drop("block_size")
    return _ordered_pairs(kept, kept, block).distinct(), sizes.filter(F.col("block_size") > max_block_size)
