"""Brand canonicalization via a small reference table."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def brand_key(brand: Column, legal_suffixes: list[str]) -> Column:
    """Comparison key of a brand: lowercase, no punctuation, no trailing legal words, no spaces.

    Spark twin of generate.brands.brand_key (a test checks both agree).
    """
    s = F.trim(F.regexp_replace(F.lower(brand), r"[^\p{L}\p{N}]+", " "))
    s = F.regexp_replace(s, r"( (" + "|".join(legal_suffixes) + r"))+$", "")
    s = F.regexp_replace(s, " ", "")
    return F.when(s == "", None).otherwise(s)


def canonicalize_brand(
    df: DataFrame, aliases: DataFrame, legal_suffixes: list[str], broadcast: bool = True
) -> DataFrame:
    """Add `brand` (canonical spelling, else the trimmed original; null if blank) next to `brand_raw`.

    `aliases` has columns brand_key, canonical_brand. It is tiny, so it is joined with a broadcast
    join: Spark copies the whole table to every task and looks brands up locally. The big side is
    never shuffled (a normal join would redistribute both sides by key across the network first).
    `broadcast=False` drops the hint, so the bench can measure what it buys (D40).
    """
    keyed = df.withColumn("brand_raw", F.col("brand")).withColumn("_brand_key", brand_key(F.col("brand_raw"), legal_suffixes))
    lookup = F.broadcast(aliases) if broadcast else aliases
    joined = keyed.join(lookup, keyed["_brand_key"] == lookup["brand_key"], "left")
    return joined.select(
        *[c for c in keyed.columns if c not in ("brand", "_brand_key")],
        F.coalesce(lookup["canonical_brand"], F.nullif(F.trim(F.col("brand_raw")), F.lit(""))).alias("brand"),
    )
