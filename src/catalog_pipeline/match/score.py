"""Tier 2 scoring: a weighted blend of three similarities, each in [0, 1], plus hard vetoes."""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

GTIN = "GTIN"
FUZZY = "FUZZY"
_GTIN_SCORE = 2.0  # above any fuzzy score (at most 1.0), so a GTIN edge always wins a ranking
_FEATURES = ["record_key", "title_norm", "title_tokens", "model_norm", "gtin14", "price"]


def token_jaccard(a: Column, b: Column) -> Column:
    """Shared words / all distinct words. Robust to word order, hurt by typos."""
    union = F.size(F.array_union(a, b))
    return F.when(union > 0, F.size(F.array_intersect(a, b)) / union).otherwise(F.lit(0.0))


def levenshtein_ratio(a: Column, b: Column) -> Column:
    """1 - edits / longer length. Robust to typos, hurt by reordered words."""
    longest = F.greatest(F.length(a), F.length(b))
    return F.when(longest > 0, 1 - F.levenshtein(a, b) / longest).otherwise(F.lit(0.0))


def price_closeness(a: Column, b: Column) -> Column:
    """1 - relative price difference; 0 when either price is unknown."""
    hi = F.greatest(a, b)
    return F.coalesce(F.when(hi > 0, 1 - F.abs(a - b) / hi), F.lit(0.0)).cast("double")


def vetoed(a_model: Column, b_model: Column, a_gtin: Column, b_gtin: Column) -> Column:
    """Known-different identifiers: both sides have a model (or a GTIN) and they differ."""
    return (a_model.isNotNull() & b_model.isNotNull() & (a_model != b_model)) | (
        a_gtin.isNotNull() & b_gtin.isNotNull() & (a_gtin != b_gtin)
    )


def score_pairs(pairs: DataFrame, records: DataFrame, weights: dict[str, float]) -> DataFrame:
    """Candidate pairs (a_key, b_key) with their similarities, blended `score` and `vetoed` flag."""
    feats = records.select(*_FEATURES)
    a = feats.select(*[F.col(c).alias(f"a_{c}") for c in _FEATURES])
    b = feats.select(*[F.col(c).alias(f"b_{c}") for c in _FEATURES])
    joined = pairs.join(a, pairs.a_key == a.a_record_key).join(b, pairs.b_key == b.b_record_key)
    sims = joined.select(
        "a_key",
        "b_key",
        token_jaccard(F.col("a_title_tokens"), F.col("b_title_tokens")).alias("jaccard"),
        levenshtein_ratio(F.col("a_title_norm"), F.col("b_title_norm")).alias("levenshtein"),
        price_closeness(F.col("a_price"), F.col("b_price")).alias("price"),
        vetoed(F.col("a_model_norm"), F.col("b_model_norm"), F.col("a_gtin14"), F.col("b_gtin14")).alias("vetoed"),
    )
    score = sum(F.col(name) * float(w) for name, w in weights.items())
    return sims.withColumn("score", score)


def accepted(scored: DataFrame, threshold: float, min_price_closeness: float) -> DataFrame:
    """Fuzzy edges: not vetoed, prices close enough, and scoring at or above the threshold."""
    keep = ~F.col("vetoed") & (F.col("price") >= min_price_closeness) & (F.col("score") >= threshold)
    return scored.filter(keep).select("a_key", "b_key", "score")


def best_match_per_vendor(edges: DataFrame) -> DataFrame:
    """Keep an edge only if, on both sides, it is the record's best-scoring edge into the other vendor.

    A vendor lists each product once (D20), so a record can have at most one true partner per other
    vendor. edges: a_key, b_key, score, method. Ties go to the smaller key, so the result is deterministic.
    """
    vendor = lambda key: F.substring_index(key, "|", 1)  # noqa: E731  record_key = vendor_id|vendor_sku
    best_for_a = Window.partitionBy("a_key", vendor(F.col("b_key"))).orderBy(F.desc("score"), "b_key")
    best_for_b = Window.partitionBy("b_key", vendor(F.col("a_key"))).orderBy(F.desc("score"), "a_key")
    return (
        edges.withColumn("rank_a", F.row_number().over(best_for_a))
        .withColumn("rank_b", F.row_number().over(best_for_b))
        .filter((F.col("rank_a") == 1) & (F.col("rank_b") == 1))
        .drop("rank_a", "rank_b")
    )


def combine_edges(gtin_pairs: DataFrame, fuzzy: DataFrame, best_per_vendor: bool) -> DataFrame:
    """Final edges (a_key, b_key, score, method) from GTIN pairs and accepted fuzzy pairs.
    A pair found both ways counts as GTIN."""
    both = gtin_pairs.select("a_key", "b_key", F.lit(_GTIN_SCORE).alias("score")).unionByName(
        fuzzy.select("a_key", "b_key", F.col("score").cast("double"))
    )
    edges = both.groupBy("a_key", "b_key").agg(F.max("score").alias("score")).withColumn(
        "method", F.when(F.col("score") >= _GTIN_SCORE, GTIN).otherwise(FUZZY)
    )
    return best_match_per_vendor(edges) if best_per_vendor else edges
