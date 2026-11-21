"""Pairwise precision and recall of ER output against ground truth.

A "pair" is two records placed in the same cluster. Counting pairs per group with n*(n-1)/2
avoids materializing them, so a large wrong cluster cannot blow up the evaluation.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _pairs_in_groups(df: DataFrame, *group: str) -> int:
    n = F.count(F.lit(1))
    row = df.groupBy(*group).agg((n * (n - 1) / 2).alias("p")).agg(F.sum("p")).first()  # one scalar
    return int(row[0] or 0)


def _ratio(num: int, den: int) -> float | None:
    return num / den if den else None


def cluster_metrics(xref: DataFrame, truth: DataFrame) -> dict[str, float | int | None]:
    """xref: record_key, master_id. truth: record_key, true_product_id. Only records in both count."""
    j = xref.select("record_key", "master_id").join(truth.select("record_key", "true_product_id"), "record_key")
    predicted = _pairs_in_groups(j, "master_id")
    actual = _pairs_in_groups(j, "true_product_id")
    correct = _pairs_in_groups(j, "master_id", "true_product_id")
    return {
        "predicted_pairs": predicted,
        "true_pairs": actual,
        "correct_pairs": correct,
        "precision": _ratio(correct, predicted),
        "recall": _ratio(correct, actual),
    }


def edge_metrics(edges: DataFrame, truth: DataFrame) -> dict[str, int]:
    """edges: a_key, b_key. Returns how many edges link records of the same true product."""
    t = truth.select("record_key", "true_product_id")
    labelled = (
        edges.join(t.withColumnRenamed("record_key", "a_key").withColumnRenamed("true_product_id", "a_true"), "a_key")
        .join(t.withColumnRenamed("record_key", "b_key").withColumnRenamed("true_product_id", "b_true"), "b_key")
    )
    row = labelled.agg(
        F.count(F.lit(1)).alias("edges"), F.sum((F.col("a_true") == F.col("b_true")).cast("int")).alias("correct")
    ).first()  # one row
    return {"edges": int(row["edges"]), "correct_edges": int(row["correct"] or 0)}
