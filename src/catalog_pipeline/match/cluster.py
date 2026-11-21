"""Clustering matched records with fixed-round label propagation (connected components, approximately)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def label_propagation(nodes: DataFrame, edges: DataFrame, rounds: int) -> DataFrame:
    """(record_key, master_id): every record ends up labelled with the smallest key it can reach
    within `rounds` hops. A cluster whose records are more than `rounds` hops apart can stay split.

    nodes: record_key. edges: a_key, b_key (undirected).
    """
    both_ways = edges.select(F.col("a_key").alias("src"), F.col("b_key").alias("dst")).unionByName(
        edges.select(F.col("b_key").alias("src"), F.col("a_key").alias("dst"))
    )
    labels = nodes.select("record_key", F.col("record_key").alias("label"))
    for _ in range(rounds):
        from_neighbours = both_ways.join(labels.withColumnRenamed("record_key", "dst"), "dst").select(
            F.col("src").alias("record_key"), "label"
        )
        labels = labels.unionByName(from_neighbours).groupBy("record_key").agg(F.min("label").alias("label"))
        # Each round adds another join to the query plan. localCheckpoint() computes the labels now and
        # cuts that lineage, so round 3 does not re-run rounds 1-2 and the plan stays small.
        labels = labels.localCheckpoint()
    return labels.withColumnRenamed("label", "master_id")
