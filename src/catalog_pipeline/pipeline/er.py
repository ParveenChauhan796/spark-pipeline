"""Cross-vendor entity resolution over all of silver.vendor_product (the matching logic is match/)."""
import hashlib
import json
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from catalog_pipeline.ingest import ops
from catalog_pipeline.match import candidates, cluster, evaluate, golden, score
from catalog_pipeline.paths import er_table_path, offer_history_path, resolve, silver_table_path
from catalog_pipeline.pipeline.history import table_version
from catalog_pipeline.pipeline.outcomes import ALREADY_DONE, DONE, NOT_READY, ErOutcome


def load_ground_truth(spark: SparkSession, config: dict) -> DataFrame:
    """record_key, true_product_id from the generator's ground truth (evaluation only)."""
    path = resolve(config["er"]["ground_truth_path"])
    truth = spark.read.format("csv").option("header", True).schema(
        "vendor_id string, vendor_sku string, true_product_id string"
    ).load(str(path))
    return truth.select(F.concat_ws("|", "vendor_id", "vendor_sku").alias("record_key"), "true_product_id")


def resolve_entities(spark: SparkSession, config: dict, force: bool = False) -> ErOutcome:
    """Cross-vendor entity resolution over all of silver.vendor_product (full recompute, D19).

    Writes gold.product_xref and gold.dim_product (overwritten) and one ops.er_run_log row. Skipped
    when the input table versions are the ones the last run used (D29), unless `force`.
    """
    product_path = silver_table_path("vendor_product", config)
    history_path = offer_history_path(config)
    if not (DeltaTable.isDeltaTable(spark, str(product_path)) and DeltaTable.isDeltaTable(spark, str(history_path))):
        return ErOutcome(NOT_READY)
    product_version = table_version(DeltaTable.forPath(spark, str(product_path)))
    history_version = table_version(DeltaTable.forPath(spark, str(history_path)))
    config_hash = hashlib.sha256(json.dumps(config["er"], sort_keys=True).encode()).hexdigest()
    if not force and ops.er_done(spark, config, product_version, history_version, config_hash):
        return ErOutcome(ALREADY_DONE)

    start = time.perf_counter()
    er = config["er"]
    products = spark.read.format("delta").option("versionAsOf", product_version).load(str(product_path))
    prices = (
        spark.read.format("delta").option("versionAsOf", history_version).load(str(history_path))
        .filter(F.col("is_current")).select("offer_key", "price")
    )
    # localCheckpoint() (not persist()) for every ER step: it stores the computed rows AND forgets how
    # they were made. A persisted DataFrame keeps its whole recipe, and each later query embeds it, so
    # after seven chained steps one evaluation query had a 1.1 MB plan that AQE re-printed on every
    # re-plan, until the driver ran out of heap. Checkpointed steps start each query from a flat plan
    # (~3 KB). Trade-off: lost blocks cannot be recomputed, which is fine with one local process.
    records = candidates.prepare(products, prices).localCheckpoint()
    gtin_pairs = candidates.gtin_pairs(records).localCheckpoint()
    (pass1, dropped1), (pass2, dropped2) = [
        candidates.block_pairs(keys, er["blocking"]["max_block_size"])
        for keys in candidates.block_key_passes(records, er["blocking"])
    ]
    pass1 = pass1.localCheckpoint()
    candidate_pairs = pass1.unionByName(pass2).distinct().localCheckpoint()
    scored = score.score_pairs(candidate_pairs, records, er["scoring"]["weights"])
    scoring = er["scoring"]
    fuzzy = score.accepted(scored, scoring["threshold"], scoring["min_price_closeness"])
    fuzzy = fuzzy.localCheckpoint()
    edges = score.combine_edges(gtin_pairs, fuzzy, scoring["best_match_per_vendor"])
    edges = edges.localCheckpoint()

    labels = cluster.label_propagation(records.select("record_key"), edges, er["label_propagation_rounds"])
    xref = golden.build_xref(records, labels, edges).localCheckpoint()
    dim = golden.build_dim(records, xref, er["golden"]["vendor_priority"])
    for df, name in ((xref, "product_xref"), (dim, "dim_product")):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(str(er_table_path(name, config)))

    truth = load_ground_truth(spark, config)
    edge_eval = evaluate.edge_metrics(edges, truth)
    metrics = {
        "product_version": product_version,
        "history_version": history_version,
        "config_hash": config_hash,
        "records": records.count(),
        "gtin_pairs": gtin_pairs.count(),
        "candidate_pairs_pass1": pass1.count(),
        "candidate_pairs": candidate_pairs.count(),
        "blocks_dropped": dropped1.count() + dropped2.count(),
        "fuzzy_edges": fuzzy.count(),
        "edges": edge_eval["edges"],
        "edge_precision": edge_eval["correct_edges"] / edge_eval["edges"] if edge_eval["edges"] else None,
        "masters": xref.select("master_id").distinct().count(),
        "same_vendor_clusters": golden.same_vendor_clusters(xref).count(),
        **evaluate.cluster_metrics(xref, truth),
    }
    metrics["seconds"] = round(time.perf_counter() - start, 1)
    ops.record_er_run(spark, config, metrics)
    return ErOutcome(DONE, metrics)
