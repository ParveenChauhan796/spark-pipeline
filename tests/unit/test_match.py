from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from catalog_pipeline.config import load_config
from catalog_pipeline.match import candidates, cluster, evaluate, golden, score
from catalog_pipeline.transform.text import title_norm, title_tokens

PRODUCT = "vendor_id string, vendor_sku string, title string, brand string, category string, model string, gtin14 string"


def records(spark, rows):
    """rows: (vendor_id, sku, title, brand, model, gtin14, price, category)."""
    products = spark.createDataFrame([(v, s, t, b, c, m, g) for v, s, t, b, m, g, _, c in rows], PRODUCT)
    products = products.withColumn("title_norm", title_norm(F.col("title"))).withColumn(
        "title_tokens", title_tokens(F.col("title_norm"))
    )
    prices = spark.createDataFrame(
        [(f"{v}|{s}", Decimal(p)) for v, s, _, _, _, _, p, _ in rows], "offer_key string, price decimal(12,2)"
    )
    return candidates.prepare(products, prices)


def pairs(df):
    return sorted((r.a_key, r.b_key) for r in df.collect())


@pytest.fixture(scope="module")
def er():
    return load_config()["er"]


def fuzzy_edges(spark, er, rows):
    """The production tier-2 path on tiny input: block (pass 1), score, accept."""
    recs = records(spark, rows)
    cands, _ = candidates.block_pairs(candidates.model_token_keys(recs, er["blocking"]["min_token_length"]), 50)
    scored = score.score_pairs(cands, recs, er["scoring"]["weights"])
    return scored, score.accepted(scored, er["scoring"]["threshold"], er["scoring"]["min_price_closeness"])


def test_gtin_exact_match_is_cross_vendor_and_listed_once(spark):
    recs = records(spark, [
        ("A", "1", "Range", "Kenmore", None, "00491930664828", "10.00", None),
        ("B", "1", "Oven range", "Kenmore", None, "00491930664828", "11.00", None),
        ("B", "2", "Other listing", "Kenmore", None, "00491930664828", "12.00", None),  # same vendor as B|1
        ("C", "1", "Fan", "GE", None, "00000000000017", "10.00", None),
    ])

    assert pairs(candidates.gtin_pairs(recs)) == [("A|1", "B|1"), ("A|1", "B|2")]


def test_model_like_tokens_come_from_model_and_title(spark):
    recs = records(spark, [
        ("A", "1", "Whirlpool W10502642 Reservoir 2 pack", "Whirlpool", "WR-55X", None, "1.00", None),
        ("C", "1", "Fan", None, None, None, "1.00", None),  # brandless: no block keys (D21)
    ])
    keys = sorted((r.record_key, r.block_token) for r in candidates.model_token_keys(recs, 4).collect())

    assert keys == [("A|1", "w10502642"), ("A|1", "wr55x")]  # "2" and "pack" are not model-like


def test_blocking_pairs_cross_vendor_only_and_drops_oversized_blocks(spark):
    rows = [(v, s, "Whirlpool W10502642 part", "Whirlpool", None, None, "1.00", None) for v, s in
            [("A", "1"), ("A", "2"), ("B", "1")]]
    keys = candidates.model_token_keys(records(spark, rows), 4)

    kept, dropped = candidates.block_pairs(keys, max_block_size=3)
    assert pairs(kept) == [("A|1", "B|1"), ("A|2", "B|1")]
    assert dropped.count() == 0
    kept, dropped = candidates.block_pairs(keys, max_block_size=2)
    assert pairs(kept) == [] and dropped.count() == 1


def test_title_prefix_keys_skip_the_brand_words(spark):
    recs = records(spark, [("A", "1", "Whirlpool Ice Maker Kit", "Whirlpool", None, None, "1.00", None)])

    assert [r.block_token for r in candidates.title_prefix_keys(recs, 2).collect()] == ["ice maker"]


def test_typo_and_missing_model_still_match(spark, er):
    _, edges = fuzzy_edges(spark, er, [
        ("A", "1", "Whirlpool W10502642 Refrigerator Reservoir", "Whirlpool", "W10502642", None, "20.00", None),
        ("C", "1", "Whirlpool W10502642 Refrigerator Resevoir", "Whirlpool", None, None, "21.00", None),
    ])

    assert pairs(edges) == [("A|1", "C|1")]


def test_different_model_is_never_a_match(spark, er):
    scored, edges = fuzzy_edges(spark, er, [
        ("A", "1", "Whirlpool W10502642 Refrigerator Reservoir", "Whirlpool", "W10502642", None, "20.00", None),
        ("B", "1", "Whirlpool W10502642 Refrigerator Reservoir", "Whirlpool", "W10502643", None, "20.00", None),
    ])

    assert scored.first().vetoed and pairs(edges) == []


def test_same_title_but_distant_price_is_not_a_match(spark, er):
    _, edges = fuzzy_edges(spark, er, [
        ("A", "1", "Supco RIM597 Universal Icemaker", "Supco", None, None, "20.00", None),
        ("B", "1", "Supco RIM597 Universal Icemaker", "Supco", None, None, "150.00", None),
    ])

    assert pairs(edges) == []


def test_similarities(spark):
    df = spark.createDataFrame(
        [(["a", "b", "c"], ["b", "c", "d"], "kitten", "sitting", Decimal("90"), Decimal("100"))],
        "x array<string>, y array<string>, s string, t string, p decimal(12,2), q decimal(12,2)",
    )
    r = df.select(
        score.token_jaccard(F.col("x"), F.col("y")).alias("j"),
        score.levenshtein_ratio(F.col("s"), F.col("t")).alias("l"),
        score.price_closeness(F.col("p"), F.col("q")).alias("p"),
        score.price_closeness(F.col("p"), F.lit(None)).alias("p_null"),
    ).first()

    assert (r.j, round(r.l, 4), round(r.p, 4), r.p_null) == (0.5, round(1 - 3 / 7, 4), 0.9, 0.0)


def test_best_match_per_vendor_keeps_only_mutual_best_edges(spark):
    edges = spark.createDataFrame(
        [("A|1", "B|1", 0.9), ("A|1", "B|2", 0.8), ("A|2", "B|2", 0.95), ("A|1", "C|1", 0.7)],
        "a_key string, b_key string, score double",
    )
    # A|1 -> B|2 is not A|1's best in B; C|1 is A|1's best (and only) edge into C.
    assert pairs(score.best_match_per_vendor(edges)) == [("A|1", "B|1"), ("A|1", "C|1"), ("A|2", "B|2")]


def test_combine_edges_prefers_gtin(spark):
    gtin = spark.createDataFrame([("A|1", "B|1")], "a_key string, b_key string")
    fuzzy = spark.createDataFrame([("A|1", "B|1", 0.9), ("A|2", "B|2", 0.8)], "a_key string, b_key string, score double")

    got = {(r.a_key, r.b_key): r.method for r in score.combine_edges(gtin, fuzzy, True).collect()}

    assert got == {("A|1", "B|1"): "GTIN", ("A|2", "B|2"): "FUZZY"}


def label_map(spark, keys, edge_list, rounds=3):
    nodes = spark.createDataFrame([(k,) for k in keys], "record_key string")
    edges = spark.createDataFrame(edge_list, "a_key string, b_key string")
    return {r.record_key: r.master_id for r in cluster.label_propagation(nodes, edges, rounds).collect()}


def test_three_record_chain_ends_in_one_cluster(spark):
    # A|1-B|1 and B|1-C|1 matched; A|1 and C|1 never compared directly.
    labels = label_map(spark, ["A|1", "B|1", "C|1", "D|1"], [("A|1", "B|1"), ("B|1", "C|1")])

    assert labels == {"A|1": "A|1", "B|1": "A|1", "C|1": "A|1", "D|1": "D|1"}


def test_chain_longer_than_the_rounds_stays_split(spark):
    keys = ["n1", "n2", "n3", "n4", "n5"]
    labels = label_map(spark, keys, list(zip(keys, keys[1:])), rounds=3)

    assert labels["n4"] == "n1" and labels["n5"] == "n2"  # the label travels one hop per round (D25)


def test_known_limitation_a_record_without_model_can_chain_two_models(spark, er):
    # A and D conflict directly (different models), but C has no model, so A-C and C-D both pass and
    # clustering puts A and D together (D25). A cluster-level model check would be needed to stop it.
    rows = [
        ("A", "1", "Whirlpool W10502642 Refrigerator Reservoir", "Whirlpool", "W10502642", None, "20.00", None),
        ("C", "1", "Whirlpool Refrigerator Reservoir W10502642", "Whirlpool", None, None, "20.00", None),
        ("D", "1", "Whirlpool W10502642 Refrigerator Reservoir", "Whirlpool", "W10502643", None, "20.00", None),
    ]
    _, edges = fuzzy_edges(spark, er, rows)
    labels = label_map(spark, ["A|1", "C|1", "D|1"], pairs(edges))

    assert pairs(edges) == [("A|1", "C|1"), ("C|1", "D|1")]
    assert set(labels.values()) == {"A|1"}


def test_xref_methods_and_golden_record_choice(spark):
    recs = records(spark, [
        ("A", "1", "Range", "Kenmore", "K30", None, "10.00", None),  # 2 of 4 attributes
        ("B", "1", "Range 30", "Kenmore", "K30", "00491930664828", "10.00", None),  # 3 of 4: golden
        ("C", "1", "Range", "Kenmore", "K30", "00491930664828", "10.00", None),  # 3 of 4, lower priority
        ("D", "1", "Fan", "GE", None, None, "5.00", "Fans"),  # singleton
    ])
    edges = spark.createDataFrame(
        [("B|1", "C|1", "GTIN"), ("A|1", "B|1", "FUZZY")], "a_key string, b_key string, method string"
    )
    labels = cluster.label_propagation(recs.select("record_key"), edges, 3)

    xref = golden.build_xref(recs, labels, edges)
    dim = golden.build_dim(recs, xref, ["A", "B", "D", "C"])

    assert {r.record_key: (r.master_id, r.match_method) for r in xref.collect()} == {
        "A|1": ("A|1", "FUZZY"), "B|1": ("A|1", "GTIN"), "C|1": ("A|1", "GTIN"), "D|1": ("D|1", "SINGLETON"),
    }
    got = {r.master_id: (r.golden_record_key, r.title, r.n_records, r.n_vendors) for r in dim.collect()}
    assert got == {"A|1": ("B|1", "Range 30", 3, 3), "D|1": ("D|1", "Fan", 1, 1)}


def test_pairwise_metrics_hand_computed(spark):
    # Predicted clusters {1,2,3} {4}; truth {1,2} {3,4}.
    # Predicted pairs: 12 13 23 = 3. True pairs: 12 34 = 2. Correct: 12 = 1.
    xref = spark.createDataFrame([("1", "m1"), ("2", "m1"), ("3", "m1"), ("4", "m4")], "record_key string, master_id string")
    truth = spark.createDataFrame([("1", "p"), ("2", "p"), ("3", "q"), ("4", "q")], "record_key string, true_product_id string")

    m = evaluate.cluster_metrics(xref, truth)

    assert (m["predicted_pairs"], m["true_pairs"], m["correct_pairs"]) == (3, 2, 1)
    assert (m["precision"], m["recall"]) == (1 / 3, 1 / 2)
    edges = spark.createDataFrame([("1", "2"), ("2", "3")], "a_key string, b_key string")
    assert evaluate.edge_metrics(edges, truth) == {"edges": 2, "correct_edges": 1}
