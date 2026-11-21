from catalog_pipeline import bench
from catalog_pipeline.config import load_config
from catalog_pipeline.transform.brand import canonicalize_brand

KEYS = "record_key string, vendor_id string, brand_key string, block_token string"


def test_variant_config_uses_own_lake_and_switches_broadcast_off():
    config = load_config()

    baseline = bench.variant_config(config, "baseline")
    tuned = bench.variant_config(config, "tuned")

    assert baseline["lake"]["root"].endswith("bench/baseline/lake")
    assert baseline["spark"]["shuffle_partitions"] == 200
    assert baseline["silver"]["broadcast_aliases"] is False
    assert baseline["spark"]["extra_conf"]["spark.sql.autoBroadcastJoinThreshold"] == -1
    assert tuned["silver"]["broadcast_aliases"] is True
    assert "spark.sql.autoBroadcastJoinThreshold" not in (tuned["spark"].get("extra_conf") or {})
    assert config["lake"]["root"] == "data/lake"  # the real config is not modified


def test_cross_vendor_pairs_hand_computed(spark):
    rows = [
        ("A|1", "A", "acme", "x"), ("A|2", "A", "acme", "x"), ("B|1", "B", "acme", "x"), ("C|1", "C", "acme", "x"),
        ("A|3", "A", "acme", "y"), ("A|4", "A", "acme", "y"),  # one vendor only: no cross-vendor pairs
        ("A|5", "A", "zeta", "x"), ("D|1", "D", "zeta", "x"),
    ]
    keys = spark.createDataFrame(rows, KEYS)

    # block (acme, x): n=4, vendors A=2 B=1 C=1 -> (16 - 6) / 2 = 5 (A1-B1, A1-C1, A2-B1, A2-C1, B1-C1)
    # block (acme, y): 0; block (zeta, x): 1
    assert bench.cross_vendor_pairs(keys, ["brand_key", "block_token"]) == 6


def test_brand_only_keys_put_a_whole_brand_in_one_block(spark):
    records = spark.createDataFrame(
        [("A|1", "A", "acme"), ("B|1", "B", "acme"), ("C|1", "C", None)], "record_key string, vendor_id string, brand_key string"
    )

    got = sorted((r.record_key, r.brand_key, r.block_token) for r in bench.brand_only_keys(records).collect())

    assert got == [("A|1", "acme", ""), ("B|1", "acme", "")]  # brandless records get no block


def test_join_operators_sees_the_alias_join_without_broadcast(spark):
    aliases = spark.createDataFrame([("acme", "Acme")], "brand_key string, canonical_brand string")
    df = spark.createDataFrame([(1, "ACME")], "id int, brand string")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        plain = canonicalize_brand(df, aliases, ["inc"], broadcast=False)
        assert bench.join_operators(plain, "_brand_key") == ["SortMergeJoin"]
        hinted = canonicalize_brand(df, aliases, ["inc"], broadcast=True)  # an explicit hint still broadcasts
        assert bench.join_operators(hinted, "_brand_key") == ["BroadcastHashJoin"]
    finally:
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
