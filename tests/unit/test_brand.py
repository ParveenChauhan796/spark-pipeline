from pyspark.sql import functions as F

from catalog_pipeline.config import load_config
from catalog_pipeline.generate.brands import brand_key as py_brand_key
from catalog_pipeline.transform.brand import brand_key, canonicalize_brand

SUFFIXES = load_config()["normalization"]["brand_legal_suffixes"]
BRANDS = ["Acme", "General Electric", "Whirlpool", "GE Appliances", "Frigidaire, Inc.", "A&B Tools", "Café Roma", "Co", "Inc."]


def variants(brand):
    """The spellings the generator produces for a brand."""
    return [brand, f"{brand} Inc.", f"{brand} Co.", f"{brand} Corp", brand.upper(), brand.lower(), brand.replace(" ", "")]


def test_spark_key_matches_python_key(spark):
    names = [v for b in BRANDS for v in variants(b)]
    df = spark.createDataFrame([(n,) for n in names], "b string").select("b", brand_key(F.col("b"), SUFFIXES).alias("k"))
    for row in df.collect():
        assert row.k == py_brand_key(row.b, SUFFIXES), row.b


def test_every_generated_variant_maps_to_the_same_key():
    for brand in ["Acme", "General Electric", "Whirlpool"]:
        assert {py_brand_key(v, SUFFIXES) for v in variants(brand)} == {py_brand_key(brand, SUFFIXES)}


def test_canonicalize_brand_uses_broadcast_join_and_falls_back_to_raw(spark):
    aliases = spark.createDataFrame([("acme", "Acme"), ("whirlpool", "Whirlpool")], "brand_key string, canonical_brand string")
    df = spark.createDataFrame(
        [(1, "ACME Inc."), (2, "acme"), (3, "Whirlpool Corp"), (4, "  Unknown Co "), (5, None), (6, "")], "id int, brand string"
    )

    out = canonicalize_brand(df, aliases, SUFFIXES)

    assert [(r.id, r.brand_raw, r.brand) for r in out.orderBy("id").collect()] == [
        (1, "ACME Inc.", "Acme"),
        (2, "acme", "Acme"),
        (3, "Whirlpool Corp", "Whirlpool"),
        (4, "  Unknown Co ", "Unknown Co"),  # not in the table: trimmed original
        (5, None, None),
        (6, "", None),  # blank is null
    ]
    assert "BroadcastHashJoin" in out._jdf.queryExecution().executedPlan().toString()
