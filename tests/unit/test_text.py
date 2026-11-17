from pyspark.sql import functions as F

from catalog_pipeline.transform.text import category_norm, title_norm, title_tokens


def test_title_norm_and_tokens(spark):
    rows = [
        ('Kenmore 30" Range, Stainless!',),
        ("  DISCOUNT   PARTS DIRECT  ",),
        ("Café Crème 2-in-1",),
        ("---",),
        ("",),
        (None,),
    ]
    df = spark.createDataFrame(rows, "t string").select(
        "t", title_norm(F.col("t")).alias("n"), title_tokens(title_norm(F.col("t"))).alias("tok")
    )
    out = [(r.n, r.tok) for r in df.collect()]
    assert out == [
        ("kenmore 30 range stainless", ["kenmore", "30", "range", "stainless"]),
        ("discount parts direct", ["discount", "parts", "direct"]),
        ("café crème 2 in 1", ["café", "crème", "2", "in", "1"]),
        (None, []),
        (None, []),
        (None, []),
    ]


def test_category_norm(spark):
    df = spark.createDataFrame([("Parts & Accessories",), ("Ranges, Ovens & Cooktops",), ("  ",), (None,)], "c string")
    assert [r.n for r in df.select(category_norm(F.col("c")).alias("n")).collect()] == [
        "parts and accessories",
        "ranges ovens and cooktops",
        None,
        None,
    ]
