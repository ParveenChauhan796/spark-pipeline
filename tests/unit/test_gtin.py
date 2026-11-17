from pyspark.sql import functions as F

from catalog_pipeline.transform.gtin import to_gtin14


def test_to_gtin14(spark):
    raw = [
        "0491930664828",  # EAN-13
        "491930664828",  # UPC-12 (leading zero dropped)
        "00491930664828",  # already 14
        "  0491930664828 ",  # whitespace
        "12345678",  # shortest accepted length
        "123",  # too short
        "04919306648ab",  # non-digits
        "123456789012345",  # too long
        "",
        None,
    ]
    df = spark.createDataFrame([(r,) for r in raw], "g string")
    out = [r.g14 for r in df.select(to_gtin14(F.col("g"), 8, 14).alias("g14")).collect()]
    assert out == [
        "00491930664828",
        "00491930664828",
        "00491930664828",
        "00491930664828",
        "00000012345678",
        None,
        None,
        None,
        None,
        None,
    ]
