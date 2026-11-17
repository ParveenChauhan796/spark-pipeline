from pyspark.sql import functions as F

from catalog_pipeline.transform.stock import parse_qty, stock_status


def test_parse_qty(spark):
    raw = ["12", "12.0", "0", " 7 ", "-3", "1.5", "abc", "", None, "99999999999"]
    df = spark.createDataFrame([(r,) for r in raw], "q string")
    assert [r.n for r in df.select(parse_qty(F.col("q")).alias("n")).collect()] == [12, 12, 0, 7, None, None, None, None, None, None]


def test_stock_status_boundaries(spark):
    df = spark.createDataFrame([(q,) for q in [None, 0, 1, 5, 6, 40]], "q int")
    assert [r.s for r in df.select(stock_status(F.col("q"), 5).alias("s")).collect()] == [None, "OUT", "LOW", "LOW", "IN", "IN"]
    assert [r.s for r in df.select(stock_status(F.col("q"), 2).alias("s")).collect()] == [None, "OUT", "LOW", "IN", "IN", "IN"]
