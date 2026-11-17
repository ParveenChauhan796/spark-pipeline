import pytest
from pyspark.sql import functions as F

from catalog_pipeline.transform.price import parse_price

CASES = [
    ("$1,299.00", "1299.00"),  # US grouping
    ("1.299,00", "1299.00"),  # European grouping
    ("12,57", "12.57"),  # decimal comma
    ("98,35", "98.35"),
    ("1229.0", "1229.00"),  # plain JSON-number text
    ("30", "30.00"),
    ("$4.50", "4.50"),
    ("  $19.99 ", "19.99"),  # whitespace around
    ("1,299", "1299.00"),  # US thousands without decimals
    ("$1,299,999.50", "1299999.50"),
    ("1.299.000,50", "1299000.50"),
    ("$ 12.00", "12.00"),
    ("-5.00", "-5.00"),  # parses; rejected later as non-positive
    ("$-5", "-5.00"),
    ("0", "0.00"),
    ("abc", None),
    ("", None),
    ("   ", None),
    (None, None),
    ("$", None),
    ("12.34.56", None),
    ("1,2,3", None),
    ("EUR 5.00", None),
    ("1e5", None),
    ("12.50 USD", None),
    ("99999999999.00", None),  # does not fit decimal(12,2)
]


@pytest.fixture(scope="module")
def parsed(spark):
    df = spark.createDataFrame([(raw,) for raw, _ in CASES], "raw string")
    return {r.raw: r.price for r in df.select("raw", parse_price(F.col("raw")).alias("price")).collect()}


@pytest.mark.parametrize("raw, expected", CASES)
def test_parse_price(parsed, raw, expected):
    assert (None if parsed[raw] is None else str(parsed[raw])) == expected


def test_result_type_is_decimal_not_float(spark):
    df = spark.range(1).select(parse_price(F.lit("$1,299.00")).alias("p"))
    assert df.dtypes == [("p", "decimal(12,2)")]
