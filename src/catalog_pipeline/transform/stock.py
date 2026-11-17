"""Stock quantity parsing and stock_status derivation."""
from pyspark.sql import Column
from pyspark.sql import functions as F


def parse_qty(qty: Column) -> Column:
    """Non-negative whole number from a raw string ("12", "12.0"); null if missing or malformed."""
    s = F.trim(qty)
    return F.when(s.rlike(r"^\d+(\.0+)?$"), F.regexp_extract(s, r"^(\d+)", 1).cast("int"))


def stock_status(qty: Column, low_max_qty: int) -> Column:
    """OUT for 0, LOW for 1..low_max_qty, IN above that; null when the quantity is unknown."""
    return (
        F.when(qty.isNull(), None)
        .when(qty == 0, "OUT")
        .when(qty <= low_max_qty, "LOW")
        .otherwise("IN")
    )
