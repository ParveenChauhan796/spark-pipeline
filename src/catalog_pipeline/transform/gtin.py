"""Barcode normalization to GTIN-14."""
from pyspark.sql import Column
from pyspark.sql import functions as F

GTIN14_LENGTH = 14  # fixed by the GTIN-14 standard


def to_gtin14(gtin: Column, min_length: int, max_length: int = GTIN14_LENGTH) -> Column:
    """Left-pad an all-digit barcode (UPC-12, EAN-13, ...) with zeros to 14 digits.

    Anything else (non-digits, too short, too long) becomes null: a wrong barcode must never match.
    """
    s = F.trim(gtin)
    valid = s.rlike(rf"^\d{{{min_length},{max_length}}}$")
    return F.when(valid, F.lpad(s, GTIN14_LENGTH, "0"))
