"""Brand reference table (brand_key -> canonical brand) used by silver to canonicalize brands.

Stands in for a curated master list of brands. `brand_key` below is the Python twin of the Spark
expression in transform/brand.py: both must give the same key (a test checks this).
"""
import csv
import re
from collections import Counter
from pathlib import Path


def brand_key(brand: str, legal_suffixes: list[str]) -> str | None:
    """Lowercase, drop punctuation, drop trailing legal words (Inc, Co, ...), drop spaces."""
    tokens = re.sub(r"[\W_]+", " ", brand.lower()).split()
    suffixes = set(legal_suffixes)
    while len(tokens) > 1 and tokens[-1] in suffixes:
        tokens.pop()
    return "".join(tokens) or None


def write_brand_aliases(brands: list[str], legal_suffixes: list[str], path: Path) -> int:
    """Write brand_key,canonical_brand. When several spellings share a key, the most common wins."""
    by_key: dict[str, Counter] = {}
    for brand in brands:
        key = brand_key(brand, legal_suffixes)
        if key:
            by_key.setdefault(key, Counter())[brand] += 1

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["brand_key", "canonical_brand"])
        for key in sorted(by_key):
            counts = by_key[key]
            writer.writerow([key, sorted(counts, key=lambda b: (-counts[b], b))[0]])
    return len(by_key)
