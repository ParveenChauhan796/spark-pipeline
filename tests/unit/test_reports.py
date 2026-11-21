from datetime import date
from decimal import Decimal

import pytest

from catalog_pipeline.history.scd2 import HISTORY_SCHEMA
from catalog_pipeline.reports import reports

D1, D2, D3 = date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3)
COLUMNS = ["title", "brand", "category", "model", "gtin14"]
PRODUCT = "vendor_id string, title string, brand string, category string, model string, gtin14 string"
DIM = "master_id string, title string, brand string, category string, model string, gtin14 string"


@pytest.fixture(scope="module")
def history(spark):
    """(offer_key, price, stock, valid_from, valid_to). is_current = valid_to is NULL."""
    rows = [
        ("A|1", "10.00", "IN", D1, D2), ("A|1", "11.00", "IN", D2, D3), ("A|1", "11.00", "LOW", D3, None),  # +10%, then stock only
        ("B|1", "20.00", "IN", D1, D2), ("B|1", "15.00", "IN", D2, None),  # -25%
        ("C|1", "50.00", "IN", D1, D3), ("C|1", "55.00", "IN", D3, None),  # +10%, master without brand
        ("D|1", "8.00", "IN", D1, None),  # never changes, single vendor
    ]
    data = [(k, k[0], k[2:], Decimal(p), s, "h", f, t, t is None) for k, p, s, f, t in rows]
    return spark.createDataFrame(data, HISTORY_SCHEMA)


@pytest.fixture(scope="module")
def xref(spark):
    rows = [("A|1", "M1"), ("B|1", "M1"), ("C|1", "M2"), ("D|1", "M3")]
    return spark.createDataFrame(rows, "record_key string, master_id string")


@pytest.fixture(scope="module")
def dim(spark):
    rows = [("M1", "Range", "Kenmore", None, "K30", None), ("M2", "Fan", None, None, None, None),
            ("M3", "Bulb", "Kenmore", "Lighting", "B1", "00000000000017")]
    return spark.createDataFrame(rows, DIM)


def test_attribute_completeness_per_vendor_and_catalogue(spark, dim):
    products = spark.createDataFrame(
        [("A", "x", "Kenmore", None, None, None), ("A", "y", None, "Parts", None, "0001"), ("B", "z", None, None, None, None)],
        PRODUCT,
    )

    got = {(r.scope, r.attribute): (r.non_null, r.total, r.completeness)
           for r in reports.attribute_completeness(products, dim, COLUMNS).collect()}

    assert got == {
        ("A", "title"): (2, 2, 1.0), ("A", "brand"): (1, 2, 0.5), ("A", "category"): (1, 2, 0.5),
        ("A", "model"): (0, 2, 0.0), ("A", "gtin14"): (1, 2, 0.5),
        ("B", "title"): (1, 1, 1.0), ("B", "brand"): (0, 1, 0.0), ("B", "category"): (0, 1, 0.0),
        ("B", "model"): (0, 1, 0.0), ("B", "gtin14"): (0, 1, 0.0),
        ("CATALOGUE", "title"): (3, 3, 1.0), ("CATALOGUE", "brand"): (2, 3, 2 / 3),
        ("CATALOGUE", "category"): (1, 3, 1 / 3), ("CATALOGUE", "model"): (2, 3, 2 / 3),
        ("CATALOGUE", "gtin14"): (1, 3, 1 / 3),
    }


def test_price_changes_skip_first_versions_and_stock_only_changes(history):
    got = sorted((r.offer_key, r.change_date, str(r.old_price), str(r.new_price), r.pct_change)
                 for r in reports.price_changes(history).collect())

    assert got == [
        ("A|1", D2, "10.00", "11.00", 10.0),
        ("B|1", D2, "20.00", "15.00", -25.0),
        ("C|1", D3, "50.00", "55.00", 10.0),
    ]


def test_price_changes_by_brand_hand_computed(history, xref, dim):
    report = reports.price_changes_by_brand(reports.price_changes(history), xref, dim)

    got = {(r.brand, r.change_date): (r.changes, r.increases, r.decreases, r.avg_pct_change) for r in report.collect()}

    assert got == {
        ("Kenmore", D2): (2, 1, 1, -7.5),  # (+10 - 25) / 2
        ("UNKNOWN", D3): (1, 1, 0, 10.0),  # M2 has no golden brand
    }


def test_price_spread_uses_current_prices_of_multi_vendor_masters(history, xref):
    got = [(r.master_id, r.n_vendors, r.n_offers, str(r.min_price), str(r.max_price), round(r.spread_pct, 4))
           for r in reports.price_spread(history, xref, 2).collect()]

    assert got == [("M1", 2, 2, "11.00", "15.00", round(4 / 11 * 100, 4))]  # M2, M3 have one vendor each


def test_top_rows_sorts_and_limits(spark):
    df = spark.createDataFrame([("x", 1), ("y", 3), ("z", 2)], "k string, v int")
    assert [r.k for r in reports.top_rows(df, ["v"], True, 2).collect()] == ["y", "z"]
    assert [r.k for r in reports.top_rows(df, ["v"], False, 2).collect()] == ["x", "z"]
