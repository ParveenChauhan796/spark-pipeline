from datetime import date

import pytest
from pyspark.sql import Row

from catalog_pipeline.config import load_config
from catalog_pipeline.transform import silver

DAY = date(2025, 3, 1)
RAW = "vendor_sku string, title string, brand string, category string, model string, price string, qty string, gtin string"


def bronze_a(spark, rows):
    """Bronze-shaped rows for a flat vendor. Each row: (sku, title, brand, category, model, price, qty, gtin)."""
    data = [(*r, "A", DAY, i, f"file:///landing/A/2025-03-01/products.csv") for i, r in enumerate(rows)]
    return spark.createDataFrame(data, RAW + ", _vendor_id string, batch_date date, _row_id long, _source_file string")


@pytest.fixture(scope="module")
def aliases(spark):
    return spark.createDataFrame([("acme", "Acme")], "brand_key string, canonical_brand string")


@pytest.fixture(scope="module")
def config():
    return load_config()


ROWS = [
    ("A-1", "Acme Range 30\"", "ACME Inc.", "Ranges & Ovens", "R30", "$1,299.00", "5", "0491930664828"),  # valid, LOW
    ("A-2", "Old title", "Acme", "Parts", None, "$10.00", "9", None),  # superseded by the next A-2 row
    ("A-2", "New title", "Acme", "Parts", None, "$12.00", "0", None),  # latest wins, OUT
    ("A-4", "Good row", "Acme", "Parts", None, "$5.00", None, None),  # valid, unknown qty
    ("A-4", "Broken later row", "Acme", "Parts", None, "0", "1", None),  # invalid; must NOT replace the good row
    ("A-5", "Bad price", "Acme", "Parts", None, "abc", "1", None),
    (None, "No sku", "Acme", "Parts", None, "$1.00", "1", None),
    ("A-7", "", "Acme", "Parts", None, "$1.00", "1", None),
    ("A-8", "No price", "Acme", "Parts", None, None, "1", None),
    ("A-9", "Negative price", "Acme", "Parts", None, "-3.50", "1", None),
    ("A-10", "Bad qty", "Acme", "Parts", None, "$1.00", "lots", None),
]


def test_classification_reasons_and_counts(spark, aliases, config):
    classified = silver.classify(bronze_a(spark, ROWS), aliases, config)

    reasons = {(r.vendor_sku, r.title): r.reason for r in classified.collect()}
    assert reasons[("A-1", 'Acme Range 30"')] is None
    assert reasons[("A-4", "Broken later row")] == "PRICE_NON_POSITIVE"
    assert reasons[("A-5", "Bad price")] == "PRICE_UNPARSEABLE"
    assert reasons[(None, "No sku")] == "NULL_SKU"
    assert reasons[("A-7", "")] == "MISSING_TITLE"
    assert reasons[("A-8", "No price")] == "MISSING_PRICE"
    assert reasons[("A-9", "Negative price")] == "PRICE_NON_POSITIVE"
    assert reasons[("A-10", "Bad qty")] == "QTY_UNPARSEABLE"

    counts = silver.batch_counts(classified)
    assert counts == {"rows_in": 11, "valid": 3, "quarantined": 7, "deduped": 1}
    assert counts["rows_in"] == counts["valid"] + counts["quarantined"] + counts["deduped"]


def test_offers_keep_the_latest_valid_row_per_key(spark, aliases, config):
    classified = silver.classify(bronze_a(spark, ROWS), aliases, config)

    offers = {r.vendor_sku: r for r in silver.to_offers(classified).collect()}
    assert sorted(offers) == ["A-1", "A-2", "A-4"]
    assert (str(offers["A-1"].price), offers["A-1"].qty, offers["A-1"].stock_status) == ("1299.00", 5, "LOW")
    assert (str(offers["A-2"].price), offers["A-2"].qty, offers["A-2"].stock_status) == ("12.00", 0, "OUT")  # latest row
    assert (str(offers["A-4"].price), offers["A-4"].stock_status) == ("5.00", None)  # later broken row ignored


def test_products_carry_cleaned_descriptive_values(spark, aliases, config):
    classified = silver.classify(bronze_a(spark, ROWS), aliases, config)

    p = {r.vendor_sku: r for r in silver.to_products(classified).collect()}["A-1"]
    assert p.title == 'Acme Range 30"' and p.title_norm == "acme range 30"
    assert p.title_tokens == ["acme", "range", "30"]
    assert (p.brand_raw, p.brand) == ("ACME Inc.", "Acme")
    assert (p.category_norm, p.gtin14, p.last_batch_date) == ("ranges and ovens", "00491930664828", DAY)


def test_quarantine_keeps_raw_values_and_reason(spark, aliases, config):
    classified = silver.classify(bronze_a(spark, ROWS), aliases, config)

    q = {r.reason: r for r in silver.to_quarantine(classified).collect() if r.reason == "PRICE_UNPARSEABLE"}["PRICE_UNPARSEABLE"]
    assert (q.vendor_sku, q.price_raw, q.qty_raw, q.vendor_id) == ("A-5", "abc", "1", "A")
    assert q._source_file.endswith("products.csv")


def test_nested_vendor_is_flattened_and_missing_columns_become_null(spark, aliases, config):
    schema = (
        "vendor_sku string, product struct<name:string,brand:string,category:string,model:string>, "
        "offer struct<price:string,qty:string>, gtin string, _vendor_id string, batch_date date, _row_id long, _source_file string"
    )
    rows = [("D1", Row(name="Oven", brand="acme", category="Ovens", model="B1"), Row(price="99.50", qty="3"), "491930664828",
             "D", DAY, 0, "f")]
    classified = silver.classify(spark.createDataFrame(rows, schema), aliases, config)

    o = silver.to_offers(classified).collect()[0]
    p = silver.to_products(classified).collect()[0]
    assert (str(o.price), o.qty, o.stock_status) == ("99.50", 3, "LOW")
    assert (p.title, p.brand, p.model, p.gtin14) == ("Oven", "Acme", "B1", "00491930664828")

    # A flat vendor with no `model` column (vendor C) still gets the full silver shape.
    flat = bronze_a(spark, [("C1", "Fan", "Acme", "Fans", None, "12.5", "3", None)]).drop("model")
    assert silver.to_products(silver.classify(flat, aliases, config)).collect()[0].model is None
