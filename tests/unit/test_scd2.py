from datetime import date
from decimal import Decimal

from catalog_pipeline.history import scd2

D1, D2, D3 = date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3)
OFFERS = "vendor_id string, vendor_sku string, batch_date date, price decimal(12,2), stock_status string"


def offers(spark, rows):
    """Each row: (sku, batch_date, price, stock_status) for vendor A."""
    return spark.createDataFrame([("A", sku, d, Decimal(p), s) for sku, d, p, s in rows], OFFERS)


def current(spark, rows):
    """Current history rows built through the same hash. Each row: (sku, price, stock_status, valid_from)."""
    df = spark.createDataFrame([("A", sku, d, Decimal(p), s) for sku, p, s, d in rows], OFFERS)
    staged = scd2.classify_changes(df, spark.createDataFrame([], scd2.HISTORY_SCHEMA))
    return staged.drop("change_type").selectExpr("*", "CAST(NULL AS DATE) AS valid_to", "true AS is_current")


def by_sku(df):
    return {r.vendor_sku: r for r in df.collect()}


def test_new_changed_unchanged_and_stale_are_classified(spark):
    cur = current(spark, [("A-1", "10.00", "IN", D1), ("A-2", "20.00", "IN", D1), ("A-3", "30.00", "LOW", D2)])
    batch = offers(spark, [
        ("A-1", D2, "11.00", "IN"),   # price changed
        ("A-2", D2, "20.00", "IN"),   # unchanged (re-sent)
        ("A-3", D2, "99.00", "OUT"),  # same day as its current version: first load wins
        ("A-4", D2, "40.00", "IN"),   # never seen
    ])

    got = {sku: r.change_type for sku, r in by_sku(scd2.classify_changes(batch, cur)).items()}

    assert got == {"A-1": "CHANGED", "A-2": "UNCHANGED", "A-3": "STALE", "A-4": "NEW"}


def test_replaying_an_older_batch_is_stale_even_with_different_values(spark):
    cur = current(spark, [("A-1", "11.00", "IN", D2)])
    replay = offers(spark, [("A-1", D1, "10.00", "OUT")])

    assert by_sku(scd2.classify_changes(replay, cur))["A-1"].change_type == "STALE"


def test_stock_status_change_alone_is_a_change(spark):
    cur = current(spark, [("A-1", "10.00", "IN", D1)])
    got = by_sku(scd2.classify_changes(offers(spark, [("A-1", D2, "10.00", "OUT")]), cur))["A-1"]

    assert (got.change_type, got.stock_status) == ("CHANGED", "OUT")


def test_unknown_stock_carries_the_last_known_status_forward(spark):
    cur = current(spark, [("A-1", "10.00", "LOW", D1), ("A-2", "20.00", "IN", D1)])
    batch = offers(spark, [("A-1", D2, "10.00", None), ("A-2", D2, "25.00", None)])

    got = by_sku(scd2.classify_changes(batch, cur))

    assert (got["A-1"].change_type, got["A-1"].stock_status) == ("UNCHANGED", "LOW")
    assert (got["A-2"].change_type, got["A-2"].stock_status) == ("CHANGED", "IN")  # new price, old status


def test_brand_new_offer_with_unknown_stock_stays_null(spark):
    empty = spark.createDataFrame([], scd2.HISTORY_SCHEMA)
    got = by_sku(scd2.classify_changes(offers(spark, [("A-1", D1, "10.00", None)]), empty))["A-1"]

    assert (got.change_type, got.stock_status, got.offer_key, got.valid_from) == ("NEW", None, "A|A-1", D1)


def test_row_hash_differs_by_price_and_by_stock_and_handles_null(spark):
    rows = [("1", "10.00", "IN"), ("2", "10.00", "OUT"), ("3", "10.01", "IN"), ("4", "10.00", None)]
    df = spark.createDataFrame([(k, Decimal(p), s) for k, p, s in rows], "k string, price decimal(12,2), stock_status string")
    hashes = [r.h for r in df.select(scd2.row_hash(df.price, df.stock_status).alias("h")).collect()]

    assert None not in hashes and len(set(hashes)) == 4


def test_stage_merge_lists_changed_twice_new_once_and_drops_the_rest(spark):
    cur = current(spark, [("A-1", "10.00", "IN", D1), ("A-2", "20.00", "IN", D1), ("A-3", "30.00", "IN", D2)])
    batch = offers(spark, [
        ("A-1", D2, "11.00", "IN"), ("A-2", D2, "20.00", "IN"), ("A-3", D2, "1.00", "IN"), ("A-4", D2, "40.00", "IN"),
    ])

    staged = sorted((r.vendor_sku, r.merge_key or "<null>") for r in scd2.stage_merge(scd2.classify_changes(batch, cur)).collect())

    assert staged == [("A-1", "<null>"), ("A-1", "A|A-1"), ("A-4", "<null>")]
