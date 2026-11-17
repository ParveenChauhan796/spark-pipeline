import copy
import csv
import json
from datetime import date

import pytest

from catalog_pipeline.config import PROJECT_ROOT, load_config
from catalog_pipeline.ingest import bronze, ops
from catalog_pipeline.ingest.contracts import load_contracts
from catalog_pipeline.paths import table_path
from catalog_pipeline.pipeline import run_all

D1, D2, D3 = "2025-03-01", "2025-03-02", "2025-03-03"
A_HEADER = ["sku", "title", "brand", "category", "model", "price", "qty", "gtin"]
A_ROWS = [
    ["A-1", 'Kenmore 30" Range, Stainless', "Kenmore", "Ranges", "K30", "$1,299.00", "5", "0491930664828"],
    ["A-2", "Whirlpool Filter", "", "Parts", "WF1", "$19.99", "0", ""],
    ["A-3", "GE Bulb", "GE", "Lighting", "", "$4.50", "12", "123"],
]
C_ROWS = [
    {"sku": "c_1", "name": "Ice Maker", "brand": "ROVSUN", "category": "Appliances", "price": 12.5, "quantity": 3, "upc": "049"},
    {"sku": "c_2", "name": "Fan", "brand": None, "category": "Appliances", "price": 30, "quantity": 0, "upc": None},
]
D_ROWS = [
    {"id": "D1", "product": {"name": "Oven", "brand": "Bosch", "category": "Ovens", "model": "B1"},
     "offer": {"price": 99.5, "qty": 2}, "barcode": "00491930664828"},
]


@pytest.fixture
def env(tmp_path):
    config = copy.deepcopy(load_config())
    config["lake"]["root"] = str(tmp_path / "lake")
    config["ingest"]["landing_dir"] = str(tmp_path / "landing")
    config["silver"]["brand_aliases_path"] = str(tmp_path / "brand_aliases.csv")  # run_all also runs silver
    (tmp_path / "brand_aliases.csv").write_text("brand_key,canonical_brand\n")
    contracts = load_contracts(PROJECT_ROOT / "conf" / "vendors")
    return config, contracts, tmp_path / "landing"


def put_csv(landing, vendor, day, header, rows, delimiter=","):
    path = landing / vendor / day
    path.mkdir(parents=True, exist_ok=True)
    with (path / "products.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=delimiter, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def put_jsonl(landing, vendor, day, rows):
    path = landing / vendor / day
    path.mkdir(parents=True, exist_ok=True)
    (path / "products.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def read(spark, config, layer, name):
    return spark.read.format("delta").load(str(table_path(layer, name, config)))


def drift_rows(spark, config):
    log = read(spark, config, "ops", "schema_drift_log")
    return sorted((r.vendor_id, r.batch_id, r.drift_type, r.source_column) for r in log.collect())


def batch_rows(spark, config):
    log = read(spark, config, "ops", "batch_log")
    return sorted((r.vendor_id, r.batch_id, r.status, r.rows_ingested) for r in log.collect())


def test_bronze_shape_and_lineage(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, A_HEADER, A_ROWS)

    outcome = bronze.ingest_batch(spark, config, contracts["A"], D1)

    assert (outcome.outcome, outcome.rows, outcome.drifts) == (bronze.SUCCESS, 3, ())
    df = read(spark, config, "bronze", "vendor_a")
    assert df.columns == ["vendor_sku", "title", "brand", "category", "model", "price", "qty", "gtin",
                          "_vendor_id", "_batch_id", "_source_file", "_ingested_at", "_row_id", "batch_date"]
    assert all(t == "string" for n, t in df.dtypes if n not in ("_ingested_at", "_row_id", "batch_date"))
    rows = {r.vendor_sku: r for r in df.collect()}
    assert rows["A-1"].title == 'Kenmore 30" Range, Stainless'  # quoted comma and doubled quote survive
    assert rows["A-1"].price == "$1,299.00" and rows["A-1"].gtin == "0491930664828"  # raw text, leading zero kept
    assert rows["A-2"].brand is None  # empty CSV field is null
    assert {(r._vendor_id, r._batch_id, r.batch_date) for r in rows.values()} == {("A", D1, date(2025, 3, 1))}
    assert rows["A-1"]._source_file.endswith("/A/2025-03-01/products.csv")
    assert (table_path("bronze", "vendor_a", config) / f"batch_date={D1}").is_dir()  # on-disk partition
    assert batch_rows(spark, config) == [("A", D1, "SUCCESS", 3)]


def test_json_vendors_keep_nested_structs_and_stringify_scalars(spark, env):
    config, contracts, landing = env
    put_jsonl(landing, "C", D1, C_ROWS)
    put_jsonl(landing, "D", D1, D_ROWS)

    assert bronze.ingest_batch(spark, config, contracts["C"], D1).rows == 2
    assert bronze.ingest_batch(spark, config, contracts["D"], D1).rows == 1

    c = {r.vendor_sku: r for r in read(spark, config, "bronze", "vendor_c").collect()}
    assert (c["c_1"].price, c["c_1"].qty, c["c_2"].price) == ("12.5", "3", "30")
    assert c["c_2"].brand is None and "model" not in c["c_1"].asDict()  # C's contract has no model column
    d_df = read(spark, config, "bronze", "vendor_d")
    assert dict(d_df.dtypes)["product"] == "struct<name:string,brand:string,category:string,model:string>"
    d = d_df.collect()[0]
    assert (d.product.name, d.offer.price, d.offer.qty, d.gtin) == ("Oven", "99.5", "2", "00491930664828")


def test_drift_events_are_logged_and_batches_still_ingest(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, A_HEADER, A_ROWS)
    put_csv(landing, "A", D2, A_HEADER + ["rating"], [r + ["4.5"] for r in A_ROWS])  # ADDED
    b_header = ["item_id", "title", "manufacturer", "cat", "mpn", "price", "stock", "ean"]
    b_rows = [["B1", "Fridge", "LG", "Cooling", "L1", "1.299,00", "3", "491"]]
    put_csv(landing, "B", D1, b_header, b_rows, ";")
    put_csv(landing, "B", D2, ["item_id", "name"] + b_header[2:], b_rows, ";")  # RENAMED (declared alias)
    put_jsonl(landing, "C", D1, C_ROWS)
    put_jsonl(landing, "C", D2, [{**r, "price": f"{r['price']:.2f}"} for r in C_ROWS])  # TYPE_CHANGED

    outcomes = [bronze_outcome for bronze_outcome, _ in run_all(spark, config)]

    assert [o.outcome for o in outcomes] == [bronze.SUCCESS] * 6
    assert drift_rows(spark, config) == [
        ("A", D2, "ADDED", "rating"),
        ("B", D2, "RENAMED", "name"),
        ("C", D2, "TYPE_CHANGED", "price"),
    ]
    # The renamed column lands under its canonical name; the added column is ignored downstream.
    b2 = read(spark, config, "bronze", "vendor_b").filter(f"batch_date = '{D2}'").collect()[0]
    assert b2.title == "Fridge" and b2.price == "1.299,00"
    assert "rating" not in read(spark, config, "bronze", "vendor_a").columns
    # TYPE_CHANGED still stores the value as a string.
    assert read(spark, config, "bronze", "vendor_c").filter(f"batch_date = '{D2}' AND vendor_sku = 'c_1'").collect()[0].price == "12.50"


def test_unknown_rename_of_required_column_quarantines_only_that_batch(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, A_HEADER, A_ROWS)
    put_csv(landing, "A", D2, ["item_sku"] + A_HEADER[1:], A_ROWS)  # `sku` renamed to a name nobody declared
    put_jsonl(landing, "C", D1, C_ROWS)
    put_jsonl(landing, "C", D2, C_ROWS)

    outcomes = {(b.vendor_id, b.batch_id): b for b, _ in run_all(spark, config)}

    quarantined = outcomes[("A", D2)]
    assert quarantined.outcome == bronze.QUARANTINED and "vendor_sku" in quarantined.reason
    assert outcomes[("C", D2)].outcome == bronze.SUCCESS  # other vendors continue
    assert batch_rows(spark, config) == [
        ("A", D1, "SUCCESS", 3), ("A", D2, "QUARANTINED", None), ("C", D1, "SUCCESS", 2), ("C", D2, "SUCCESS", 2),
    ]
    assert (D2, "MISSING", "sku") in {(b, t, c) for _, b, t, c in drift_rows(spark, config)}
    # Not written to bronze, and the files stay in landing.
    assert read(spark, config, "bronze", "vendor_a").filter(f"batch_date = '{D2}'").count() == 0
    assert (landing / "A" / D2 / "products.csv").is_file()


def test_missing_optional_column_is_null_filled(spark, env):
    config, contracts, landing = env
    header = [c for c in A_HEADER if c != "model"]
    put_csv(landing, "A", D1, header, [[v for c, v in zip(A_HEADER, r) if c != "model"] for r in A_ROWS])

    outcome = bronze.ingest_batch(spark, config, contracts["A"], D1)

    assert outcome.outcome == bronze.SUCCESS
    assert [(d.drift_type, d.source_column, d.is_required) for d in outcome.drifts] == [("MISSING", "model", False)]
    assert read(spark, config, "bronze", "vendor_a").filter("model IS NOT NULL").count() == 0


def test_rerunning_everything_changes_nothing(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, A_HEADER, A_ROWS)
    put_csv(landing, "A", D2, A_HEADER + ["rating"], [r + ["4.5"] for r in A_ROWS])
    put_jsonl(landing, "C", D1, C_ROWS)
    run_all(spark, config)

    def snapshot():
        history = spark.sql(f"DESCRIBE HISTORY delta.`{table_path('bronze', 'vendor_a', config)}`").count()
        return (batch_rows(spark, config), drift_rows(spark, config), read(spark, config, "bronze", "vendor_a").count(), history)

    before = snapshot()
    second = [bronze_outcome for bronze_outcome, _ in run_all(spark, config)]

    assert {o.outcome for o in second} == {bronze.SKIPPED}
    assert snapshot() == before


def test_batch_older_than_last_success_is_rejected(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, A_HEADER, A_ROWS)
    put_csv(landing, "A", D2, A_HEADER, A_ROWS)

    assert bronze.ingest_batch(spark, config, contracts["A"], D2).outcome == bronze.SUCCESS
    stale = bronze.ingest_batch(spark, config, contracts["A"], D1)

    assert stale.outcome == bronze.REJECTED_STALE
    assert read(spark, config, "bronze", "vendor_a").filter(f"batch_date = '{D1}'").count() == 0
    assert batch_rows(spark, config) == [("A", D2, "SUCCESS", 3)]  # rejection is not logged


def test_quarantined_batch_can_be_retried_once_the_feed_is_fixed(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, ["item_sku"] + A_HEADER[1:], A_ROWS)
    assert bronze.ingest_batch(spark, config, contracts["A"], D1).outcome == bronze.QUARANTINED

    put_csv(landing, "A", D1, A_HEADER, A_ROWS)  # vendor re-delivers a corrected file
    assert bronze.ingest_batch(spark, config, contracts["A"], D1).outcome == bronze.SUCCESS

    assert batch_rows(spark, config) == [("A", D1, "SUCCESS", 3)]  # the log row was replaced, not duplicated
    assert drift_rows(spark, config) == []  # and the old drift rows are gone


def test_retry_after_crash_between_write_and_log_does_not_duplicate_rows(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, A_HEADER, A_ROWS)
    bronze.ingest_batch(spark, config, contracts["A"], D1)
    # Simulate a crash after the bronze write but before the batch_log row was saved.
    spark.sql(f"DELETE FROM delta.`{ops.log_path('batch_log', config)}` WHERE vendor_id = 'A'")
    assert batch_rows(spark, config) == []

    outcome = bronze.ingest_batch(spark, config, contracts["A"], D1)

    assert outcome.outcome == bronze.SUCCESS
    assert read(spark, config, "bronze", "vendor_a").count() == 3  # replaceWhere rewrote the partition: no 6 rows
    assert batch_rows(spark, config) == [("A", D1, "SUCCESS", 3)]


def test_missing_landing_files_raise(spark, env):
    config, contracts, _ = env
    with pytest.raises(FileNotFoundError):
        bronze.ingest_batch(spark, config, contracts["A"], D1)


def test_batch_id_must_be_a_date_string(spark, env):
    config, contracts, _ = env
    with pytest.raises(ValueError):
        bronze.ingest_batch(spark, config, contracts["A"], "2025-03-01' OR 1=1 --")
