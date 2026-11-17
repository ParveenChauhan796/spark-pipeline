import copy

import pytest
from test_bronze_ingest import A_HEADER, C_ROWS, D1, D2, D_ROWS, put_csv, put_jsonl

from catalog_pipeline import pipeline
from catalog_pipeline.config import PROJECT_ROOT, load_config
from catalog_pipeline.ingest import ops
from catalog_pipeline.ingest.contracts import load_contracts
from catalog_pipeline.paths import table_path

ALIASES = "brand_key,canonical_brand\nkenmore,Kenmore\nrovsun,ROVSUN\nbosch,Bosch\n"

A_DAY1 = [
    ["A-1", 'Kenmore 30" Range', "KENMORE Inc.", "Ranges & Ovens", "K30", "$1,299.00", "5", "0491930664828"],
    ["A-2", "Water Filter", "Kenmore", "Parts", "", "$19.99", "0", ""],
    ["A-2", "Water Filter (v2)", "Kenmore", "Parts", "", "$21.99", "3", ""],  # in-file duplicate, later row wins
    ["A-3", "Bad price", "Kenmore", "Parts", "", "abc", "1", ""],
    ["", "No sku", "Kenmore", "Parts", "", "$5.00", "1", ""],
]
A_DAY2 = [
    ["A-1", 'Kenmore 30" Range, Stainless', "KENMORE Inc.", "Ranges & Ovens", "K30", "$1,199.00", "2", "0491930664828"],
    ["A-4", "New Item", "Kenmore", "Parts", "", "$7.00", "9", ""],
]


@pytest.fixture
def env(tmp_path):
    config = copy.deepcopy(load_config())
    config["lake"]["root"] = str(tmp_path / "lake")
    config["ingest"]["landing_dir"] = str(tmp_path / "landing")
    config["silver"]["brand_aliases_path"] = str(tmp_path / "brand_aliases.csv")
    (tmp_path / "brand_aliases.csv").write_text(ALIASES)
    contracts = load_contracts(PROJECT_ROOT / "conf" / "vendors")
    return config, contracts, tmp_path / "landing"


def silver(spark, config, name):
    return spark.read.format("delta").load(str(table_path("silver", config["silver"]["tables"][name], config)))


def silver_log(spark, config):
    log = spark.read.format("delta").load(str(ops.log_path("silver_log", config)))
    return {(r.vendor_id, r.batch_id): (r.rows_in, r.valid, r.quarantined, r.deduped) for r in log.collect()}


def land_two_days(landing):
    put_csv(landing, "A", D1, A_HEADER, A_DAY1)
    put_csv(landing, "A", D2, A_HEADER, A_DAY2)


def test_silver_end_to_end_reconciles_for_every_batch(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, A_HEADER, A_DAY1)
    put_jsonl(landing, "C", D1, C_ROWS)
    put_jsonl(landing, "D", D1, D_ROWS)

    results = pipeline.run_all(spark, config)

    assert [s.status for _, s in results] == [pipeline.DONE] * 3
    log = silver_log(spark, config)
    assert log[("C", D1)] == (2, 2, 0, 0) and log[("D", D1)] == (1, 1, 0, 0)
    for (vendor, batch), (rows_in, valid, quarantined, deduped) in log.items():
        assert rows_in == valid + quarantined + deduped, (vendor, batch)

    assert log[("A", D1)] == (5, 2, 2, 1)  # valid: A-1 and A-2; quarantined: bad price + null sku; deduped: the older A-2

    offers = {(r.vendor_id, r.vendor_sku): r for r in silver(spark, config, "vendor_offer").collect()}
    assert sorted(offers) == [("A", "A-1"), ("A", "A-2"), ("C", "c_1"), ("C", "c_2"), ("D", "D1")]
    assert (str(offers[("A", "A-2")].price), offers[("A", "A-2")].qty, offers[("A", "A-2")].stock_status) == ("21.99", 3, "LOW")
    assert (str(offers[("C", "c_1")].price), offers[("C", "c_1")].stock_status) == ("12.50", "LOW")
    assert (str(offers[("D", "D1")].price), offers[("D", "D1")].qty) == ("99.50", 2)

    quarantine = {r.reason: r for r in silver(spark, config, "quarantine").collect()}
    assert sorted(quarantine) == ["NULL_SKU", "PRICE_UNPARSEABLE"]
    assert quarantine["PRICE_UNPARSEABLE"].price_raw == "abc" and quarantine["PRICE_UNPARSEABLE"].vendor_sku == "A-3"

    products = {(r.vendor_id, r.vendor_sku): r for r in silver(spark, config, "vendor_product").collect()}
    assert products[("A", "A-1")].brand == "Kenmore" and products[("A", "A-1")].gtin14 == "00491930664828"
    assert (products[("D", "D1")].title, products[("D", "D1")].brand) == ("Oven", "Bosch")  # nested flattened
    assert products[("C", "c_2")].brand is None and products[("C", "c_2")].model is None


def test_vendor_product_holds_latest_values_and_offers_keep_history(spark, env):
    config, contracts, landing = env
    land_two_days(landing)

    pipeline.run_all(spark, config)

    products = {r.vendor_sku: r for r in silver(spark, config, "vendor_product").collect()}
    assert sorted(products) == ["A-1", "A-2", "A-4"]  # one row per key
    assert products["A-1"].title == 'Kenmore 30" Range, Stainless' and str(products["A-1"].last_batch_date) == D2
    assert str(products["A-2"].last_batch_date) == D1  # untouched on day 2

    history = sorted((str(r.batch_date), r.vendor_sku, str(r.price)) for r in silver(spark, config, "vendor_offer").collect())
    assert history == [
        (D1, "A-1", "1299.00"), (D1, "A-2", "21.99"), (D2, "A-1", "1199.00"), (D2, "A-4", "7.00"),
    ]


def test_rerunning_silver_changes_nothing(spark, env):
    config, contracts, landing = env
    land_two_days(landing)
    pipeline.run_all(spark, config)

    def versions():
        return {
            name: spark.sql(f"DESCRIBE HISTORY delta.`{table_path('silver', config['silver']['tables'][name], config)}`").count()
            for name in ("vendor_offer", "vendor_product", "quarantine")
        }

    before = (versions(), silver_log(spark, config), silver(spark, config, "vendor_offer").count())
    second = pipeline.run_all(spark, config)

    assert {s.status for _, s in second} == {pipeline.ALREADY_DONE}
    assert (versions(), silver_log(spark, config), silver(spark, config, "vendor_offer").count()) == before


def test_older_batch_replay_cannot_overwrite_newer_product_values(spark, env):
    config, contracts, landing = env
    land_two_days(landing)
    pipeline.run_all(spark, config)
    aliases = pipeline.load_brand_aliases(spark, config)
    # Forget that day 1 was processed, then process it again after day 2 already updated A-1.
    spark.sql(f"DELETE FROM delta.`{ops.log_path('silver_log', config)}` WHERE batch_id = '{D1}'")

    outcome = pipeline.silver_batch(spark, config, contracts["A"], D1, aliases)

    assert outcome.status == pipeline.DONE
    a1 = next(r for r in silver(spark, config, "vendor_product").collect() if r.vendor_sku == "A-1")
    assert a1.title == 'Kenmore 30" Range, Stainless' and str(a1.last_batch_date) == D2  # day-2 values kept
    assert silver(spark, config, "vendor_offer").count() == 4  # day-1 offers rewritten, not duplicated


def test_retry_after_crash_between_write_and_log_does_not_duplicate(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, A_HEADER, A_DAY1)
    pipeline.run_all(spark, config)
    spark.sql(f"DELETE FROM delta.`{ops.log_path('silver_log', config)}` WHERE vendor_id = 'A'")

    pipeline.run_all(spark, config)

    assert silver(spark, config, "vendor_offer").count() == 2
    assert silver(spark, config, "quarantine").count() == 2
    assert silver(spark, config, "vendor_product").count() == 2
    assert silver_log(spark, config) == {("A", D1): (5, 2, 2, 1)}


def test_quarantined_bronze_batch_produces_no_silver(spark, env):
    config, contracts, landing = env
    put_csv(landing, "A", D1, ["item_sku"] + A_HEADER[1:], A_DAY1)  # required column renamed: bronze quarantines it
    put_jsonl(landing, "C", D1, C_ROWS)

    results = {b.vendor_id: (b.outcome, s.status) for b, s in pipeline.run_all(spark, config)}

    assert results == {"A": ("QUARANTINED", pipeline.NOT_INGESTED), "C": ("SUCCESS", pipeline.DONE)}
    assert {r.vendor_id for r in silver(spark, config, "vendor_offer").collect()} == {"C"}
    assert list(silver_log(spark, config)) == [("C", D1)]
