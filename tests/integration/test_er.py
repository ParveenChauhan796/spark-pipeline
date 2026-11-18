import copy

from test_bronze_ingest import A_HEADER, D1, D2, put_csv, put_jsonl
from test_silver import env  # noqa: F401  (pytest fixture: tmp lake, landing and brand aliases)

from catalog_pipeline import pipeline
from catalog_pipeline.ingest import ops
from catalog_pipeline.paths import table_path

B_HEADER = ["item_id", "title", "manufacturer", "cat", "mpn", "price", "stock", "ean"]

# Product P1: the same reservoir at three vendors (A and B share a GTIN; C has a typo, no model, no GTIN).
# Product P2: the sibling part W10502643 at vendor D, near-identical title -> must stay separate.
# Product P3: a fan only vendor B sells -> singleton.
A_ROWS = [["A-1", "Whirlpool W10502642 Refrigerator Reservoir", "Whirlpool", "Parts", "W10502642", "$20.00", "9", "0883049224432"]]
B_ROWS = [
    ["B-1", "Whirlpool W10502642 Refrigerator Water Reservoir", "Whirlpool", "Parts", "W10502642", "20.50", "9", "883049224432"],
    ["B-2", "Bosch Ceiling Fan", "Bosch", "Fans", "BF100", "55.00", "3", ""],
]
C_ROWS = [{"sku": "c_1", "name": "Whirlpool W10502642 Refrigerator Resevoir", "brand": "Whirlpool",
           "category": "Parts", "price": 21.0, "quantity": 9, "upc": None}]
D_ROWS = [{"id": "D-1", "product": {"name": "Whirlpool W10502643 Refrigerator Reservoir", "brand": "Whirlpool",
                                   "category": "Parts", "model": "W10502643"},
           "offer": {"price": 20.0, "qty": 9}, "barcode": None}]
TRUTH = "vendor_id,vendor_sku,true_product_id\nA,A-1,P1\nB,B-1,P1\nC,c_1,P1\nD,D-1,P2\nB,B-2,P3\n"


def er_env(env):
    config, _, landing = env
    config = copy.deepcopy(config)
    truth = landing.parent / "ground_truth.csv"
    truth.write_text(TRUTH)
    config["er"]["ground_truth_path"] = str(truth)
    put_csv(landing, "A", D1, A_HEADER, A_ROWS)
    put_csv(landing, "B", D1, B_HEADER, B_ROWS, ";")
    put_jsonl(landing, "C", D1, C_ROWS)
    put_jsonl(landing, "D", D1, D_ROWS)
    return config, landing


def gold(spark, config, name):
    return spark.read.format("delta").load(str(table_path("gold", config["er"]["tables"][name], config)))


def gold_report(spark, config, name):
    return spark.read.format("delta").load(str(table_path("gold", config["reports"]["tables"][name], config)))


def er_log(spark, config):
    return spark.read.format("delta").load(str(ops.log_path("er_run_log", config))).collect()


def test_er_end_to_end_on_tiny_feeds(spark, env):  # noqa: F811
    config, _ = er_env(env)
    pipeline.run_all(spark, config)

    outcome = pipeline.resolve_entities(spark, config)

    assert outcome.status == pipeline.DONE
    xref = {r.record_key: (r.master_id, r.match_method) for r in gold(spark, config, "product_xref").collect()}
    assert xref == {
        "A|A-1": ("A|A-1", "GTIN"),
        "B|B-1": ("A|A-1", "GTIN"),
        "C|c_1": ("A|A-1", "FUZZY"),  # typo + missing model, matched on title and price
        "D|D-1": ("D|D-1", "SINGLETON"),  # one character apart, but a different model
        "B|B-2": ("B|B-2", "SINGLETON"),
    }
    dim = {r.master_id: (r.golden_record_key, r.n_records) for r in gold(spark, config, "dim_product").collect()}
    assert dim == {"A|A-1": ("A|A-1", 3), "D|D-1": ("D|D-1", 1), "B|B-2": ("B|B-2", 1)}

    [log] = er_log(spark, config)
    assert (log.records, log.gtin_pairs, log.masters, log.same_vendor_clusters) == (5, 1, 3, 0)
    assert (log.predicted_pairs, log.true_pairs, log.correct_pairs) == (3, 3, 3)
    assert (log.precision, log.recall) == (1.0, 1.0)

    # Reports: 4 vendors + CATALOGUE x 5 attributes; one day of data so no price changes; only the
    # reservoir master is sold by 2+ vendors.
    assert pipeline.build_reports(spark, config) == {
        "attribute_completeness": 25, "price_changes_by_brand": 0, "price_spread": 1,
    }
    [spread] = gold_report(spark, config, "price_spread").collect()
    assert (spread.master_id, spread.n_vendors, str(spread.min_price), str(spread.max_price)) == ("A|A-1", 3, "20.00", "21.00")

    previews = pipeline.report_previews(spark, config)
    assert {name: df.count() for name, df in previews.items()} == {
        "attribute_completeness": 25, "price_changes_by_brand": 0, "price_spread": 1,
    }
    first = previews["attribute_completeness"].first()
    assert (first.scope, first.attribute) == ("A", "brand")


def test_er_rerun_is_skipped_until_inputs_or_config_change(spark, env):  # noqa: F811
    config, landing = er_env(env)
    pipeline.run_all(spark, config)
    pipeline.resolve_entities(spark, config)

    assert pipeline.resolve_entities(spark, config).status == pipeline.ALREADY_DONE

    put_csv(landing, "A", D2, A_HEADER, [A_ROWS[0][:5] + ["$20.10", "9", A_ROWS[0][7]]])  # new silver + history versions
    pipeline.run_all(spark, config)
    assert pipeline.resolve_entities(spark, config).status == pipeline.DONE

    config["er"]["scoring"]["threshold"] = 0.99  # config change: ER must run again
    assert pipeline.resolve_entities(spark, config).status == pipeline.DONE
    assert len(er_log(spark, config)) == 3
