from datetime import date
from decimal import Decimal

from test_bronze_ingest import A_HEADER, C_ROWS, D1, D2, D3, put_csv, put_jsonl
from test_silver import env  # noqa: F401  (pytest fixture: tmp lake, landing and brand aliases)

from catalog_pipeline import pipeline
from catalog_pipeline.history import scd2
from catalog_pipeline.ingest import ops
from catalog_pipeline.paths import table_path


def row(sku, price, qty):
    return [sku, f"Item {sku}", "Kenmore", "Parts", "", price, qty, ""]


A_DAYS = {
    D1: [row("A-1", "$10.00", "9"), row("A-2", "$20.00", "0"), row("A-3", "$30.00", "9")],
    D2: [
        row("A-1", "$11.00", "9"),  # price changed
        row("A-2", "$20.00", "0"),  # re-sent, unchanged
        row("A-3", "$30.00", ""),   # qty unknown: keeps IN, so unchanged
        row("A-4", "$40.00", ""),   # new, stock unknown
    ],
    D3: [row("A-1", "$11.00", "2"), row("A-4", "$40.00", "9")],  # A-1 IN->LOW, A-4 NULL->IN; A-2/A-3 absent
}

EXPECTED = [
    ("A|A-1", "10.00", "IN", D1, D2, False),
    ("A|A-1", "11.00", "IN", D2, D3, False),
    ("A|A-1", "11.00", "LOW", D3, None, True),
    ("A|A-2", "20.00", "OUT", D1, None, True),
    ("A|A-3", "30.00", "IN", D1, None, True),
    ("A|A-4", "40.00", None, D2, D3, False),
    ("A|A-4", "40.00", "IN", D3, None, True),
]


def land(landing, days=(D1, D2, D3)):
    for day in days:
        put_csv(landing, "A", day, A_HEADER, A_DAYS[day])


def history_path(config):
    return table_path("gold", config["history"]["tables"]["offer_history"], config)


def history(spark, config):
    rows = spark.read.format("delta").load(str(history_path(config))).collect()
    as_str = lambda d: None if d is None else str(d)  # noqa: E731
    return sorted(
        ((r.offer_key, str(r.price), r.stock_status, as_str(r.valid_from), as_str(r.valid_to), r.is_current) for r in rows),
        key=lambda t: (t[0], t[3]),  # offer_key, valid_from: never NULL, so always comparable
    )


def history_log(spark, config):
    log = spark.read.format("delta").load(str(ops.log_path("history_log", config)))
    return {(r.vendor_id, r.batch_id): (r.rows_in, r.new, r.changed, r.unchanged, r.stale) for r in log.collect()}


def table_versions(spark, config):
    return spark.sql(f"DESCRIBE HISTORY delta.`{history_path(config)}`").count()


def assert_invariants(spark, config):
    table = spark.read.format("delta").load(str(history_path(config)))
    assert scd2.invariant_violations(table).collect() == []


def test_three_days_build_scd2_history(spark, env):  # noqa: F811
    config, _, landing = env
    land(landing)

    results = pipeline.run_all(spark, config)

    assert [r.history.status for r in results] == [pipeline.DONE] * 3
    assert history(spark, config) == EXPECTED
    assert history_log(spark, config) == {
        ("A", D1): (3, 3, 0, 0, 0),
        ("A", D2): (4, 1, 1, 2, 0),
        ("A", D3): (2, 0, 2, 0, 0),
    }
    assert_invariants(spark, config)


def test_history_is_checked_after_every_batch(spark, env):  # noqa: F811
    config, _, landing = env
    for day in (D1, D2, D3):
        land(landing, [day])
        pipeline.run_all(spark, config)
        assert_invariants(spark, config)


def test_rerunning_all_batches_changes_nothing(spark, env):  # noqa: F811
    config, _, landing = env
    land(landing)
    pipeline.run_all(spark, config)
    before = (history(spark, config), history_log(spark, config), table_versions(spark, config))

    second = pipeline.run_all(spark, config)

    assert {r.history.status for r in second} == {pipeline.ALREADY_DONE}
    assert (history(spark, config), history_log(spark, config), table_versions(spark, config)) == before


def test_retry_after_crash_between_merge_and_log_keeps_history_identical(spark, env):  # noqa: F811
    config, _, landing = env
    land(landing)
    pipeline.run_all(spark, config)
    spark.sql(f"DELETE FROM delta.`{ops.log_path('history_log', config)}` WHERE batch_id = '{D3}'")

    outcome = pipeline.history_batch(spark, config, "A", D3)

    assert outcome.status == pipeline.DONE
    assert history(spark, config) == EXPECTED
    assert history_log(spark, config)[("A", D3)] == (2, 0, 0, 0, 2)  # rows already applied count as stale
    assert_invariants(spark, config)


def test_replaying_an_old_batch_after_newer_ones_cannot_rewrite_history(spark, env):  # noqa: F811
    config, _, landing = env
    land(landing)
    pipeline.run_all(spark, config)
    versions = table_versions(spark, config)
    spark.sql(f"DELETE FROM delta.`{ops.log_path('history_log', config)}` WHERE batch_id = '{D1}'")

    outcome = pipeline.history_batch(spark, config, "A", D1)

    assert outcome.counts == {"rows_in": 3, "NEW": 0, "CHANGED": 0, "UNCHANGED": 0, "STALE": 3}
    assert history(spark, config) == EXPECTED
    assert table_versions(spark, config) == versions  # nothing to merge, so no new table version


def test_vendors_are_independent_and_quarantined_batch_gets_no_history(spark, env):  # noqa: F811
    config, _, landing = env
    put_csv(landing, "A", D1, ["item_sku"] + A_HEADER[1:], A_DAYS[D1])  # required column renamed: quarantined
    put_jsonl(landing, "C", D1, C_ROWS)

    results = {r.bronze.vendor_id: r.history.status for r in pipeline.run_all(spark, config)}

    assert results == {"A": pipeline.NOT_READY, "C": pipeline.DONE}
    assert [k for k, *_ in history(spark, config)] == ["C|c_1", "C|c_2"]


def test_invariant_checker_flags_broken_history(spark):
    rows = [
        ("A|1", "A", "1", "1.00", "IN", "h", date(2025, 3, 1), date(2025, 3, 3), False),
        ("A|1", "A", "1", "2.00", "IN", "h", date(2025, 3, 2), None, True),  # overlaps the row above
        ("A|2", "A", "2", "1.00", "IN", "h", date(2025, 3, 1), None, True),
        ("A|2", "A", "2", "1.00", "IN", "h", date(2025, 3, 1), None, True),  # two current rows
    ]
    df = spark.createDataFrame(
        [(*r[:3], Decimal(r[3]), *r[4:]) for r in rows], scd2.HISTORY_SCHEMA
    )

    problems = {(r.offer_key, r.problem) for r in scd2.invariant_violations(df).collect()}

    assert ("A|1", "GAP_OR_OVERLAP") in problems and ("A|2", "NOT_ONE_CURRENT") in problems
