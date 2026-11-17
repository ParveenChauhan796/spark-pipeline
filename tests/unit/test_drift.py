import pytest

from catalog_pipeline.ingest.contracts import Column, Contract
from catalog_pipeline.ingest.drift import ADDED, MISSING, OK, QUARANTINE, RENAMED, TYPE_CHANGED, detect_drift


def col(source, canonical=None, type="string", required=False, aliases=()):
    return Column(source, canonical or source, type, required, tuple(aliases))


def contract(fmt="csv", *columns):
    return Contract("X", fmt, {}, tuple(columns))


CSV = contract(
    "csv",
    col("sku", "vendor_sku", required=True),
    col("title", required=True, aliases=["name"]),
    col("brand"),
    col("price", required=True),
)
JSON = contract(
    "json",
    col("sku", "vendor_sku", required=True),
    col("price", type="number", required=True),
    col("brand"),
)
CSV_OBSERVED = {"sku": "string", "title": "string", "brand": "string", "price": "string"}


def kinds(result):
    return [(d.drift_type, d.source_column) for d in result.drifts]


def test_clean_batch_has_no_drift():
    result = detect_drift(CSV_OBSERVED, CSV)
    assert result.drifts == () and result.verdict == OK
    assert result.rename_mapping == {"sku": "vendor_sku", "title": "title", "brand": "brand", "price": "price"}


def test_added_column_is_logged_and_ignored():
    result = detect_drift({**CSV_OBSERVED, "rating": "string"}, CSV)
    assert kinds(result) == [(ADDED, "rating")]
    assert result.verdict == OK
    assert "rating" not in result.rename_mapping


def test_declared_rename_maps_to_canonical_and_processes_normally():
    observed = {"sku": "string", "name": "string", "brand": "string", "price": "string"}
    result = detect_drift(observed, CSV)
    assert kinds(result) == [(RENAMED, "name")]
    assert result.drifts[0].canonical_column == "title"
    assert result.rename_mapping["name"] == "title"
    assert result.verdict == OK


def test_alias_matching_is_case_insensitive_and_trimmed():
    observed = {"sku": "string", "  NAME ": "string", "brand": "string", "price": "string"}
    result = detect_drift(observed, CSV)
    assert kinds(result) == [(RENAMED, "  NAME ")]
    assert result.rename_mapping["  NAME "] == "title"


def test_expected_name_wins_when_alias_is_also_present():
    observed = {**CSV_OBSERVED, "name": "string"}
    result = detect_drift(observed, CSV)
    assert kinds(result) == [(ADDED, "name")]
    assert result.rename_mapping["title"] == "title" and "name" not in result.rename_mapping
    assert result.verdict == OK


def test_expected_name_matching_is_case_insensitive():
    observed = {"SKU": "string", "Title": "string", "Brand": "string", "price ": "string"}
    result = detect_drift(observed, CSV)
    assert result.drifts == () and result.verdict == OK


def test_missing_optional_column_warns_but_batch_processes():
    observed = {"sku": "string", "title": "string", "price": "string"}
    result = detect_drift(observed, CSV)
    assert kinds(result) == [(MISSING, "brand")]
    assert result.drifts[0].is_required is False
    assert result.verdict == OK


def test_unknown_rename_of_required_column_quarantines_the_batch():
    # `sku` is renamed to `item_sku`, and that name is not a declared alias.
    observed = {"item_sku": "string", "title": "string", "brand": "string", "price": "string"}
    result = detect_drift(observed, CSV)
    assert kinds(result) == [(MISSING, "sku"), (ADDED, "item_sku")]
    assert result.drifts[0].is_required is True
    assert result.verdict == QUARANTINE
    assert "vendor_sku" in result.quarantine_reasons[0]


def test_json_type_change_is_logged_but_batch_processes():
    observed = {"sku": "string", "price": "string", "brand": "string"}  # price was a number
    result = detect_drift(observed, JSON)
    assert kinds(result) == [(TYPE_CHANGED, "price")]
    assert (result.drifts[0].expected_type, result.drifts[0].observed_type) == ("number", "string")
    assert result.verdict == OK


def test_json_matching_types_have_no_drift():
    result = detect_drift({"sku": "string", "price": "number", "brand": "string"}, JSON)
    assert result.drifts == () and result.verdict == OK


def test_csv_has_no_type_drift():
    # A CSV contract may say `number`, but CSV observed types are always strings: never TYPE_CHANGED.
    numeric_csv = contract("csv", col("sku", required=True), col("price", type="number", required=True))
    result = detect_drift({"sku": "string", "price": "string"}, numeric_csv)
    assert result.drifts == ()


def test_alias_does_not_steal_a_column_that_belongs_to_another_contract_column():
    # `name` is both an alias of title and the expected name of another column: the expected name wins.
    c = contract("csv", col("title", aliases=["name"], required=True), col("name", "product_name"))
    result = detect_drift({"name": "string"}, c)
    assert result.rename_mapping == {"name": "product_name"}
    assert (MISSING, "title") in kinds(result)
