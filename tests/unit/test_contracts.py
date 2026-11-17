import textwrap

import pytest

from catalog_pipeline.config import PROJECT_ROOT
from catalog_pipeline.ingest.contracts import ContractError, load_contract, load_contracts


def write(tmp_path, body: str):
    path = tmp_path / "v.yaml"
    path.write_text(textwrap.dedent(body))
    return path


def test_real_contracts_load():
    contracts = load_contracts(PROJECT_ROOT / "conf" / "vendors")
    assert sorted(contracts) == ["A", "B", "C", "D"]
    assert contracts["A"].format == "csv" and contracts["C"].format == "json"
    b_title = next(c for c in contracts["B"].columns if c.canonical == "title")
    assert b_title.source == "title" and b_title.aliases == ("name",)
    d_product = next(c for c in contracts["D"].columns if c.canonical == "product")
    assert d_product.type == "struct" and ("name", "string") in d_product.fields


def test_alias_on_two_columns_is_invalid(tmp_path):
    path = write(
        tmp_path,
        """
        vendor_id: X
        format: csv
        columns:
          - {source: title, canonical: title, type: string, required: true, aliases: [name]}
          - {source: brand, canonical: brand, type: string, required: false, aliases: [" Name "]}
        """,
    )
    with pytest.raises(ContractError, match="more than one column"):
        load_contract(path)


def test_duplicate_canonical_names_are_invalid(tmp_path):
    path = write(
        tmp_path,
        """
        vendor_id: X
        format: csv
        columns:
          - {source: a, canonical: title, type: string, required: true}
          - {source: b, canonical: title, type: string, required: false}
        """,
    )
    with pytest.raises(ContractError, match="duplicate canonical"):
        load_contract(path)


@pytest.mark.parametrize(
    "column, message",
    [
        ("{source: a, canonical: a, type: date, required: true}", "unknown type"),
        ("{source: a, canonical: a, type: struct, required: true}", "needs `fields`"),
        ("{source: a, canonical: a, type: string}", "missing key"),
    ],
)
def test_bad_columns_are_invalid(tmp_path, column, message):
    path = write(tmp_path, f"vendor_id: X\nformat: json\ncolumns:\n  - {column}\n")
    with pytest.raises(ContractError, match=message):
        load_contract(path)


def test_unknown_format_is_invalid(tmp_path):
    path = write(
        tmp_path,
        """
        vendor_id: X
        format: xml
        columns:
          - {source: a, canonical: a, type: string, required: true}
        """,
    )
    with pytest.raises(ContractError, match="unknown format"):
        load_contract(path)
