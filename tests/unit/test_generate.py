import copy
import csv
import hashlib
import json
from pathlib import Path

import pytest

from catalog_pipeline.config import load_config
from catalog_pipeline.generate.catalogue import build_catalogue
from catalog_pipeline.generate.writer import generate_dataset

DATES = ["2025-03-01", "2025-03-02", "2025-03-03"]
VENDORS = ["A", "B", "C", "D"]
SKU_FIELD = {"A": "sku", "B": "item_id", "C": "sku", "D": "id"}


def raw_records(n: int = 60) -> list[dict]:
    """Tiny hand-built stand-in for the source JSONL (same shape as Amazon item metadata)."""
    return [
        {
            "parent_asin": f"B{i:05d}",
            "title": f"Acme Series {i} Stainless Steel Countertop Ice Maker Machine {i}",
            "store": f"Visit the Brand{i % 5} Store",
            "details": {"Brand": f"Brand{i % 5}", "Item model number": f"X{i}-{i * 7}"},
            "categories": ["Appliances", "Ice Makers"],
            "average_rating": 4.0,
        }
        for i in range(n)
    ]


def tiny_config(root: Path, seed: int = 42) -> dict:
    config = copy.deepcopy(load_config())
    gen = config["generate"]
    gen["seed"] = seed
    gen["landing_dir"] = str(root / "landing")
    gen["ground_truth_path"] = str(root / "ground_truth.csv")
    gen["brand_aliases_path"] = str(root / "brand_aliases.csv")
    gen["mess"]["duplicate_row_rate"] = 0.2
    gen["mess"]["null_sku_rate"] = 0.1
    gen["change_rate"] = 0.2
    gen["new_item_rate"] = 0.2
    gen["resend_rate"] = 0.2
    return config


def read_feed(root: Path, vendor: str, day: int) -> tuple[list[str] | None, list[dict]]:
    path = root / "landing" / vendor / DATES[day]
    if vendor in ("A", "B"):
        delimiter = "," if vendor == "A" else ";"
        with (path / "products.csv").open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            return reader.fieldnames, list(reader)
    lines = (path / "products.jsonl").read_text(encoding="utf-8").splitlines()
    return None, [json.loads(line) for line in lines]


def tree_hashes(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


@pytest.fixture
def generated(tmp_path):
    config = tiny_config(tmp_path)
    summary = generate_dataset(config, raw_records())
    return tmp_path, summary


def test_files_exist_for_every_vendor_and_day(generated):
    root, summary = generated
    for vendor in VENDORS:
        for date in DATES:
            name = "products.csv" if vendor in ("A", "B") else "products.jsonl"
            assert (root / "landing" / vendor / date / name).is_file()
    assert (root / "ground_truth.csv").is_file()
    assert summary["products"] == 60


def test_generation_is_deterministic(tmp_path):
    out1, out2, out3 = tmp_path / "one", tmp_path / "two", tmp_path / "three"
    generate_dataset(tiny_config(out1), raw_records())
    generate_dataset(tiny_config(out2), raw_records())
    generate_dataset(tiny_config(out3, seed=7), raw_records())

    same = tree_hashes(out1)
    assert same and same == tree_hashes(out2)
    assert same != tree_hashes(out3)


def test_day_one_is_snapshot_and_later_days_are_change_feeds(generated):
    root, _ = generated
    for vendor in VENDORS:
        day1 = len(read_feed(root, vendor, 0)[1])
        day2 = len(read_feed(root, vendor, 1)[1])
        assert day1 > day2 > 0


def test_injected_drift(generated):
    root, _ = generated
    # A adds `rating` on day 2 (ADDED)
    assert "rating" not in read_feed(root, "A", 0)[0]
    assert "rating" in read_feed(root, "A", 1)[0]
    # B renames title -> name on day 3 (RENAMED)
    assert "title" in read_feed(root, "B", 1)[0] and "name" not in read_feed(root, "B", 1)[0]
    assert "name" in read_feed(root, "B", 2)[0] and "title" not in read_feed(root, "B", 2)[0]
    # C price number -> string on day 3 (TYPE_CHANGED)
    for day in (0, 1):
        prices = [r["price"] for r in read_feed(root, "C", day)[1] if r["price"] is not None]
        assert prices and all(isinstance(p, (int, float)) for p in prices)
    prices = [r["price"] for r in read_feed(root, "C", 2)[1] if r["price"] is not None]
    assert prices and all(isinstance(p, str) for p in prices)


def test_locale_prices_and_nested_json(generated):
    root, _ = generated
    a_prices = [r["price"] for r in read_feed(root, "A", 0)[1] if r["price"]]
    b_prices = [r["price"] for r in read_feed(root, "B", 0)[1] if r["price"]]
    assert all(p.startswith("$") and "." in p for p in a_prices)
    assert all("," in p and "$" not in p for p in b_prices)
    d_row = read_feed(root, "D", 0)[1][0]
    assert isinstance(d_row["product"], dict) and isinstance(d_row["offer"], dict)


def test_ground_truth_covers_every_keyed_row(generated):
    root, summary = generated
    with (root / "ground_truth.csv").open(encoding="utf-8", newline="") as f:
        truth = {(r["vendor_id"], r["vendor_sku"]): r["true_product_id"] for r in csv.DictReader(f)}
    assert len(truth) == summary["ground_truth_rows"]

    null_sku_rows = 0
    for vendor in VENDORS:
        for day in range(3):
            for row in read_feed(root, vendor, day)[1]:
                sku = row[SKU_FIELD[vendor]]
                if sku in (None, ""):
                    null_sku_rows += 1
                else:
                    assert (vendor, sku) in truth
    assert null_sku_rows > 0  # injected mess is actually present


def test_in_file_duplicates_are_present(generated):
    root, _ = generated
    rows = read_feed(root, "C", 0)[1]
    keyed = [r["sku"] for r in rows if r["sku"]]
    assert len(keyed) > len(set(keyed))


def test_catalogue_rules(tmp_path):
    records = raw_records(40) + [
        {"parent_asin": "B00000", "title": "Duplicate asin", "store": "Dup"},  # repeated parent_asin
        {"parent_asin": "NOTITLE", "title": None, "store": "Acme"},  # no title
        {"parent_asin": "NOBRAND", "title": "Orphan product", "store": None, "details": {}},  # no brand
        {"parent_asin": "STORE", "title": "Store brand fallback", "store": "Visit the Zeta Store", "details": {}},
    ]
    products = build_catalogue(records, tiny_config(tmp_path))

    assert len(products) == 41
    assert len({p.true_product_id for p in products}) == 41
    assert products[0].brand == "Brand0" and products[0].model == "X0-0" and products[0].category == "Ice Makers"
    assert products[-1].brand == "Zeta" and products[-1].model is None  # store name cleaned, used as brand

    gtins = [p.gtin for p in products if p.gtin]
    assert 0 < len(gtins) < len(products) and len(set(gtins)) == len(gtins)
    for gtin in gtins:  # valid EAN-13: weighted digit sum is a multiple of 10
        assert len(gtin) == 13
        assert sum(int(d) * (3 if i % 2 else 1) for i, d in enumerate(gtin)) % 10 == 0
