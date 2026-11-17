"""Write generated feeds and ground truth to disk (deterministic bytes for a given seed)."""
import csv
import json
from collections.abc import Iterable
from pathlib import Path

from catalog_pipeline.generate.brands import write_brand_aliases
from catalog_pipeline.generate.catalogue import build_catalogue
from catalog_pipeline.generate.vendors import Batch, build_batches
from catalog_pipeline.paths import resolve


def write_csv(path: Path, columns: list[str], rows: list[dict], delimiter: str) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter=delimiter, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: "" if v is None else v for k, v in row.items()})


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_ground_truth(path: Path, truth: dict[tuple[str, str], str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["vendor_id", "vendor_sku", "true_product_id"])
        for (vendor_id, sku), true_id in sorted(truth.items()):
            writer.writerow([vendor_id, sku, true_id])


def batch_path(landing: Path, batch: Batch) -> Path:
    return landing / batch.vendor_id / batch.batch_date / batch.filename


def generate_dataset(config: dict, records: Iterable[dict]) -> dict:
    """Build the catalogue, simulate all vendor batches and write everything to disk.

    Files land in <landing_dir>/<vendor>/<YYYY-MM-DD>/ and the ground truth in ground_truth_path.
    Existing files with the same names are overwritten (re-running with the same seed is a no-op).
    """
    gen = config["generate"]
    products = build_catalogue(records, config)
    batches, truth = build_batches(products, config)

    landing = resolve(gen["landing_dir"])
    for batch in batches:
        path = batch_path(landing, batch)
        path.parent.mkdir(parents=True, exist_ok=True)
        if batch.fmt == "csv":
            write_csv(path, batch.columns, batch.rows, batch.delimiter)
        else:
            write_jsonl(path, batch.rows)
    write_ground_truth(resolve(gen["ground_truth_path"]), truth)
    n_brands = write_brand_aliases(
        [p.brand for p in products],
        config["normalization"]["brand_legal_suffixes"],
        resolve(gen["brand_aliases_path"]),
    )

    return {
        "products": len(products),
        "ground_truth_rows": len(truth),
        "brand_aliases": n_brands,
        "batches": [(b.vendor_id, b.batch_date, len(b.rows)) for b in batches],
    }
