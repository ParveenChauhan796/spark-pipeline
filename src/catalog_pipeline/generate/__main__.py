"""`python -m catalog_pipeline.generate`: download the source slice, then generate vendor feeds."""
import time

from catalog_pipeline.config import load_config
from catalog_pipeline.generate.source import download_source, read_source_records
from catalog_pipeline.generate.writer import generate_dataset


def main() -> None:
    config = load_config()
    start = time.perf_counter()

    source_path = download_source(config)
    print(f"source: {source_path}")

    summary = generate_dataset(config, read_source_records(source_path, config["generate"]["source"]["max_records"]))

    print(f"golden products: {summary['products']}")
    for vendor_id, batch_date, n_rows in summary["batches"]:
        print(f"  vendor {vendor_id} {batch_date}: {n_rows} rows")
    print(f"ground truth rows: {summary['ground_truth_rows']}")
    print(f"brand reference rows: {summary['brand_aliases']}")
    print(f"done in {time.perf_counter() - start:.1f}s")


if __name__ == "__main__":
    main()
