"""Rebuild the gold report tables (the report logic is reports/reports.py)."""
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from catalog_pipeline.paths import er_table_path, offer_history_path, silver_table_path, table_path
from catalog_pipeline.reports import reports


def build_reports(spark: SparkSession, config: dict) -> dict[str, int] | None:
    """Rebuild every gold report table (overwrite, D36). Returns row counts, or None before ER has run."""
    xref_path, dim_path = er_table_path("product_xref", config), er_table_path("dim_product", config)
    if not all(DeltaTable.isDeltaTable(spark, str(p)) for p in (xref_path, dim_path, offer_history_path(config))):
        return None
    cfg = config["reports"]
    load = lambda path: spark.read.format("delta").load(str(path))  # noqa: E731
    products, history = load(silver_table_path("vendor_product", config)), load(offer_history_path(config))
    xref, dim = load(xref_path), load(dim_path)

    built = {
        "attribute_completeness": reports.attribute_completeness(products, dim, cfg["completeness_columns"]),
        "price_changes_by_brand": reports.price_changes_by_brand(reports.price_changes(history), xref, dim),
        "price_spread": reports.price_spread(history, xref, cfg["min_vendors_for_spread"]),
    }
    counts = {}
    for name, df in built.items():
        path = table_path("gold", cfg["tables"][name], config)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(str(path))
        counts[name] = load(path).count()
    return counts
