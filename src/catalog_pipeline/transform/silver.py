"""Bronze -> silver for one vendor batch, as pure DataFrame functions (no I/O).

classify() cleans every bronze row once and marks it as either valid or quarantined (with a reason),
and ranks valid rows that share a key. The to_* functions then cut that one frame into the silver
outputs, and batch_counts() reports the numbers that must reconcile:
rows_in == valid + quarantined + deduped.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from catalog_pipeline.transform import brand, gtin, price, stock, text

RAW_COLUMNS = ["title", "brand", "category", "model", "price", "qty", "gtin"]


def standardize(df: DataFrame) -> DataFrame:
    """Give every vendor's bronze rows the same flat shape; all values are still raw strings.

    Vendor D keeps `product` and `offer` as nested structs in bronze; they are flattened here.
    Columns a vendor does not have (e.g. `model` for vendor C) become null.
    """
    if "product" in df.columns:
        df = (
            df.withColumn("title", F.col("product.name"))
            .withColumn("brand", F.col("product.brand"))
            .withColumn("category", F.col("product.category"))
            .withColumn("model", F.col("product.model"))
            .withColumn("price", F.col("offer.price"))
            .withColumn("qty", F.col("offer.qty"))
        )
    for name in RAW_COLUMNS:
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast("string"))
    return df.select(
        F.col("_vendor_id").alias("vendor_id"), "vendor_sku", "batch_date", "_row_id", "_source_file", *RAW_COLUMNS
    )


def _blank(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == "")


def classify(df: DataFrame, aliases: DataFrame, config: dict) -> DataFrame:
    """Clean a bronze batch and flag each row.

    Adds: price_usd, qty_int, stock_status, title_norm, title_tokens, category_norm, gtin14, brand
    (canonical) / brand_raw, `reason` (null when valid) and `rn` (1 = latest row of its key among
    rows of the same validity).
    """
    silver_cfg = config["silver"]
    suffixes = config["normalization"]["brand_legal_suffixes"]

    cleaned = (
        standardize(df)
        .withColumn("price_usd", price.parse_price(F.col("price")))
        .withColumn("qty_int", stock.parse_qty(F.col("qty")))
    )
    cleaned = (
        cleaned.withColumn("stock_status", stock.stock_status(F.col("qty_int"), silver_cfg["stock_low_max_qty"]))
        .withColumn("title_norm", text.title_norm(F.col("title")))
        .withColumn("title_tokens", text.title_tokens(F.col("title_norm")))
        .withColumn("category_norm", text.category_norm(F.col("category")))
        .withColumn(
            "gtin14", gtin.to_gtin14(F.col("gtin"), silver_cfg["gtin_min_length"], silver_cfg["gtin_max_length"])
        )
    )
    cleaned = brand.canonicalize_brand(cleaned, aliases, suffixes)

    # First failing check wins. Bad rows are kept (not dropped) and routed to silver.quarantine.
    reason = (
        F.when(_blank(F.col("vendor_sku")), "NULL_SKU")
        .when(F.col("title_norm").isNull(), "MISSING_TITLE")
        .when(_blank(F.col("price")), "MISSING_PRICE")
        .when(F.col("price_usd").isNull(), "PRICE_UNPARSEABLE")
        .when(F.col("price_usd") <= 0, "PRICE_NON_POSITIVE")
        .when(~_blank(F.col("qty")) & F.col("qty_int").isNull(), "QTY_UNPARSEABLE")
    )
    flagged = cleaned.withColumn("reason", reason).withColumn("is_valid", F.col("reason").isNull())

    # Window function: like groupBy it works per key, but it keeps every row and adds a value to
    # each. Here row_number() numbers the rows of one key from the last file line backwards, so
    # rn == 1 is "latest row wins". Partitioning also by is_valid ranks valid rows among themselves,
    # so a later broken row can never push out an earlier good one.
    latest_first = Window.partitionBy("vendor_id", "vendor_sku", "is_valid").orderBy(F.col("_row_id").desc())
    return flagged.withColumn("rn", F.row_number().over(latest_first))


def to_offers(classified: DataFrame) -> DataFrame:
    """silver.vendor_offer rows: one per offer per batch."""
    return classified.filter(F.col("is_valid") & (F.col("rn") == 1)).select(
        "vendor_id",
        "vendor_sku",
        "batch_date",
        F.col("price_usd").alias("price"),
        F.col("qty_int").alias("qty"),
        "stock_status",
    )


def to_products(classified: DataFrame) -> DataFrame:
    """silver.vendor_product rows from one batch: latest descriptive values per key."""
    return classified.filter(F.col("is_valid") & (F.col("rn") == 1)).select(
        "vendor_id",
        "vendor_sku",
        "title",
        "title_norm",
        "title_tokens",
        "brand_raw",
        "brand",
        "category",
        "category_norm",
        "model",
        F.col("gtin14"),
        F.col("batch_date").alias("last_batch_date"),
    )


def to_quarantine(classified: DataFrame) -> DataFrame:
    """silver.quarantine rows: invalid rows with their raw values and the reason."""
    return classified.filter(~F.col("is_valid")).select(
        "vendor_id",
        "vendor_sku",
        "batch_date",
        "reason",
        "title",
        F.col("brand_raw").alias("brand"),
        F.col("price").alias("price_raw"),
        F.col("qty").alias("qty_raw"),
        F.col("gtin").alias("gtin_raw"),
        "_source_file",
        "_row_id",
    )


def batch_counts(classified: DataFrame) -> dict[str, int]:
    """rows_in, valid, quarantined, deduped from one pass. Each is counted independently, so the
    identity rows_in == valid + quarantined + deduped is a real check, not a tautology."""
    row = classified.agg(
        F.count(F.lit(1)).alias("rows_in"),
        F.sum(F.when(F.col("is_valid") & (F.col("rn") == 1), 1).otherwise(0)).alias("valid"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("quarantined"),
        F.sum(F.when(F.col("is_valid") & (F.col("rn") > 1), 1).otherwise(0)).alias("deduped"),
    ).collect()[0]  # a single row of four numbers
    return {k: int(row[k] or 0) for k in ("rows_in", "valid", "quarantined", "deduped")}
