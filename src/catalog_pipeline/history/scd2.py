"""SCD2 for gold.offer_history: pure functions that decide what changed and stage the MERGE source.

SCD2 (slowly changing dimension, type 2) keeps history by never overwriting a value: when an
offer's price or stock_status changes, its current row is closed (valid_to set, is_current false)
and a new version is inserted. Each version is valid for the half-open interval
[valid_from, valid_to), and valid_to is NULL while the version is current.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DecimalType, StringType, StructField, StructType

NEW = "NEW"
CHANGED = "CHANGED"
UNCHANGED = "UNCHANGED"
STALE = "STALE"  # batch date not newer than the offer's current version: replay, retry or same-day resend
CHANGE_TYPES = (NEW, CHANGED, UNCHANGED, STALE)

HISTORY_SCHEMA = StructType(
    [
        StructField("offer_key", StringType(), False),  # vendor_id|vendor_sku
        StructField("vendor_id", StringType(), False),
        StructField("vendor_sku", StringType(), False),
        StructField("price", DecimalType(12, 2), False),
        StructField("stock_status", StringType(), True),  # NULL only while the qty has never been known
        StructField("row_hash", StringType(), False),
        StructField("valid_from", DateType(), False),
        StructField("valid_to", DateType(), True),  # NULL while current
        StructField("is_current", BooleanType(), False),
    ]
)
HISTORY_COLUMNS = [f.name for f in HISTORY_SCHEMA.fields]


def offer_key(vendor_id: Column, vendor_sku: Column) -> Column:
    return F.concat_ws("|", vendor_id, vendor_sku)


def row_hash(price: Column, stock_status: Column) -> Column:
    """sha2 over the tracked attributes. Comparing one hash replaces comparing every column, and
    NULL stock is turned into an explicit marker so (price, NULL) still hashes to a stable value."""
    return F.sha2(F.concat_ws("|", price.cast("string"), F.coalesce(stock_status, F.lit("<null>"))), 256)


def classify_changes(offers: DataFrame, current: DataFrame) -> DataFrame:
    """Compare one batch of silver.vendor_offer rows with the current history rows.

    offers:  vendor_id, vendor_sku, batch_date, price, stock_status (one row per offer)
    current: the is_current rows of gold.offer_history (any vendors; extra rows are harmless)
    Returns one row per offer with the history columns of the candidate new version plus
    `change_type` (NEW | CHANGED | UNCHANGED | STALE).
    """
    cur = current.select(
        "offer_key",
        F.col("stock_status").alias("cur_stock_status"),
        F.col("row_hash").alias("cur_row_hash"),
        F.col("valid_from").alias("cur_valid_from"),
    )
    # Left join: every incoming offer is kept, and offers never seen before get NULL cur_* columns.
    joined = offers.withColumn("offer_key", offer_key(F.col("vendor_id"), F.col("vendor_sku"))).join(
        cur, "offer_key", "left"
    )
    # Unknown stock (no qty in this batch) keeps the last known status instead of becoming a change.
    stock = F.coalesce(F.col("stock_status"), F.col("cur_stock_status"))
    candidate = joined.withColumn("stock_status", stock).withColumn(
        "row_hash", row_hash(F.col("price"), F.col("stock_status"))
    )
    change_type = (
        F.when(F.col("cur_row_hash").isNull(), NEW)
        .when(F.col("batch_date") <= F.col("cur_valid_from"), STALE)
        .when(F.col("row_hash") == F.col("cur_row_hash"), UNCHANGED)
        .otherwise(CHANGED)
    )
    return candidate.select(
        "offer_key",
        "vendor_id",
        "vendor_sku",
        F.col("price").cast(DecimalType(12, 2)).alias("price"),
        "stock_status",
        "row_hash",
        F.col("batch_date").alias("valid_from"),
        change_type.alias("change_type"),
    )


def stage_merge(classified: DataFrame) -> DataFrame:
    """The MERGE source. A changed offer appears twice:
    - merge_key = offer_key: matches the current row, so the MERGE closes it;
    - merge_key = NULL: a NULL key never equals anything, so the MERGE inserts it as the new version.
    New offers appear once with a NULL key. Unchanged and stale offers are left out entirely.
    """
    changed = classified.filter(F.col("change_type") == CHANGED)
    inserts = classified.filter(F.col("change_type").isin(NEW, CHANGED))
    return changed.withColumn("merge_key", F.col("offer_key")).unionByName(
        inserts.withColumn("merge_key", F.lit(None).cast("string"))
    )


def invariant_violations(history: DataFrame) -> DataFrame:
    """Rows (offer_key, problem) breaking an SCD2 rule; empty when the history is consistent.

    Rules: exactly one current row per offer; versions are back to back (each valid_to equals the
    next version's valid_from, so no gaps and no overlaps); valid_from < valid_to; only the last
    version is open (valid_to NULL) and it is the current one.
    """
    # lead() looks at the next version of the same offer (ordered by valid_from) without a self-join.
    by_offer = Window.partitionBy("offer_key").orderBy("valid_from")
    v = history.withColumn("next_from", F.lead("valid_from").over(by_offer))
    per_row = v.select(
        "offer_key",
        F.when(F.col("next_from").isNotNull() & ~F.col("valid_to").eqNullSafe(F.col("next_from")), "GAP_OR_OVERLAP")
        .when(F.col("valid_to").isNotNull() & (F.col("valid_from") >= F.col("valid_to")), "EMPTY_INTERVAL")
        .when(F.col("next_from").isNull() & (F.col("valid_to").isNotNull() | ~F.col("is_current")), "LAST_NOT_CURRENT")
        .when(F.col("next_from").isNotNull() & F.col("is_current"), "CURRENT_NOT_LAST")
        .alias("problem"),
    ).filter(F.col("problem").isNotNull())
    per_offer = (
        history.groupBy("offer_key")
        .agg(F.sum(F.col("is_current").cast("int")).alias("n_current"))
        .filter(F.col("n_current") != 1)
        .select("offer_key", F.lit("NOT_ONE_CURRENT").alias("problem"))
    )
    return per_row.unionByName(per_offer)
