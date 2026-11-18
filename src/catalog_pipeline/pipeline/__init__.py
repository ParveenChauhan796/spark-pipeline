"""Stage orchestration: every lake read and write outside ingest/ happens in this package.

One module per stage (silver, history, er, reports), plus run.py (all batches in date order) and
cli.py. The pure logic they call lives in transform/, history/, match/ and reports/.
"""
from catalog_pipeline.pipeline.er import load_ground_truth, resolve_entities
from catalog_pipeline.pipeline.history import history_batch, table_version
from catalog_pipeline.pipeline.outcomes import (
    ALREADY_DONE,
    DONE,
    NOT_INGESTED,
    NOT_READY,
    BatchResult,
    ErOutcome,
    HistoryOutcome,
    SilverOutcome,
)
from catalog_pipeline.pipeline.reports import build_reports
from catalog_pipeline.pipeline.run import discover_batches, run_all
from catalog_pipeline.pipeline.silver import load_brand_aliases, silver_batch

__all__ = [
    "ALREADY_DONE", "DONE", "NOT_INGESTED", "NOT_READY", "BatchResult", "ErOutcome", "HistoryOutcome",
    "SilverOutcome", "build_reports", "discover_batches", "history_batch", "load_brand_aliases",
    "load_ground_truth", "resolve_entities", "run_all", "silver_batch", "table_version",
]
