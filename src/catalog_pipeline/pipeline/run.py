"""Run every landed batch through bronze, silver and history, oldest day first."""
import time

from pyspark.sql import SparkSession

from catalog_pipeline.ingest.bronze import SKIPPED, SUCCESS, ingest_batch
from catalog_pipeline.ingest.contracts import load_contracts
from catalog_pipeline.paths import resolve
from catalog_pipeline.pipeline.history import history_batch
from catalog_pipeline.pipeline.outcomes import (
    ALREADY_DONE,
    DONE,
    NOT_INGESTED,
    NOT_READY,
    BatchResult,
    HistoryOutcome,
    SilverOutcome,
)
from catalog_pipeline.pipeline.silver import load_brand_aliases, silver_batch


def discover_batches(config: dict, vendor_ids: list[str]) -> list[tuple[str, str]]:
    """(batch_id, vendor_id) for every batch folder in landing, in date order."""
    landing = resolve(config["ingest"]["landing_dir"])
    found = []
    for vendor_id in vendor_ids:
        vendor_dir = landing / vendor_id
        if vendor_dir.is_dir():
            found += [(d.name, vendor_id) for d in vendor_dir.iterdir() if d.is_dir()]
    return sorted(found)


def run_all(spark: SparkSession, config: dict) -> list[BatchResult]:
    """Run every landed batch through bronze, silver and history, oldest day first. One vendor's
    quarantined batch never stops the others."""
    contracts = load_contracts(resolve(config["ingest"]["contracts_dir"]))
    aliases = load_brand_aliases(spark, config)
    results = []
    for batch_id, vendor_id in discover_batches(config, sorted(contracts)):
        start = time.perf_counter()
        bronze_outcome = ingest_batch(spark, config, contracts[vendor_id], batch_id)
        silver_outcome = (
            silver_batch(spark, config, contracts[vendor_id], batch_id, aliases)
            if bronze_outcome.outcome in (SUCCESS, SKIPPED)
            else SilverOutcome(vendor_id, batch_id, NOT_INGESTED)
        )
        history_outcome = (
            history_batch(spark, config, vendor_id, batch_id)
            if silver_outcome.status in (DONE, ALREADY_DONE)
            else HistoryOutcome(vendor_id, batch_id, NOT_READY)
        )
        result = BatchResult(bronze_outcome, silver_outcome, history_outcome)
        results.append(result)
        print(describe_batch(result, time.perf_counter() - start))
    return results


def describe_batch(result: BatchResult, seconds: float) -> str:
    b, s, h = result.bronze, result.silver, result.history
    drift = ", ".join(f"{d.drift_type}({d.source_column})" for d in b.drifts) or "-"
    bronze = f"bronze {b.outcome}" + (f" rows={b.rows}" if b.rows is not None else f" ({b.reason})")
    silver = f"silver {s.status}"
    if s.counts:
        c = s.counts
        silver += f" in={c['rows_in']} valid={c['valid']} quarantined={c['quarantined']} deduped={c['deduped']}"
    history = f"history {h.status}"
    if h.counts:
        c = h.counts
        history += f" new={c['NEW']} changed={c['CHANGED']} unchanged={c['UNCHANGED']} stale={c['STALE']} v{h.table_version}"
    return f"vendor {b.vendor_id} {b.batch_id}: {bronze}; drift: {drift}; {silver}; {history} [{seconds:.1f}s]"
