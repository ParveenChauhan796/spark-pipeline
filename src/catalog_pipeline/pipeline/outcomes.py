"""Stage statuses and the result records every stage returns."""
from dataclasses import dataclass

from catalog_pipeline.ingest.bronze import BatchOutcome

DONE = "DONE"
ALREADY_DONE = "ALREADY_DONE"
NOT_INGESTED = "NOT_INGESTED"
NOT_READY = "NOT_READY"  # history: silver has not processed this batch; ER: no input tables yet


@dataclass(frozen=True)
class SilverOutcome:
    vendor_id: str
    batch_id: str
    status: str  # DONE | ALREADY_DONE | NOT_INGESTED
    counts: dict[str, int] | None = None
    reasons: dict[str, int] | None = None


@dataclass(frozen=True)
class HistoryOutcome:
    vendor_id: str
    batch_id: str
    status: str  # DONE | ALREADY_DONE | NOT_READY
    counts: dict[str, int] | None = None  # rows_in, NEW, CHANGED, UNCHANGED, STALE
    table_version: int | None = None


@dataclass(frozen=True)
class BatchResult:
    """What every stage did with one vendor batch."""
    bronze: BatchOutcome
    silver: SilverOutcome
    history: HistoryOutcome


@dataclass(frozen=True)
class ErOutcome:
    status: str  # DONE | ALREADY_DONE | NOT_READY
    metrics: dict | None = None
