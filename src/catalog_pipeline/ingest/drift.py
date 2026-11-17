"""Schema drift detection: a pure function, no Spark and no I/O.

detect_drift(observed schema, contract) -> DriftResult(drifts, rename_mapping, verdict)

Observed schema is {column name as found in the feed: type category}, where the type category is
"string", "number", "struct", ... (CSV columns are always "string").
"""
from collections.abc import Mapping
from dataclasses import dataclass

from catalog_pipeline.ingest.contracts import Contract, normalize_name

ADDED = "ADDED"
MISSING = "MISSING"
RENAMED = "RENAMED"
TYPE_CHANGED = "TYPE_CHANGED"

OK = "OK"
QUARANTINE = "QUARANTINE"


@dataclass(frozen=True)
class Drift:
    drift_type: str
    source_column: str  # the name in the feed (or the expected name, for MISSING)
    canonical_column: str | None  # None for ADDED columns
    is_required: bool | None
    expected_type: str | None
    observed_type: str | None


@dataclass(frozen=True)
class DriftResult:
    drifts: tuple[Drift, ...]
    rename_mapping: Mapping[str, str]  # observed column name -> canonical name, for every matched column
    verdict: str  # OK | QUARANTINE
    quarantine_reasons: tuple[str, ...]


def detect_drift(observed: Mapping[str, str], contract: Contract) -> DriftResult:
    """Compare the columns found in a batch with the vendor contract.

    * expected name present (trimmed, case-insensitive)      -> matched, no drift
    * expected name absent but a declared alias present      -> RENAMED, mapped to the canonical name
    * both expected name and an alias present                -> expected name wins; alias is ADDED
    * neither present                                        -> MISSING (required => quarantine batch)
    * column not in the contract (and not a claimed alias)   -> ADDED (ignored downstream)
    * JSON only: matched column whose type differs           -> TYPE_CHANGED (logged, still ingested)
    """
    by_norm: dict[str, list[str]] = {}
    for name in observed:
        by_norm.setdefault(normalize_name(name), []).append(name)

    matched: dict[str, str] = {}  # canonical -> observed name
    claimed: set[str] = set()  # observed names that map to a contract column

    # Pass 1: expected names win over aliases.
    for col in contract.columns:
        hits = by_norm.get(normalize_name(col.source), [])
        if hits:
            matched[col.canonical] = hits[0]
            claimed.add(hits[0])

    # Pass 2: aliases, for columns whose expected name is absent.
    renamed: set[str] = set()
    for col in contract.columns:
        if col.canonical in matched:
            continue
        for alias in col.aliases:
            hits = [n for n in by_norm.get(normalize_name(alias), []) if n not in claimed]
            if hits:
                matched[col.canonical] = hits[0]
                claimed.add(hits[0])
                renamed.add(col.canonical)
                break

    drifts: list[Drift] = []
    reasons: list[str] = []
    for col in contract.columns:
        if col.canonical not in matched:
            drifts.append(Drift(MISSING, col.source, col.canonical, col.required, col.type, None))
            if col.required:
                reasons.append(f"required column {col.canonical!r} (expected {col.source!r}) is missing")
            continue
        name = matched[col.canonical]
        if col.canonical in renamed:
            drifts.append(Drift(RENAMED, name, col.canonical, col.required, col.type, observed[name]))
        if contract.format == "json" and observed[name] != col.type:
            drifts.append(Drift(TYPE_CHANGED, name, col.canonical, col.required, col.type, observed[name]))

    for name in observed:  # in feed order
        if name not in claimed:
            drifts.append(Drift(ADDED, name, None, None, None, observed[name]))

    mapping = {matched[col.canonical]: col.canonical for col in contract.columns if col.canonical in matched}
    return DriftResult(
        drifts=tuple(drifts),
        rename_mapping=mapping,
        verdict=QUARANTINE if reasons else OK,
        quarantine_reasons=tuple(reasons),
    )

