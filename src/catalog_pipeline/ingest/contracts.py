"""Vendor contracts (conf/vendors/*.yaml): what a vendor feed is expected to look like."""
from dataclasses import dataclass
from pathlib import Path

import yaml

FORMATS = {"csv", "json"}
TYPES = {"string", "number", "struct"}


def normalize_name(name: str) -> str:
    """Column names are compared trimmed and case-insensitively."""
    return name.strip().lower()


@dataclass(frozen=True)
class Column:
    source: str  # expected column name in the vendor feed
    canonical: str  # name used in bronze and everywhere downstream
    type: str  # "string" | "number" | "struct"
    required: bool
    aliases: tuple[str, ...] = ()  # other known source names for the same column
    fields: tuple[tuple[str, str], ...] = ()  # (name, type) of struct members


@dataclass(frozen=True)
class Contract:
    vendor_id: str
    format: str
    read_options: dict
    columns: tuple[Column, ...]


class ContractError(ValueError):
    pass


def _parse_column(vendor_id: str, raw: dict) -> Column:
    try:
        col = Column(
            source=raw["source"],
            canonical=raw["canonical"],
            type=raw["type"],
            required=bool(raw["required"]),
            aliases=tuple(raw.get("aliases") or ()),
            fields=tuple((raw.get("fields") or {}).items()),
        )
    except KeyError as e:
        raise ContractError(f"vendor {vendor_id}: column {raw!r} is missing key {e}") from e
    if col.type not in TYPES:
        raise ContractError(f"vendor {vendor_id}: column {col.canonical!r} has unknown type {col.type!r}")
    if col.type == "struct" and not col.fields:
        raise ContractError(f"vendor {vendor_id}: struct column {col.canonical!r} needs `fields`")
    return col


def validate_contract(contract: Contract) -> None:
    """Raise ContractError if the contract is invalid."""
    vid = contract.vendor_id
    if contract.format not in FORMATS:
        raise ContractError(f"vendor {vid}: unknown format {contract.format!r}")
    if not contract.columns:
        raise ContractError(f"vendor {vid}: no columns")

    canonicals = [c.canonical for c in contract.columns]
    if len(set(canonicals)) != len(canonicals):
        raise ContractError(f"vendor {vid}: duplicate canonical column names")

    alias_owner: dict[str, str] = {}
    for col in contract.columns:
        for alias in col.aliases:
            key = normalize_name(alias)
            if key in alias_owner:
                raise ContractError(
                    f"vendor {vid}: alias {alias!r} appears on more than one column "
                    f"({alias_owner[key]!r} and {col.canonical!r})"
                )
            alias_owner[key] = col.canonical


def load_contract(path: Path | str) -> Contract:
    with Path(path).open() as f:
        raw = yaml.safe_load(f)
    vendor_id = str(raw["vendor_id"])
    contract = Contract(
        vendor_id=vendor_id,
        format=raw["format"],
        read_options=dict(raw.get("read_options") or {}),
        columns=tuple(_parse_column(vendor_id, c) for c in raw["columns"]),
    )
    validate_contract(contract)
    return contract


def load_contracts(directory: Path | str) -> dict[str, Contract]:
    """Load every *.yaml in the directory, keyed by vendor_id."""
    contracts = {}
    for path in sorted(Path(directory).glob("*.yaml")):
        contract = load_contract(path)
        if contract.vendor_id in contracts:
            raise ContractError(f"duplicate vendor_id {contract.vendor_id!r} in {path}")
        contracts[contract.vendor_id] = contract
    return contracts
