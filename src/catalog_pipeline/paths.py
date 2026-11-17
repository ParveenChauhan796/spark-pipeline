"""Resolves lake paths (bronze/silver/gold/ops) from config. No hardcoded paths."""
from pathlib import Path

from catalog_pipeline.config import PROJECT_ROOT, load_config


def resolve(path: Path | str) -> Path:
    """Make a config path absolute; relative paths are relative to the project root."""
    p = Path(path)
    return p if p.is_absolute() else PROJECT_ROOT / p


def layer_path(layer: str, config: dict | None = None) -> Path:
    """Directory of one lake layer, e.g. layer_path('bronze') -> <root>/data/lake/bronze."""
    config = config or load_config()
    lake = config["lake"]
    return resolve(lake["root"]) / lake["layers"][layer]


def table_path(layer: str, table: str, config: dict | None = None) -> Path:
    """Directory of a Delta table inside a layer, e.g. table_path('gold', 'offer_history')."""
    return layer_path(layer, config) / table
