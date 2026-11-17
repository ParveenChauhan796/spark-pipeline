"""Loads conf/pipeline.yaml. All settings and thresholds live there, not in code."""
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "conf" / "pipeline.yaml"


def load_config(path: Path | str | None = None) -> dict:
    """Return the parsed pipeline config (defaults to conf/pipeline.yaml)."""
    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    with config_path.open() as f:
        return yaml.safe_load(f)
