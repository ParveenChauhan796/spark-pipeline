"""Fetch a slice of the public source dataset (Amazon Reviews 2023 item metadata)."""
import json
from collections.abc import Iterator
from pathlib import Path

import requests

from catalog_pipeline.paths import resolve


def download_source(config: dict) -> Path:
    """Stream the first `max_records` lines of the source JSONL into data/source/.

    Skips the download when the file already exists. Writes to a .part file first so an
    interrupted download never leaves a truncated file that looks complete.
    """
    src = config["generate"]["source"]
    dest = resolve(src["path"])
    if dest.exists():
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    written = 0
    headers = {"User-Agent": src["user_agent"]}
    with requests.get(src["url"], headers=headers, stream=True, timeout=src["timeout_seconds"]) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as out:
            for line in resp.iter_lines(chunk_size=1 << 16):
                if not line:
                    continue
                out.write(line + b"\n")
                written += 1
                if written >= src["max_records"]:
                    break
    tmp.replace(dest)
    return dest


def read_source_records(path: Path | str, limit: int | None = None) -> Iterator[dict]:
    """Yield parsed records from the source JSONL, skipping unparseable lines."""
    with Path(path).open(encoding="utf-8") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                break
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
