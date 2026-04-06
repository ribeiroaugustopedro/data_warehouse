from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass(frozen=True)
class DatasetConfig:
    """In-memory representation of a dataset YAML file."""
    name: str
    raw: Dict[str, Any]


def load_dataset_config(path: str | Path) -> DatasetConfig:
    """Load a dataset YAML config from disk."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Dataset config not found: {p}")

    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    name = raw.get("name") or p.stem
    return DatasetConfig(name=name, raw=raw)
