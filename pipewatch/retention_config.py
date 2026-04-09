"""Load RetentionPolicy from a config dict or YAML/JSON file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from pipewatch.retention_policy import RetentionPolicy


def build_policy_from_config(config: Dict[str, Any]) -> RetentionPolicy:
    """Build a RetentionPolicy from a plain dict.

    Expected keys (all optional):
        max_age_days (int): maximum age of a run record in days.
        max_runs (int): maximum number of run records to keep.
    """
    max_age_days: Optional[int] = config.get("max_age_days")
    max_runs: Optional[int] = config.get("max_runs")

    if max_age_days is not None and not isinstance(max_age_days, int):
        raise ValueError(f"max_age_days must be an integer, got {type(max_age_days).__name__}")
    if max_runs is not None and not isinstance(max_runs, int):
        raise ValueError(f"max_runs must be an integer, got {type(max_runs).__name__}")
    if max_age_days is not None and max_age_days <= 0:
        raise ValueError("max_age_days must be a positive integer")
    if max_runs is not None and max_runs <= 0:
        raise ValueError("max_runs must be a positive integer")

    return RetentionPolicy(max_age_days=max_age_days, max_runs=max_runs)


def load_policy_from_file(path: str | Path) -> RetentionPolicy:
    """Load a RetentionPolicy from a JSON file.

    The file should contain a JSON object with optional keys
    ``max_age_days`` and ``max_runs``.
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Retention config file not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == ".json":
        with file_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    else:
        raise ValueError(f"Unsupported config file format: {suffix!r}. Use .json")

    if not isinstance(data, dict):
        raise ValueError("Retention config file must contain a JSON object")

    return build_policy_from_config(data)
