"""Config helpers for RunSnapshot."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_snapshot import RunSnapshot


def build_snapshot_from_config(config: Dict[str, Any]) -> RunSnapshot:
    """Build a RunSnapshot from a config dictionary.

    Required keys:
        log_file (str): path to the JSONL run log.
        snapshot_dir (str): directory where snapshots are stored.
    """
    if "log_file" not in config:
        raise KeyError("'log_file' is required in snapshot config.")
    if "snapshot_dir" not in config:
        raise KeyError("'snapshot_dir' is required in snapshot config.")

    log_file = config["log_file"]
    snapshot_dir = config["snapshot_dir"]

    if not isinstance(log_file, str) or not log_file.strip():
        raise ValueError("'log_file' must be a non-empty string.")
    if not isinstance(snapshot_dir, str) or not snapshot_dir.strip():
        raise ValueError("'snapshot_dir' must be a non-empty string.")

    return RunSnapshot(
        log_file=os.path.expanduser(log_file),
        snapshot_dir=os.path.expanduser(snapshot_dir),
    )


def load_snapshot_from_file(config_path: str) -> RunSnapshot:
    """Load snapshot config from a JSON file and return a RunSnapshot."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Snapshot config file not found: {config_path}")
    with path.open() as fh:
        config = json.load(fh)
    return build_snapshot_from_config(config)
