"""Config loader for RunComparator."""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_comparator import RunComparator


def build_comparator_from_config(config: Dict[str, Any]) -> RunComparator:
    """Build a RunComparator from a config dictionary.

    Expected keys:
        log_file (str): Path to the JSONL run log file.
    """
    if "log_file" not in config:
        raise KeyError("'log_file' is required in comparator config")

    log_file = config["log_file"]

    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")

    if not log_file.strip():
        raise ValueError("'log_file' must not be empty")

    return RunComparator(log_file=os.path.expanduser(log_file))


def load_comparator_from_file(config_path: str) -> RunComparator:
    """Load a RunComparator from a JSON config file."""
    path = Path(config_path)
    with path.open() as fh:
        config = json.load(fh)
    return build_comparator_from_config(config)
