"""Config helpers for RunReplay."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_replay import RunReplay


def build_replay_from_config(config: Dict[str, Any]) -> RunReplay:
    """Construct a :class:`RunReplay` from a config dict.

    Expected keys
    -------------
    log_file : str  (required)
        Path to the JSONL run log.
    """
    if "log_file" not in config:
        raise KeyError("'log_file' is required in replay config")

    log_file = config["log_file"]
    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")
    if not log_file.strip():
        raise ValueError("'log_file' must not be empty")

    return RunReplay(log_file=Path(log_file).expanduser())


def load_replay_from_file(config_path: str) -> RunReplay:
    """Load replay config from a JSON file and return a :class:`RunReplay`."""
    path = Path(config_path).expanduser()
    with path.open() as fh:
        config = json.load(fh)
    return build_replay_from_config(config)
