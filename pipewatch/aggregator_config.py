from __future__ import annotations

import json
from pathlib import Path

from pipewatch.run_aggregator import RunAggregator


def build_aggregator_from_config(config: dict) -> RunAggregator:
    """Build a RunAggregator from a configuration dictionary."""
    if "log_file" not in config:
        raise KeyError("aggregator config requires 'log_file'")
    log_file = config["log_file"]
    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")
    if not log_file.strip():
        raise ValueError("'log_file' must not be empty")
    log_file = str(Path(log_file).expanduser())
    return RunAggregator(log_file=log_file)


def load_aggregator_from_file(config_path: str) -> RunAggregator:
    """Load aggregator config from a JSON file and return a RunAggregator."""
    path = Path(config_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with path.open() as fh:
        config = json.load(fh)
    return build_aggregator_from_config(config.get("aggregator", config))
