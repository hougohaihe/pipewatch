from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_sampler import RunSampler


def build_sampler_from_config(config: Dict[str, Any]) -> RunSampler:
    """Build a RunSampler from a configuration dictionary."""
    if "log_file" not in config:
        raise KeyError("'log_file' is required in sampler config")

    log_file = config["log_file"]

    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")

    log_file = log_file.strip()
    if not log_file:
        raise ValueError("'log_file' must not be empty")

    log_file = os.path.expanduser(log_file)
    return RunSampler(log_file=log_file)


def load_sampler_from_file(path: str) -> RunSampler:
    """Load a RunSampler from a JSON or YAML config file."""
    import json

    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with config_path.open() as fh:
        config = json.load(fh)

    return build_sampler_from_config(config)
