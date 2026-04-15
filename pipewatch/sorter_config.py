from __future__ import annotations

import json
from pathlib import Path

from pipewatch.run_sorter import RunSorter


def build_sorter_from_config(config: dict) -> RunSorter:
    """Build a RunSorter from a configuration dictionary."""
    if "log_file" not in config:
        raise KeyError("sorter config must include 'log_file'")

    log_file = config["log_file"]

    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")

    if not log_file.strip():
        raise ValueError("'log_file' must not be empty")

    return RunSorter(log_file=str(Path(log_file).expanduser()))


def load_sorter_from_file(config_path: str) -> RunSorter:
    """Load a RunSorter from a JSON config file."""
    path = Path(config_path).expanduser()
    with path.open() as fh:
        config = json.load(fh)
    return build_sorter_from_config(config.get("sorter", config))
