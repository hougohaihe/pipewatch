from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pipewatch.run_ranker import RunRanker


def build_ranker_from_config(config: dict[str, Any]) -> RunRanker:
    """Construct a RunRanker from a configuration dictionary."""
    if "log_file" not in config:
        raise KeyError("ranker config missing required key: 'log_file'")
    log_file = config["log_file"]
    if not isinstance(log_file, str):
        raise TypeError("ranker config 'log_file' must be a string")
    if not log_file.strip():
        raise ValueError("ranker config 'log_file' must not be empty")
    return RunRanker(log_file=os.path.expanduser(log_file))


def load_ranker_from_file(config_path: str) -> RunRanker:
    """Load ranker configuration from a JSON file and return a RunRanker."""
    path = Path(config_path)
    with path.open() as fh:
        config = json.load(fh)
    return build_ranker_from_config(config)
