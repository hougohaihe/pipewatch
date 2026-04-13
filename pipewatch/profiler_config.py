from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_profiler import RunProfiler


def build_profiler_from_config(config: Dict[str, Any]) -> RunProfiler:
    """Build a RunProfiler from a plain config dict."""
    if "log_file" not in config:
        raise KeyError("profiler config missing required key: 'log_file'")
    log_file = config["log_file"]
    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")
    if not log_file.strip():
        raise ValueError("'log_file' must not be empty")
    return RunProfiler(log_file=os.path.expanduser(log_file))


def load_profiler_from_file(config_path: str) -> RunProfiler:
    """Load a RunProfiler from a JSON or YAML config file."""
    import json

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with path.open() as fh:
        config = json.load(fh)
    return build_profiler_from_config(config)
