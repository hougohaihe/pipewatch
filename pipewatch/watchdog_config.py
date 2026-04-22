from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_watchdog import RunWatchdog, WatchdogError


def build_watchdog_from_config(config: Dict[str, Any]) -> RunWatchdog:
    """Build a RunWatchdog from a config dictionary."""
    log_file = config.get("log_file")
    if not log_file:
        raise WatchdogError("config must include 'log_file'")
    if not isinstance(log_file, str):
        raise WatchdogError("'log_file' must be a string")
    if not log_file.strip():
        raise WatchdogError("'log_file' must not be empty")

    log_file = os.path.expanduser(log_file)

    default_threshold = config.get("default_threshold_seconds", 300.0)
    if not isinstance(default_threshold, (int, float)) or default_threshold <= 0:
        raise WatchdogError("'default_threshold_seconds' must be a positive number")

    return RunWatchdog(
        log_file=log_file,
        default_threshold_seconds=float(default_threshold),
    )


def load_watchdog_from_file(config_path: str) -> RunWatchdog:
    """Load a RunWatchdog from a JSON config file."""
    path = Path(os.path.expanduser(config_path))
    if not path.exists():
        raise WatchdogError(f"Config file not found: {config_path}")
    with path.open() as fh:
        config = json.load(fh)
    return build_watchdog_from_config(config)
