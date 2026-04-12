"""Build a RunWatcher from a plain-dict config or a YAML/JSON file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_watcher import RunWatcher, WatcherError


def build_watcher_from_config(config: Dict[str, Any]) -> RunWatcher:
    """Construct a :class:`RunWatcher` from a configuration dictionary.

    Required keys
    -------------
    log_file : str
        Path to the JSONL run log.

    Optional keys
    -------------
    poll_interval : float
        Seconds between polls (default 1.0).
    """
    log_file = config.get("log_file")
    if not log_file:
        raise WatcherError("config must include a non-empty 'log_file'")
    if not isinstance(log_file, str):
        raise WatcherError("'log_file' must be a string")

    poll_interval = config.get("poll_interval", 1.0)
    try:
        poll_interval = float(poll_interval)
    except (TypeError, ValueError) as exc:
        raise WatcherError("'poll_interval' must be a number") from exc
    if poll_interval <= 0:
        raise WatcherError("'poll_interval' must be positive")

    return RunWatcher(log_file=log_file, poll_interval=poll_interval)


def load_watcher_from_file(path: str) -> RunWatcher:
    """Load watcher config from a JSON file and return a :class:`RunWatcher`."""
    config_path = Path(path).expanduser()
    if not config_path.exists():
        raise WatcherError(f"config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        config = json.load(fh)
    return build_watcher_from_config(config)
