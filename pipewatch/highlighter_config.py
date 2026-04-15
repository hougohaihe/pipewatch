from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipewatch.run_highlighter import RunHighlighter


def build_highlighter_from_config(config: dict[str, Any]) -> RunHighlighter:
    """Build a RunHighlighter from a configuration dictionary."""
    log_file = config.get("log_file")
    if not log_file:
        raise ValueError("'log_file' is required in highlighter config")
    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")
    if not log_file.strip():
        raise ValueError("'log_file' must not be empty")
    return RunHighlighter(log_file=str(Path(log_file).expanduser()))


def load_highlighter_from_file(config_path: str) -> RunHighlighter:
    """Load a RunHighlighter from a JSON config file."""
    path = Path(config_path).expanduser()
    with path.open() as fh:
        config: dict[str, Any] = json.load(fh)
    return build_highlighter_from_config(config)
