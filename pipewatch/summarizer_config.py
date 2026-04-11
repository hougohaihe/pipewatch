from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from pipewatch.run_summarizer import RunSummarizer


def build_summarizer_from_config(config: Dict[str, Any]) -> RunSummarizer:
    """Build a RunSummarizer from a config dictionary."""
    if "log_file" not in config:
        raise KeyError("'log_file' is required in summarizer config")
    log_file = config["log_file"]
    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")
    if not log_file.strip():
        raise ValueError("'log_file' must not be empty")
    return RunSummarizer(log_file=str(Path(log_file).expanduser()))


def load_summarizer_from_file(config_path: str) -> RunSummarizer:
    """Load a RunSummarizer from a JSON config file."""
    import json

    path = Path(config_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open() as fh:
        config = json.load(fh)
    return build_summarizer_from_config(config.get("summarizer", config))
