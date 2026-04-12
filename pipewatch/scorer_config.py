"""Config loader for RunScorer."""

from __future__ import annotations

import os
from typing import Any, Dict

from pipewatch.run_scorer import RunScorer


def build_scorer_from_config(config: Dict[str, Any]) -> RunScorer:
    """Build a RunScorer from a config dictionary.

    Expected keys:
        log_file (str): Path to the JSONL run log.
    """
    if "log_file" not in config:
        raise KeyError("scorer config missing required field: 'log_file'")
    log_file = config["log_file"]
    if not isinstance(log_file, str):
        raise TypeError("scorer config 'log_file' must be a string")
    if not log_file.strip():
        raise ValueError("scorer config 'log_file' must not be empty")
    return RunScorer(log_file=os.path.expanduser(log_file))


def load_scorer_from_file(path: str) -> RunScorer:
    """Load a RunScorer from a YAML or JSON config file."""
    import json
    from pathlib import Path

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"scorer config file not found: {path}")
    suffix = p.suffix.lower()
    if suffix in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise ImportError("PyYAML is required to load YAML config files") from exc
        with p.open() as fh:
            config = yaml.safe_load(fh) or {}
    elif suffix == ".json":
        with p.open() as fh:
            config = json.load(fh)
    else:
        raise ValueError(f"unsupported config file format: {suffix}")
    return build_scorer_from_config(config)
