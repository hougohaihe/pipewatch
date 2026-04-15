from __future__ import annotations

import os
from typing import Any, Dict

from pipewatch.run_scorer import RunScorer


def build_scorer_from_config(config: Dict[str, Any]) -> RunScorer:
    """Construct a RunScorer from a configuration dictionary."""
    log_file = config.get("log_file")
    if not log_file:
        raise ValueError("scorer config must include 'log_file'")
    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")
    if not log_file.strip():
        raise ValueError("'log_file' must not be blank")

    log_file = os.path.expanduser(log_file)

    kwargs: Dict[str, Any] = {}
    if "success_weight" in config:
        w = config["success_weight"]
        if not isinstance(w, (int, float)) or not (0.0 <= w <= 1.0):
            raise ValueError("'success_weight' must be a float between 0 and 1")
        kwargs["success_weight"] = float(w)

    if "duration_weight" in config:
        w = config["duration_weight"]
        if not isinstance(w, (int, float)) or not (0.0 <= w <= 1.0):
            raise ValueError("'duration_weight' must be a float between 0 and 1")
        kwargs["duration_weight"] = float(w)

    if "max_expected_duration" in config:
        d = config["max_expected_duration"]
        if not isinstance(d, (int, float)) or d <= 0:
            raise ValueError("'max_expected_duration' must be a positive number")
        kwargs["max_expected_duration"] = float(d)

    return RunScorer(log_file=log_file, **kwargs)


def load_scorer_from_file(path: str) -> RunScorer:
    """Load scorer config from a JSON or YAML file and return a RunScorer."""
    import json
    from pathlib import Path

    p = Path(os.path.expanduser(path))
    if not p.exists():
        raise FileNotFoundError(f"scorer config file not found: {p}")

    with p.open() as fh:
        config = json.load(fh)

    return build_scorer_from_config(config)
