from __future__ import annotations

import json
from pathlib import Path

from pipewatch.run_heatmap import RunHeatmap


def build_heatmap_from_config(config: dict) -> RunHeatmap:
    """Build a RunHeatmap instance from a configuration dictionary."""
    log_file = config.get("log_file")
    if not log_file:
        raise ValueError("'log_file' is required in heatmap config.")
    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string.")
    log_file = log_file.strip()
    if not log_file:
        raise ValueError("'log_file' must not be empty.")
    return RunHeatmap(log_file=str(Path(log_file).expanduser()))


def load_heatmap_from_file(config_path: str) -> RunHeatmap:
    """Load heatmap config from a JSON file and return a RunHeatmap."""
    path = Path(config_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open() as fh:
        config = json.load(fh)
    return build_heatmap_from_config(config.get("heatmap", config))
