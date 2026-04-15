from __future__ import annotations

import os
from typing import Any, Dict

from pipewatch.run_anomaly import AnomalyError, RunAnomaly


def build_anomaly_from_config(config: Dict[str, Any]) -> RunAnomaly:
    """Build a RunAnomaly instance from a config dictionary."""
    if not isinstance(config, dict):
        raise AnomalyError("config must be a dictionary")

    log_file = config.get("log_file")
    if log_file is None:
        raise AnomalyError("config missing required key: log_file")
    if not isinstance(log_file, str):
        raise AnomalyError("log_file must be a string")
    if not log_file.strip():
        raise AnomalyError("log_file must not be empty")

    log_file = os.path.expanduser(log_file)

    z_threshold = config.get("z_threshold", 2.0)
    if not isinstance(z_threshold, (int, float)):
        raise AnomalyError("z_threshold must be a number")
    if z_threshold <= 0:
        raise AnomalyError("z_threshold must be positive")

    return RunAnomaly(log_file=log_file, z_threshold=float(z_threshold))


def load_anomaly_from_file(path: str) -> RunAnomaly:
    """Load a RunAnomaly instance from a JSON config file."""
    import json

    path = os.path.expanduser(path)
    with open(path, "r") as fh:
        config = json.load(fh)
    return build_anomaly_from_config(config)
