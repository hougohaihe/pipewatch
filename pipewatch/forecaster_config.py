from __future__ import annotations

import json
from pathlib import Path

from pipewatch.run_forecaster import RunForecaster, ForecastError


def build_forecaster_from_config(config: dict) -> RunForecaster:
    if "log_file" not in config:
        raise ForecastError("config must include 'log_file'")
    log_file = config["log_file"]
    if not isinstance(log_file, str):
        raise ForecastError("'log_file' must be a string")
    if not log_file.strip():
        raise ForecastError("'log_file' must not be empty")
    log_file = str(Path(log_file).expanduser())
    window = config.get("window", 10)
    if not isinstance(window, int) or window < 2:
        raise ForecastError("'window' must be an integer >= 2")
    return RunForecaster(log_file=log_file, window=window)


def load_forecaster_from_file(path: str) -> RunForecaster:
    p = Path(path).expanduser()
    if not p.exists():
        raise ForecastError(f"Config file not found: {path}")
    with open(p) as fh:
        config = json.load(fh)
    return build_forecaster_from_config(config)
