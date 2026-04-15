from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.run_forecaster import RunForecaster, ForecastError
from pipewatch.forecaster_config import build_forecaster_from_config, load_forecaster_from_file


def test_build_returns_run_forecaster_instance(tmp_path: Path) -> None:
    cfg = {"log_file": str(tmp_path / "runs.jsonl")}
    result = build_forecaster_from_config(cfg)
    assert isinstance(result, RunForecaster)


def test_build_expands_user() -> None:
    cfg = {"log_file": "~/runs.jsonl"}
    result = build_forecaster_from_config(cfg)
    assert isinstance(result, RunForecaster)


def test_build_missing_log_file_raises() -> None:
    with pytest.raises(ForecastError, match="log_file"):
        build_forecaster_from_config({})


def test_build_non_string_log_file_raises() -> None:
    with pytest.raises(ForecastError, match="string"):
        build_forecaster_from_config({"log_file": 42})


def test_build_empty_log_file_raises() -> None:
    with pytest.raises(ForecastError):
        build_forecaster_from_config({"log_file": "   "})


def test_build_custom_window(tmp_path: Path) -> None:
    cfg = {"log_file": str(tmp_path / "runs.jsonl"), "window": 7}
    result = build_forecaster_from_config(cfg)
    assert isinstance(result, RunForecaster)


def test_build_invalid_window_raises(tmp_path: Path) -> None:
    cfg = {"log_file": str(tmp_path / "runs.jsonl"), "window": 1}
    with pytest.raises(ForecastError, match="window"):
        build_forecaster_from_config(cfg)


def test_load_from_file(tmp_path: Path) -> None:
    log = tmp_path / "runs.jsonl"
    cfg_path = tmp_path / "forecaster.json"
    cfg_path.write_text(json.dumps({"log_file": str(log), "window": 5}))
    result = load_forecaster_from_file(str(cfg_path))
    assert isinstance(result, RunForecaster)


def test_load_from_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ForecastError, match="not found"):
        load_forecaster_from_file(str(tmp_path / "missing.json"))
