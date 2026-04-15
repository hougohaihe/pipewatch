from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.run_forecaster import RunForecaster, ForecastError, PipelineForecast


def _write_records(path: Path, records: list) -> None:
    with open(path, "w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


@pytest.fixture
def forecaster(log_file: Path) -> RunForecaster:
    return RunForecaster(log_file=str(log_file), window=5)


def test_forecast_returns_none_for_missing_file(forecaster: RunForecaster) -> None:
    assert forecaster.forecast("my_pipe") is None


def test_forecast_returns_none_for_unknown_pipeline(log_file: Path, forecaster: RunForecaster) -> None:
    _write_records(log_file, [{"pipeline": "other", "status": "success", "duration_seconds": 1.0}])
    assert forecaster.forecast("my_pipe") is None


def test_forecast_returns_pipeline_forecast_instance(log_file: Path, forecaster: RunForecaster) -> None:
    records = [{"pipeline": "pipe_a", "status": "success", "duration_seconds": float(i)} for i in range(1, 6)]
    _write_records(log_file, records)
    result = forecaster.forecast("pipe_a")
    assert isinstance(result, PipelineForecast)


def test_forecast_sample_size_respects_window(log_file: Path) -> None:
    records = [{"pipeline": "p", "status": "success", "duration_seconds": 1.0} for _ in range(20)]
    _write_records(log_file, records)
    fc = RunForecaster(str(log_file), window=5).forecast("p")
    assert fc.sample_size == 5


def test_forecast_success_rate_all_success(log_file: Path, forecaster: RunForecaster) -> None:
    records = [{"pipeline": "p", "status": "success", "duration_seconds": 2.0} for _ in range(5)]
    _write_records(log_file, records)
    fc = forecaster.forecast("p")
    assert fc.predicted_success_rate == 1.0


def test_forecast_success_rate_all_failure(log_file: Path, forecaster: RunForecaster) -> None:
    records = [{"pipeline": "p", "status": "failure", "duration_seconds": 2.0} for _ in range(5)]
    _write_records(log_file, records)
    fc = forecaster.forecast("p")
    assert fc.predicted_success_rate == 0.0


def test_forecast_trend_stable(log_file: Path, forecaster: RunForecaster) -> None:
    records = [{"pipeline": "p", "status": "success", "duration_seconds": 5.0} for _ in range(5)]
    _write_records(log_file, records)
    fc = forecaster.forecast("p")
    assert fc.trend == "stable"


def test_forecast_trend_degrading(log_file: Path) -> None:
    durations = [1.0, 1.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0]
    records = [{"pipeline": "p", "status": "success", "duration_seconds": d} for d in durations]
    _write_records(log_file, records)
    fc = RunForecaster(str(log_file), window=8).forecast("p")
    assert fc.trend == "degrading"


def test_forecast_all_returns_all_pipelines(log_file: Path, forecaster: RunForecaster) -> None:
    records = [
        {"pipeline": "alpha", "status": "success", "duration_seconds": 1.0},
        {"pipeline": "beta", "status": "failure", "duration_seconds": 2.0},
    ]
    _write_records(log_file, records)
    results = forecaster.forecast_all()
    names = {fc.pipeline for fc in results}
    assert names == {"alpha", "beta"}


def test_forecast_all_empty_file(log_file: Path, forecaster: RunForecaster) -> None:
    log_file.write_text("")
    assert forecaster.forecast_all() == []


def test_invalid_window_raises() -> None:
    with pytest.raises(ForecastError, match="window"):
        RunForecaster(log_file="/tmp/x.jsonl", window=1)


def test_invalid_log_file_raises() -> None:
    with pytest.raises(ForecastError):
        RunForecaster(log_file="", window=5)


def test_to_dict_contains_expected_keys(log_file: Path, forecaster: RunForecaster) -> None:
    records = [{"pipeline": "p", "status": "success", "duration_seconds": 3.0} for _ in range(3)]
    _write_records(log_file, records)
    fc = forecaster.forecast("p")
    d = fc.to_dict()
    assert set(d.keys()) == {"pipeline", "sample_size", "avg_duration_seconds",
                              "predicted_duration_seconds", "predicted_success_rate", "trend"}
