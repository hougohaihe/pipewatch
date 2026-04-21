import json
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pipewatch.run_cadence import CadenceError, PipelineCadence, RunCadence


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _ts(offset_hours: float = 0.0) -> str:
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(hours=offset_hours)).isoformat()


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


@pytest.fixture
def cadence(log_file: Path) -> RunCadence:
    return RunCadence(str(log_file))


# --- construction ---

def test_missing_log_file_raises():
    with pytest.raises(CadenceError):
        RunCadence("")


def test_non_string_log_file_raises():
    with pytest.raises(CadenceError):
        RunCadence(None)  # type: ignore


def test_min_runs_too_low_raises(log_file: Path):
    with pytest.raises(CadenceError):
        RunCadence(str(log_file), min_runs=1)


# --- compute ---

def test_compute_returns_empty_for_missing_file(cadence: RunCadence):
    result = cadence.compute()
    assert result == {}


def test_compute_returns_pipeline_cadence_instances(log_file: Path, cadence: RunCadence):
    records = [
        {"pipeline": "etl", "start_time": _ts(0)},
        {"pipeline": "etl", "start_time": _ts(1)},
        {"pipeline": "etl", "start_time": _ts(2)},
    ]
    _write_records(log_file, records)
    result = cadence.compute()
    assert "etl" in result
    assert isinstance(result["etl"], PipelineCadence)


def test_compute_correct_run_count(log_file: Path, cadence: RunCadence):
    records = [
        {"pipeline": "etl", "start_time": _ts(i)}
        for i in range(5)
    ]
    _write_records(log_file, records)
    result = cadence.compute()
    assert result["etl"].run_count == 5


def test_compute_avg_interval_seconds(log_file: Path, cadence: RunCadence):
    records = [
        {"pipeline": "etl", "start_time": _ts(i)}
        for i in range(4)
    ]
    _write_records(log_file, records)
    result = cadence.compute()
    assert result["etl"].avg_interval_seconds == pytest.approx(3600.0, rel=1e-3)


def test_compute_is_regular_for_uniform_cadence(log_file: Path, cadence: RunCadence):
    records = [
        {"pipeline": "etl", "start_time": _ts(i)}
        for i in range(6)
    ]
    _write_records(log_file, records)
    result = cadence.compute()
    assert result["etl"].is_regular is True


def test_compute_not_regular_for_irregular_cadence(log_file: Path, cadence: RunCadence):
    offsets = [0, 0.1, 5, 5.1, 20, 20.1]
    records = [
        {"pipeline": "etl", "start_time": _ts(o)}
        for o in offsets
    ]
    _write_records(log_file, records)
    result = cadence.compute()
    assert result["etl"].is_regular is False


def test_compute_none_intervals_below_min_runs(log_file: Path, cadence: RunCadence):
    records = [
        {"pipeline": "etl", "start_time": _ts(0)},
        {"pipeline": "etl", "start_time": _ts(1)},
    ]
    _write_records(log_file, records)
    result = cadence.compute()
    assert result["etl"].avg_interval_seconds is None
    assert result["etl"].is_regular is False


def test_compute_filters_by_pipeline(log_file: Path, cadence: RunCadence):
    records = [
        {"pipeline": "etl", "start_time": _ts(i)} for i in range(4)
    ] + [
        {"pipeline": "load", "start_time": _ts(i)} for i in range(4)
    ]
    _write_records(log_file, records)
    result = cadence.compute(pipeline="etl")
    assert "etl" in result
    assert "load" not in result


def test_to_dict_contains_expected_keys(log_file: Path, cadence: RunCadence):
    records = [
        {"pipeline": "etl", "start_time": _ts(i)}
        for i in range(4)
    ]
    _write_records(log_file, records)
    result = cadence.compute()
    d = result["etl"].to_dict()
    for key in ("pipeline", "run_count", "avg_interval_seconds",
                "stddev_interval_seconds", "min_interval_seconds",
                "max_interval_seconds", "is_regular"):
        assert key in d
