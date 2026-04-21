import json
import pytest
from datetime import datetime, timedelta
from pathlib import Path

from pipewatch.run_correlator import CorrelationError, PipelineCorrelation, RunCorrelator


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def correlator(log_file):
    return RunCorrelator(str(log_file))


def _ts(offset_seconds: float = 0) -> str:
    base = datetime(2024, 1, 15, 12, 0, 0)
    return (base + timedelta(seconds=offset_seconds)).isoformat()


def test_correlate_returns_empty_for_missing_file(correlator):
    result = correlator.correlate()
    assert result == []


def test_correlate_returns_empty_for_single_pipeline(log_file, correlator):
    records = [
        {"pipeline": "etl", "status": "success", "start_time": _ts(0)},
        {"pipeline": "etl", "status": "failed", "start_time": _ts(30)},
    ]
    _write_records(log_file, records)
    result = correlator.correlate(window_seconds=60)
    assert result == []


def test_correlate_returns_pipeline_correlation_instances(log_file, correlator):
    records = [
        {"pipeline": "etl", "status": "failed", "start_time": _ts(0)},
        {"pipeline": "loader", "status": "failed", "start_time": _ts(10)},
    ]
    _write_records(log_file, records)
    result = correlator.correlate(window_seconds=60)
    assert len(result) == 1
    assert isinstance(result[0], PipelineCorrelation)


def test_correlate_identifies_co_failures(log_file, correlator):
    records = [
        {"pipeline": "etl", "status": "failed", "start_time": _ts(0)},
        {"pipeline": "loader", "status": "failed", "start_time": _ts(5)},
    ]
    _write_records(log_file, records)
    result = correlator.correlate(window_seconds=60)
    assert result[0].co_failure_count == 1
    assert result[0].co_success_count == 0


def test_correlate_identifies_co_successes(log_file, correlator):
    records = [
        {"pipeline": "etl", "status": "success", "start_time": _ts(0)},
        {"pipeline": "loader", "status": "success", "start_time": _ts(5)},
    ]
    _write_records(log_file, records)
    result = correlator.correlate(window_seconds=60)
    assert result[0].co_success_count == 1
    assert result[0].co_failure_count == 0


def test_correlate_pipeline_pair_sorted_alphabetically(log_file, correlator):
    records = [
        {"pipeline": "zzz", "status": "failed", "start_time": _ts(0)},
        {"pipeline": "aaa", "status": "failed", "start_time": _ts(5)},
    ]
    _write_records(log_file, records)
    result = correlator.correlate(window_seconds=60)
    assert result[0].pipeline_a == "aaa"
    assert result[0].pipeline_b == "zzz"


def test_correlate_excludes_pairs_outside_window(log_file, correlator):
    records = [
        {"pipeline": "etl", "status": "failed", "start_time": _ts(0)},
        {"pipeline": "loader", "status": "failed", "start_time": _ts(200)},
    ]
    _write_records(log_file, records)
    result = correlator.correlate(window_seconds=60)
    assert result == []


def test_co_failure_rate_calculated_correctly(log_file, correlator):
    records = [
        {"pipeline": "etl", "status": "failed", "start_time": _ts(0)},
        {"pipeline": "loader", "status": "failed", "start_time": _ts(5)},
        {"pipeline": "etl", "status": "success", "start_time": _ts(120)},
        {"pipeline": "loader", "status": "failed", "start_time": _ts(125)},
    ]
    _write_records(log_file, records)
    result = correlator.correlate(window_seconds=60)
    assert len(result) == 1
    corr = result[0]
    assert corr.shared_run_count == 2
    assert corr.co_failure_count == 1
    assert corr.co_failure_rate == 0.5


def test_to_dict_contains_expected_keys(log_file, correlator):
    records = [
        {"pipeline": "etl", "status": "failed", "start_time": _ts(0)},
        {"pipeline": "loader", "status": "failed", "start_time": _ts(5)},
    ]
    _write_records(log_file, records)
    result = correlator.correlate(window_seconds=60)
    d = result[0].to_dict()
    assert "pipeline_a" in d
    assert "pipeline_b" in d
    assert "co_failure_rate" in d
    assert "shared_run_count" in d
