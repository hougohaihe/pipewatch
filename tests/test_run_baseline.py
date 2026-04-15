import json
import pytest
from pathlib import Path
from pipewatch.run_baseline import RunBaseline, PipelineBaseline


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def baseline(log_file):
    return RunBaseline(str(log_file))


def test_compute_returns_none_for_missing_file(baseline):
    assert baseline.compute("etl") is None


def test_compute_returns_none_for_unknown_pipeline(log_file, baseline):
    _write_records(log_file, [{"pipeline": "other", "status": "success", "duration_seconds": 5.0}])
    assert baseline.compute("etl") is None


def test_compute_returns_pipeline_baseline_instance(log_file, baseline):
    _write_records(log_file, [{"pipeline": "etl", "status": "success", "duration_seconds": 10.0}])
    result = baseline.compute("etl")
    assert isinstance(result, PipelineBaseline)


def test_compute_avg_duration(log_file, baseline):
    records = [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 20.0},
    ]
    _write_records(log_file, records)
    result = baseline.compute("etl")
    assert result.avg_duration_seconds == 15.0


def test_compute_success_rate(log_file, baseline):
    records = [
        {"pipeline": "etl", "status": "success", "duration_seconds": 5.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 3.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 7.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 2.0},
    ]
    _write_records(log_file, records)
    result = baseline.compute("etl")
    assert result.success_rate == 0.5


def test_compute_sample_size(log_file, baseline):
    records = [
        {"pipeline": "etl", "status": "success", "duration_seconds": 4.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 6.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 2.0},
    ]
    _write_records(log_file, records)
    result = baseline.compute("etl")
    assert result.sample_size == 3


def test_compute_avg_duration_none_when_no_durations(log_file, baseline):
    _write_records(log_file, [{"pipeline": "etl", "status": "success"}])
    result = baseline.compute("etl")
    assert result.avg_duration_seconds is None


def test_compute_all_returns_empty_for_missing_file(baseline):
    assert baseline.compute_all() == {}


def test_compute_all_returns_all_pipelines(log_file, baseline):
    records = [
        {"pipeline": "etl", "status": "success", "duration_seconds": 5.0},
        {"pipeline": "load", "status": "failure", "duration_seconds": 1.0},
    ]
    _write_records(log_file, records)
    result = baseline.compute_all()
    assert set(result.keys()) == {"etl", "load"}


def test_to_dict_has_expected_keys(log_file, baseline):
    _write_records(log_file, [{"pipeline": "etl", "status": "success", "duration_seconds": 3.0}])
    result = baseline.compute("etl")
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "avg_duration_seconds", "success_rate", "sample_size", "fields"}
