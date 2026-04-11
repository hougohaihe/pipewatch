"""Tests for pipewatch.run_stats."""

import json
import pytest
from pathlib import Path
from pipewatch.run_stats import RunStats, PipelineStats


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def stats(log_file):
    return RunStats(str(log_file))


_RECORDS = [
    {"run_id": "a1", "pipeline": "etl", "status": "success", "duration_seconds": 10.0},
    {"run_id": "a2", "pipeline": "etl", "status": "success", "duration_seconds": 20.0},
    {"run_id": "a3", "pipeline": "etl", "status": "failure", "duration_seconds": 5.0},
    {"run_id": "b1", "pipeline": "ingest", "status": "success", "duration_seconds": 30.0},
]


def test_compute_returns_pipeline_stats_instances(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute()
    assert isinstance(result["etl"], PipelineStats)
    assert isinstance(result["ingest"], PipelineStats)


def test_compute_run_count(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute()
    assert result["etl"].run_count == 3
    assert result["ingest"].run_count == 1


def test_compute_success_and_failure_counts(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute()
    assert result["etl"].success_count == 2
    assert result["etl"].failure_count == 1


def test_compute_success_rate(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute()
    assert result["etl"].success_rate == pytest.approx(2 / 3, rel=1e-3)


def test_compute_avg_duration(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute()
    assert result["etl"].avg_duration == pytest.approx(35.0 / 3, rel=1e-3)


def test_compute_min_max_duration(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute()
    assert result["etl"].min_duration == 5.0
    assert result["etl"].max_duration == 20.0


def test_compute_p50_duration(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute()
    # sorted durations for etl: [5.0, 10.0, 20.0] — p50 index = 1
    assert result["etl"].p50_duration == 10.0


def test_compute_filter_by_pipeline(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute(pipeline="ingest")
    assert "ingest" in result
    assert "etl" not in result


def test_compute_empty_file_returns_empty(log_file, stats):
    log_file.write_text("")
    result = stats.compute()
    assert result == {}


def test_compute_missing_file_returns_empty(stats):
    result = stats.compute()
    assert result == {}


def test_to_dict_contains_all_keys(log_file, stats):
    _write_records(log_file, _RECORDS)
    result = stats.compute()
    d = result["etl"].to_dict()
    expected_keys = {
        "pipeline", "run_count", "success_count", "failure_count",
        "success_rate", "avg_duration", "min_duration", "max_duration",
        "p50_duration", "p95_duration",
    }
    assert expected_keys == set(d.keys())


def test_none_duration_when_no_duration_field(log_file, stats):
    _write_records(log_file, [{"run_id": "x1", "pipeline": "noop", "status": "success"}])
    result = stats.compute()
    assert result["noop"].avg_duration is None
    assert result["noop"].p95_duration is None
