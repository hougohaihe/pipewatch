from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.run_aggregator import AggregatedBucket, AggregationError, RunAggregator


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def aggregator(log_file):
    return RunAggregator(log_file=str(log_file))


def test_aggregate_by_returns_empty_for_missing_file(aggregator):
    result = aggregator.aggregate_by("status")
    assert result == {}


def test_aggregate_by_groups_by_status(log_file, aggregator):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 5.0},
        {"pipeline": "load", "status": "success", "duration_seconds": 8.0},
    ])
    result = aggregator.aggregate_by("status")
    assert "success" in result
    assert "failure" in result
    assert result["success"].run_count == 2
    assert result["failure"].run_count == 1


def test_aggregate_by_counts_success_and_failure(log_file, aggregator):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 2.0},
    ])
    result = aggregator.aggregate_by("pipeline")
    assert result["etl"].success_count == 1
    assert result["etl"].failure_count == 1


def test_aggregate_by_calculates_avg_duration(log_file, aggregator):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 20.0},
    ])
    result = aggregator.aggregate_by("pipeline")
    assert result["etl"].avg_duration == 15.0


def test_aggregate_by_skips_records_without_field(log_file, aggregator):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
        {"status": "success"},  # no pipeline key
    ])
    result = aggregator.aggregate_by("pipeline")
    assert len(result) == 1
    assert "etl" in result


def test_aggregate_by_invalid_field_raises(aggregator):
    with pytest.raises(AggregationError):
        aggregator.aggregate_by("")


def test_aggregate_by_non_string_field_raises(aggregator):
    with pytest.raises(AggregationError):
        aggregator.aggregate_by(None)


def test_summary_returns_sorted_list(log_file, aggregator):
    _write_records(log_file, [
        {"pipeline": "zzz", "status": "success", "duration_seconds": 1.0},
        {"pipeline": "aaa", "status": "failure", "duration_seconds": 2.0},
    ])
    result = aggregator.summary("pipeline")
    assert isinstance(result, list)
    assert result[0]["key"] == "aaa"
    assert result[1]["key"] == "zzz"


def test_summary_to_dict_keys(log_file, aggregator):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 5.0},
    ])
    result = aggregator.summary("pipeline")
    expected_keys = {"key", "run_count", "success_count", "failure_count",
                     "avg_duration_seconds", "success_rate", "pipelines"}
    assert set(result[0].keys()) == expected_keys


def test_success_rate_calculation(log_file, aggregator):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 1.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 1.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 1.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 1.0},
    ])
    result = aggregator.aggregate_by("pipeline")
    assert result["etl"].success_rate == 0.5


def test_avg_duration_none_when_no_runs():
    bucket = AggregatedBucket(key="test")
    assert bucket.avg_duration is None
    assert bucket.success_rate is None
