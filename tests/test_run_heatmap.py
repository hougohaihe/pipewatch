from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_heatmap import HeatmapBucket, HeatmapError, RunHeatmap


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.jsonl"


@pytest.fixture
def heatmap(log_file):
    return RunHeatmap(log_file=str(log_file))


# --- HeatmapBucket ---

def test_bucket_error_rate_zero_when_no_runs():
    b = HeatmapBucket(label="2024-01-01")
    assert b.error_rate == 0.0


def test_bucket_error_rate_calculated_correctly():
    b = HeatmapBucket(label="2024-01-01", total=4, success=3, failure=1)
    assert b.error_rate == 0.25


def test_bucket_to_dict_has_expected_keys():
    b = HeatmapBucket(label="2024-01-01", total=2, success=2, failure=0)
    d = b.to_dict()
    assert set(d.keys()) == {"label", "total", "success", "failure", "error_rate"}


# --- RunHeatmap.build ---

def test_build_returns_empty_for_missing_file(heatmap):
    result = heatmap.build(granularity="day")
    assert result == []


def test_build_raises_for_invalid_granularity(heatmap):
    with pytest.raises(HeatmapError, match="Invalid granularity"):
        heatmap.build(granularity="month")


def test_build_groups_by_day(log_file, heatmap):
    records = [
        {"pipeline": "etl", "start_time": "2024-03-01T10:00:00", "status": "success"},
        {"pipeline": "etl", "start_time": "2024-03-01T14:00:00", "status": "failure"},
        {"pipeline": "etl", "start_time": "2024-03-02T09:00:00", "status": "success"},
    ]
    _write_records(log_file, records)
    result = heatmap.build(granularity="day")
    assert len(result) == 2
    day1 = next(b for b in result if b.label == "2024-03-01")
    assert day1.total == 2
    assert day1.success == 1
    assert day1.failure == 1


def test_build_groups_by_hour(log_file, heatmap):
    records = [
        {"pipeline": "p", "start_time": "2024-03-01T10:00:00", "status": "success"},
        {"pipeline": "p", "start_time": "2024-03-01T10:30:00", "status": "success"},
        {"pipeline": "p", "start_time": "2024-03-01T11:00:00", "status": "failure"},
    ]
    _write_records(log_file, records)
    result = heatmap.build(granularity="hour")
    assert len(result) == 2
    h10 = next(b for b in result if b.label == "2024-03-01T10")
    assert h10.total == 2


def test_build_filters_by_pipeline(log_file, heatmap):
    records = [
        {"pipeline": "etl", "start_time": "2024-03-01T10:00:00", "status": "success"},
        {"pipeline": "other", "start_time": "2024-03-01T10:00:00", "status": "failure"},
    ]
    _write_records(log_file, records)
    result = heatmap.build(granularity="day", pipeline="etl")
    assert len(result) == 1
    assert result[0].success == 1
    assert result[0].failure == 0


def test_build_groups_by_weekday(log_file, heatmap):
    records = [
        {"pipeline": "p", "start_time": "2024-03-04T10:00:00", "status": "success"},  # Monday
        {"pipeline": "p", "start_time": "2024-03-05T10:00:00", "status": "failure"},  # Tuesday
    ]
    _write_records(log_file, records)
    result = heatmap.build(granularity="weekday")
    labels = {b.label for b in result}
    assert "Monday" in labels
    assert "Tuesday" in labels


def test_build_skips_records_with_missing_start_time(log_file, heatmap):
    records = [
        {"pipeline": "p", "status": "success"},
        {"pipeline": "p", "start_time": "2024-03-01T10:00:00", "status": "success"},
    ]
    _write_records(log_file, records)
    result = heatmap.build(granularity="day")
    assert len(result) == 1
    assert result[0].total == 1


def test_build_result_is_sorted_by_label(log_file, heatmap):
    records = [
        {"pipeline": "p", "start_time": "2024-03-03T10:00:00", "status": "success"},
        {"pipeline": "p", "start_time": "2024-03-01T10:00:00", "status": "success"},
        {"pipeline": "p", "start_time": "2024-03-02T10:00:00", "status": "success"},
    ]
    _write_records(log_file, records)
    result = heatmap.build(granularity="day")
    labels = [b.label for b in result]
    assert labels == sorted(labels)
