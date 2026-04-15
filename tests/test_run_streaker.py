"""Tests for pipewatch.run_streaker."""

import json
from pathlib import Path

import pytest

from pipewatch.run_streaker import PipelineStreak, RunStreaker, StreakerError


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def streaker(log_file):
    return RunStreaker(str(log_file))


def test_compute_returns_empty_for_missing_file(streaker):
    result = streaker.compute()
    assert result == {}


def test_compute_returns_pipeline_streak_instance(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
    ])
    result = streaker.compute()
    assert isinstance(result["etl"], PipelineStreak)


def test_current_streak_all_success(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "etl", "status": "success"},
    ])
    streak = streaker.compute()["etl"]
    assert streak.current_streak == 3
    assert streak.streak_type == "success"


def test_current_streak_ends_on_failure(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "etl", "status": "failure"},
    ])
    streak = streaker.compute()["etl"]
    assert streak.current_streak == 1
    assert streak.streak_type == "failure"


def test_longest_success_streak_tracked(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "etl", "status": "failure"},
        {"pipeline": "etl", "status": "success"},
    ])
    streak = streaker.compute()["etl"]
    assert streak.longest_success_streak == 2


def test_longest_failure_streak_tracked(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "failure"},
        {"pipeline": "etl", "status": "failure"},
        {"pipeline": "etl", "status": "failure"},
        {"pipeline": "etl", "status": "success"},
    ])
    streak = streaker.compute()["etl"]
    assert streak.longest_failure_streak == 3


def test_compute_filters_by_pipeline(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "ingest", "status": "failure"},
    ])
    result = streaker.compute(pipeline="etl")
    assert "etl" in result
    assert "ingest" not in result


def test_multiple_pipelines_independent(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "etl", "status": "success"},
        {"pipeline": "ingest", "status": "failure"},
    ])
    result = streaker.compute()
    assert result["etl"].current_streak == 2
    assert result["ingest"].current_streak == 1
    assert result["ingest"].streak_type == "failure"


def test_to_dict_contains_expected_keys(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
    ])
    d = streaker.compute()["etl"].to_dict()
    assert set(d.keys()) == {
        "pipeline", "current_streak", "streak_type",
        "longest_success_streak", "longest_failure_streak",
    }


def test_streak_type_none_for_unknown_status(log_file, streaker):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "unknown"},
    ])
    streak = streaker.compute()["etl"]
    assert streak.streak_type == "none"
    assert streak.current_streak == 0
