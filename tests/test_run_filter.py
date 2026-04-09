"""Tests for pipewatch.run_filter.RunFilter."""

import json
import pytest
from pathlib import Path

from pipewatch.run_filter import RunFilter


SAMPLE_RECORDS = [
    {"run_id": "aaa", "pipeline": "etl", "status": "success", "ts": "2024-01-01T00:00:00"},
    {"run_id": "bbb", "pipeline": "etl", "status": "failure", "ts": "2024-01-02T00:00:00"},
    {"run_id": "ccc", "pipeline": "ingest", "status": "success", "ts": "2024-01-03T00:00:00"},
    {"run_id": "ddd", "pipeline": "ingest", "status": "failure", "ts": "2024-01-04T00:00:00"},
    {"run_id": "eee", "pipeline": "etl", "status": "success", "ts": "2024-01-05T00:00:00"},
]


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    path = tmp_path / "runs.log"
    with path.open("w") as fh:
        for rec in SAMPLE_RECORDS:
            fh.write(json.dumps(rec) + "\n")
    return path


@pytest.fixture
def rf(log_file: Path) -> RunFilter:
    return RunFilter(log_file)


def test_all_returns_all_records(rf: RunFilter) -> None:
    assert len(rf.all()) == 5


def test_all_returns_empty_for_missing_file(tmp_path: Path) -> None:
    rf = RunFilter(tmp_path / "nonexistent.log")
    assert rf.all() == []


def test_by_pipeline_filters_correctly(rf: RunFilter) -> None:
    results = rf.by_pipeline("etl")
    assert len(results) == 3
    assert all(r["pipeline"] == "etl" for r in results)


def test_by_pipeline_returns_empty_for_unknown(rf: RunFilter) -> None:
    assert rf.by_pipeline("unknown") == []


def test_by_status_success(rf: RunFilter) -> None:
    results = rf.by_status("success")
    assert len(results) == 3
    assert all(r["status"] == "success" for r in results)


def test_by_status_failure(rf: RunFilter) -> None:
    results = rf.by_status("failure")
    assert len(results) == 2


def test_by_run_id_found(rf: RunFilter) -> None:
    rec = rf.by_run_id("ccc")
    assert rec is not None
    assert rec["pipeline"] == "ingest"


def test_by_run_id_not_found(rf: RunFilter) -> None:
    assert rf.by_run_id("zzz") is None


def test_where_custom_predicate(rf: RunFilter) -> None:
    results = rf.where(lambda r: r["pipeline"] == "ingest" and r["status"] == "success")
    assert len(results) == 1
    assert results[0]["run_id"] == "ccc"


def test_latest_returns_last_n(rf: RunFilter) -> None:
    results = rf.latest(3)
    assert len(results) == 3
    assert results[-1]["run_id"] == "eee"


def test_latest_returns_all_when_fewer_than_n(rf: RunFilter) -> None:
    results = rf.latest(100)
    assert len(results) == 5


def test_skips_malformed_lines(tmp_path: Path) -> None:
    path = tmp_path / "bad.log"
    path.write_text('{"run_id": "x", "pipeline": "p", "status": "success"}\nNOT_JSON\n')
    rf = RunFilter(path)
    assert len(rf.all()) == 1
