"""Tests for pipewatch.run_grouper."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_grouper import GroupError, RunGrouper


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.log"


@pytest.fixture()
def grouper(log_file: Path) -> RunGrouper:
    records = [
        {"run_id": "a1", "pipeline": "etl", "status": "success"},
        {"run_id": "a2", "pipeline": "etl", "status": "failure"},
        {"run_id": "a3", "pipeline": "ingest", "status": "success"},
        {"run_id": "a4", "pipeline": "ingest", "status": "success"},
        {"run_id": "a5", "pipeline": "report", "status": "failure"},
    ]
    _write_records(log_file, records)
    return RunGrouper(str(log_file))


def test_group_by_pipeline_returns_correct_keys(grouper: RunGrouper) -> None:
    groups = grouper.group_by("pipeline")
    assert set(groups.keys()) == {"etl", "ingest", "report"}


def test_group_by_pipeline_correct_counts(grouper: RunGrouper) -> None:
    groups = grouper.group_by("pipeline")
    assert len(groups["etl"]) == 2
    assert len(groups["ingest"]) == 2
    assert len(groups["report"]) == 1


def test_group_by_status_buckets(grouper: RunGrouper) -> None:
    groups = grouper.group_by("status")
    assert len(groups["success"]) == 3
    assert len(groups["failure"]) == 2


def test_group_by_invalid_field_raises(grouper: RunGrouper) -> None:
    with pytest.raises(GroupError, match="Cannot group by"):
        grouper.group_by("nonexistent_field")


def test_group_counts_returns_integers(grouper: RunGrouper) -> None:
    counts = grouper.group_counts("pipeline")
    assert all(isinstance(v, int) for v in counts.values())
    assert counts["etl"] == 2


def test_largest_group_returns_correct_key(grouper: RunGrouper) -> None:
    # "success" has 3 records vs "failure" with 2
    assert grouper.largest_group("status") == "success"


def test_group_by_missing_file_returns_empty(tmp_path: Path) -> None:
    g = RunGrouper(str(tmp_path / "missing.log"))
    assert g.group_by("pipeline") == {}


def test_largest_group_missing_file_returns_none(tmp_path: Path) -> None:
    g = RunGrouper(str(tmp_path / "missing.log"))
    assert g.largest_group("pipeline") is None


def test_group_by_skips_malformed_lines(log_file: Path) -> None:
    log_file.write_text(
        '{"pipeline": "etl", "status": "success"}\nNOT_JSON\n'
        '{"pipeline": "etl", "status": "failure"}\n'
    )
    g = RunGrouper(str(log_file))
    groups = g.group_by("pipeline")
    assert len(groups["etl"]) == 2
