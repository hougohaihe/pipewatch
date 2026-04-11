"""Tests for pipewatch.run_diff."""

import json
import pytest
from pathlib import Path

from pipewatch.run_diff import FieldDiff, RunDiff, RunDiffResult


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    p = tmp_path / "runs.jsonl"
    _write_records(p, [
        {"run_id": "aaa", "pipeline": "etl", "status": "success", "duration_seconds": 10.0, "start_time": "t1", "end_time": "t2"},
        {"run_id": "bbb", "pipeline": "etl", "status": "failure", "duration_seconds": 5.5, "start_time": "t3", "end_time": "t4"},
        {"run_id": "ccc", "pipeline": "load", "status": "success", "duration_seconds": 3.0, "start_time": "t5", "end_time": "t6"},
    ])
    return p


@pytest.fixture()
def rd(log_file: Path) -> RunDiff:
    return RunDiff(str(log_file))


def test_diff_returns_run_diff_result(rd: RunDiff) -> None:
    result = rd.diff("aaa", "bbb")
    assert isinstance(result, RunDiffResult)


def test_diff_identifies_changed_fields(rd: RunDiff) -> None:
    result = rd.diff("aaa", "bbb")
    assert "status" in result.changed_fields
    assert "duration_seconds" in result.changed_fields


def test_diff_unchanged_field_not_in_changed_fields(rd: RunDiff) -> None:
    result = rd.diff("aaa", "bbb")
    assert "pipeline" not in result.changed_fields


def test_diff_has_changes_true_when_fields_differ(rd: RunDiff) -> None:
    result = rd.diff("aaa", "bbb")
    assert result.has_changes is True


def test_diff_has_changes_false_for_identical_runs(log_file: Path) -> None:
    _write_records(log_file, [
        {"run_id": "x1", "pipeline": "etl", "status": "success", "start_time": "t", "end_time": "t"},
        {"run_id": "x2", "pipeline": "etl", "status": "success", "start_time": "t", "end_time": "t"},
    ])
    rd = RunDiff(str(log_file))
    result = rd.diff("x1", "x2")
    assert result.has_changes is False


def test_diff_ignores_run_id_start_end_time(rd: RunDiff) -> None:
    result = rd.diff("aaa", "bbb")
    assert "run_id" not in result.changed_fields
    assert "start_time" not in result.changed_fields
    assert "end_time" not in result.changed_fields


def test_diff_respects_custom_ignore_keys(rd: RunDiff) -> None:
    result = rd.diff("aaa", "bbb", ignore_keys=["status"])
    assert "status" not in result.changed_fields


def test_diff_raises_for_missing_run_id_a(rd: RunDiff) -> None:
    with pytest.raises(KeyError, match="missing_id"):
        rd.diff("missing_id", "bbb")


def test_diff_raises_for_missing_run_id_b(rd: RunDiff) -> None:
    with pytest.raises(KeyError, match="missing_id"):
        rd.diff("aaa", "missing_id")


def test_diff_to_dict_structure(rd: RunDiff) -> None:
    result = rd.diff("aaa", "bbb")
    d = result.to_dict()
    assert d["run_id_a"] == "aaa"
    assert d["run_id_b"] == "bbb"
    assert isinstance(d["diffs"], list)
    assert "has_changes" in d
    assert "changed_fields" in d


def test_field_diff_to_dict(rd: RunDiff) -> None:
    result = rd.diff("aaa", "bbb")
    status_diff = next(d for d in result.diffs if d.key == "status")
    fd = status_diff.to_dict()
    assert fd["key"] == "status"
    assert fd["old"] == "success"
    assert fd["new"] == "failure"
    assert fd["changed"] is True


def test_diff_missing_log_file_raises_key_error(tmp_path: Path) -> None:
    rd = RunDiff(str(tmp_path / "nonexistent.jsonl"))
    with pytest.raises(KeyError):
        rd.diff("aaa", "bbb")
