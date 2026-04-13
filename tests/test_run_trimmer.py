"""Tests for RunTrimmer."""

import json
import os
import pytest

from pipewatch.run_trimmer import RunTrimmer, TrimError, TrimResult


def _write_records(path, records):
    with open(path, "w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return str(tmp_path / "runs.log")


@pytest.fixture
def trimmer(log_file):
    return RunTrimmer(log_file, max_length=10)


def test_trim_returns_trim_result(trimmer, log_file):
    _write_records(log_file, [{"pipeline": "etl", "status": "success"}])
    result = trimmer.trim()
    assert isinstance(result, TrimResult)


def test_trim_truncates_long_field(trimmer, log_file):
    _write_records(log_file, [{"pipeline": "a" * 50, "status": "ok"}])
    trimmer.trim()
    with open(log_file) as fh:
        record = json.loads(fh.readline())
    assert record["pipeline"] == "a" * 10


def test_trim_leaves_short_fields_unchanged(trimmer, log_file):
    _write_records(log_file, [{"pipeline": "etl", "status": "ok"}])
    trimmer.trim()
    with open(log_file) as fh:
        record = json.loads(fh.readline())
    assert record["pipeline"] == "etl"


def test_trim_count_reflects_modified_records(trimmer, log_file):
    _write_records(log_file, [
        {"pipeline": "x" * 20},
        {"pipeline": "short"},
    ])
    result = trimmer.trim()
    assert result.trimmed_count == 1
    assert result.total_records == 2


def test_trim_reports_affected_fields(trimmer, log_file):
    _write_records(log_file, [{"pipeline": "x" * 20, "note": "y" * 15}])
    result = trimmer.trim()
    assert "pipeline" in result.fields_truncated
    assert "note" in result.fields_truncated


def test_trim_only_specified_fields(log_file):
    tr = RunTrimmer(log_file, max_length=5, fields=["note"])
    _write_records(log_file, [{"pipeline": "toolongname", "note": "toolongnote"}])
    tr.trim()
    with open(log_file) as fh:
        record = json.loads(fh.readline())
    assert record["pipeline"] == "toolongname"  # untouched
    assert record["note"] == "toolo"


def test_trim_empty_file_returns_zero_counts(trimmer, log_file):
    _write_records(log_file, [])
    result = trimmer.trim()
    assert result.trimmed_count == 0
    assert result.total_records == 0


def test_trim_missing_file_returns_zero_counts(trimmer):
    result = trimmer.trim()
    assert result.trimmed_count == 0
    assert result.total_records == 0


def test_invalid_log_file_raises():
    with pytest.raises(TrimError):
        RunTrimmer("", max_length=10)


def test_invalid_max_length_raises(log_file):
    with pytest.raises(TrimError):
        RunTrimmer(log_file, max_length=0)


def test_to_dict_contains_expected_keys(trimmer, log_file):
    _write_records(log_file, [{"pipeline": "x" * 20}])
    result = trimmer.trim()
    d = result.to_dict()
    assert "trimmed_count" in d
    assert "total_records" in d
    assert "fields_truncated" in d
