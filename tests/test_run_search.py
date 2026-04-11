"""Tests for pipewatch.run_search.RunSearch."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_search import RunSearch


DEFAULT_RECORDS = [
    {"run_id": "aaa", "pipeline": "etl", "status": "success", "note": "nightly run"},
    {"run_id": "bbb", "pipeline": "etl", "status": "failure", "note": "disk full"},
    {"run_id": "ccc", "pipeline": "report", "status": "success", "note": "weekly"},
]


def _write_records(path: Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    p = tmp_path / "runs.log"
    _write_records(p, DEFAULT_RECORDS)
    return p


@pytest.fixture()
def search(log_file: Path) -> RunSearch:
    return RunSearch(str(log_file))


def test_by_field_returns_matching_records(search: RunSearch) -> None:
    results = search.by_field("pipeline", "etl")
    assert len(results) == 2
    assert all(r["pipeline"] == "etl" for r in results)


def test_by_field_case_insensitive(search: RunSearch) -> None:
    results = search.by_field("status", "SUCCESS")
    assert len(results) == 2


def test_by_field_no_match_returns_empty(search: RunSearch) -> None:
    results = search.by_field("pipeline", "nonexistent")
    assert results == []


def test_by_fields_multiple_criteria(search: RunSearch) -> None:
    results = search.by_fields(pipeline="etl", status="failure")
    assert len(results) == 1
    assert results[0]["run_id"] == "bbb"


def test_by_fields_no_match(search: RunSearch) -> None:
    results = search.by_fields(pipeline="report", status="failure")
    assert results == []


def test_text_search_finds_substring(search: RunSearch) -> None:
    results = search.text_search("disk")
    assert len(results) == 1
    assert results[0]["run_id"] == "bbb"


def test_text_search_case_insensitive(search: RunSearch) -> None:
    results = search.text_search("NIGHTLY")
    assert len(results) == 1
    assert results[0]["run_id"] == "aaa"


def test_text_search_no_match_returns_empty(search: RunSearch) -> None:
    results = search.text_search("zzznomatch")
    assert results == []


def test_where_predicate(search: RunSearch) -> None:
    results = search.where(lambda r: r.get("status") == "success")
    assert len(results) == 2


def test_missing_log_file_returns_empty(tmp_path: Path) -> None:
    s = RunSearch(str(tmp_path / "missing.log"))
    assert s.by_field("pipeline", "etl") == []
    assert s.text_search("anything") == []
    assert s.where(lambda r: True) == []
