"""Tests for pipewatch.run_deduplicator."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_deduplicator import (
    DeduplicationError,
    DeduplicationResult,
    RunDeduplicator,
)


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.log"


@pytest.fixture
def dedup(log_file: Path) -> RunDeduplicator:
    return RunDeduplicator(str(log_file))


def test_deduplicate_returns_result_instance(log_file, dedup):
    _write_records(log_file, [{"run_id": "a", "status": "success"}])
    result = dedup.deduplicate()
    assert isinstance(result, DeduplicationResult)


def test_deduplicate_no_duplicates(log_file, dedup):
    records = [
        {"run_id": "a", "status": "success"},
        {"run_id": "b", "status": "failed"},
    ]
    _write_records(log_file, records)
    result = dedup.deduplicate()
    assert result.duplicates_removed == 0
    assert result.total_records == 2


def test_deduplicate_removes_exact_duplicates(log_file, dedup):
    records = [
        {"run_id": "a", "status": "success"},
        {"run_id": "a", "status": "success"},
        {"run_id": "b", "status": "failed"},
    ]
    _write_records(log_file, records)
    result = dedup.deduplicate()
    assert result.duplicates_removed == 1
    assert len(result.unique_records) == 2


def test_deduplicate_keeps_first_occurrence(log_file, dedup):
    records = [
        {"run_id": "a", "status": "success"},
        {"run_id": "a", "status": "failed"},
    ]
    _write_records(log_file, records)
    result = dedup.deduplicate()
    assert result.unique_records[0]["status"] == "success"


def test_deduplicate_writes_to_disk(log_file, dedup):
    records = [
        {"run_id": "x", "status": "success"},
        {"run_id": "x", "status": "success"},
    ]
    _write_records(log_file, records)
    dedup.deduplicate()
    lines = [l for l in log_file.read_text().splitlines() if l.strip()]
    assert len(lines) == 1


def test_deduplicate_dry_run_does_not_modify_file(log_file, dedup):
    records = [
        {"run_id": "x", "status": "success"},
        {"run_id": "x", "status": "success"},
    ]
    _write_records(log_file, records)
    dedup.deduplicate(dry_run=True)
    lines = [l for l in log_file.read_text().splitlines() if l.strip()]
    assert len(lines) == 2


def test_deduplicate_missing_file_returns_empty_result(log_file, dedup):
    result = dedup.deduplicate()
    assert result.total_records == 0
    assert result.duplicates_removed == 0
    assert result.unique_records == []


def test_find_duplicates_returns_only_duplicated_ids(log_file, dedup):
    records = [
        {"run_id": "a"},
        {"run_id": "a"},
        {"run_id": "b"},
    ]
    _write_records(log_file, records)
    dupes = dedup.find_duplicates()
    assert "a" in dupes
    assert "b" not in dupes
    assert len(dupes["a"]) == 2


def test_malformed_json_raises_deduplication_error(log_file, dedup):
    log_file.write_text("not-json\n")
    with pytest.raises(DeduplicationError):
        dedup.deduplicate()


def test_to_dict_contains_expected_keys(log_file, dedup):
    _write_records(log_file, [{"run_id": "a"}, {"run_id": "a"}])
    result = dedup.deduplicate()
    d = result.to_dict()
    assert "total_records" in d
    assert "duplicates_removed" in d
    assert "unique_count" in d
