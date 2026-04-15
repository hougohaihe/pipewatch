from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_sorter import RunSorter, SortError, SortResult


def _write_records(path: Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.log"


@pytest.fixture()
def sorter(log_file: Path) -> RunSorter:
    _write_records(
        log_file,
        [
            {"run_id": "a", "pipeline": "etl", "status": "success", "duration_seconds": 10.0},
            {"run_id": "b", "pipeline": "etl", "status": "failure", "duration_seconds": 5.0},
            {"run_id": "c", "pipeline": "ingest", "status": "success", "duration_seconds": 20.0},
            {"run_id": "d", "pipeline": "ingest", "status": "success", "duration_seconds": 3.0},
        ],
    )
    return RunSorter(log_file=str(log_file))


def test_sort_returns_sort_result(sorter: RunSorter) -> None:
    result = sorter.sort_by("duration_seconds")
    assert isinstance(result, SortResult)


def test_sort_ascending_order(sorter: RunSorter) -> None:
    result = sorter.sort_by("duration_seconds")
    durations = [r["duration_seconds"] for r in result.records]
    assert durations == sorted(durations)


def test_sort_descending_order(sorter: RunSorter) -> None:
    result = sorter.sort_by("duration_seconds", reverse=True)
    durations = [r["duration_seconds"] for r in result.records]
    assert durations == sorted(durations, reverse=True)


def test_sort_total_reflects_record_count(sorter: RunSorter) -> None:
    result = sorter.sort_by("run_id")
    assert result.total == 4


def test_sort_by_pipeline_filters_correctly(sorter: RunSorter) -> None:
    result = sorter.sort_by("duration_seconds", pipeline="etl")
    assert all(r["pipeline"] == "etl" for r in result.records)
    assert result.total == 2


def test_sort_result_stores_field_and_reverse(sorter: RunSorter) -> None:
    result = sorter.sort_by("status", reverse=True)
    assert result.field == "status"
    assert result.reverse is True


def test_sort_missing_file_returns_empty(tmp_path: Path) -> None:
    s = RunSorter(log_file=str(tmp_path / "missing.log"))
    result = s.sort_by("run_id")
    assert result.records == []
    assert result.total == 0


def test_sort_invalid_field_raises(sorter: RunSorter) -> None:
    with pytest.raises(SortError):
        sorter.sort_by("")


def test_sort_records_with_missing_field_pushed_to_end(log_file: Path) -> None:
    _write_records(
        log_file,
        [
            {"run_id": "x", "pipeline": "p", "duration_seconds": 7.0},
            {"run_id": "y", "pipeline": "p"},
            {"run_id": "z", "pipeline": "p", "duration_seconds": 2.0},
        ],
    )
    s = RunSorter(log_file=str(log_file))
    result = s.sort_by("duration_seconds")
    assert result.records[-1]["run_id"] == "y"


def test_sort_result_to_dict_contains_required_keys(sorter: RunSorter) -> None:
    result = sorter.sort_by("run_id")
    d = result.to_dict()
    assert "field" in d
    assert "reverse" in d
    assert "total" in d
    assert "records" in d
