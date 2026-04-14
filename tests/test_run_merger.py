"""Tests for pipewatch.run_merger."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_merger import MergeError, MergeResult, RunMerger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_records(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture()
def dirs(tmp_path):
    src_a = tmp_path / "a.jsonl"
    src_b = tmp_path / "b.jsonl"
    out = tmp_path / "merged.jsonl"
    return src_a, src_b, out


@pytest.fixture()
def merger(dirs):
    _, _, out = dirs
    return RunMerger(output_file=str(out))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_merge_returns_merge_result(dirs, merger):
    src_a, src_b, _ = dirs
    _write_records(src_a, [{"run_id": "1", "pipeline": "p"}])
    _write_records(src_b, [{"run_id": "2", "pipeline": "q"}])
    result = merger.merge([str(src_a), str(src_b)])
    assert isinstance(result, MergeResult)


def test_merge_count_reflects_unique_records(dirs, merger):
    src_a, src_b, _ = dirs
    _write_records(src_a, [{"run_id": "1"}, {"run_id": "2"}])
    _write_records(src_b, [{"run_id": "3"}])
    result = merger.merge([str(src_a), str(src_b)])
    assert result.merged_count == 3
    assert result.skipped_count == 0


def test_merge_deduplicates_by_run_id(dirs, merger):
    src_a, src_b, _ = dirs
    _write_records(src_a, [{"run_id": "dup", "val": 1}])
    _write_records(src_b, [{"run_id": "dup", "val": 2}])
    result = merger.merge([str(src_a), str(src_b)], deduplicate=True)
    assert result.merged_count == 1
    assert result.skipped_count == 1


def test_merge_no_dedup_keeps_all(dirs, merger):
    src_a, src_b, _ = dirs
    _write_records(src_a, [{"run_id": "dup"}])
    _write_records(src_b, [{"run_id": "dup"}])
    result = merger.merge([str(src_a), str(src_b)], deduplicate=False)
    assert result.merged_count == 2


def test_merge_writes_output_file(dirs, merger):
    src_a, _, out = dirs
    _write_records(src_a, [{"run_id": "x"}])
    merger.merge([str(src_a)])
    assert out.exists()
    lines = [l for l in out.read_text().splitlines() if l.strip()]
    assert len(lines) == 1


def test_merge_missing_source_produces_empty(dirs, merger):
    src_a, _, _ = dirs
    # src_a does not exist
    result = merger.merge([str(src_a)])
    assert result.merged_count == 0


def test_merge_raises_on_empty_source_list(merger):
    with pytest.raises(MergeError):
        merger.merge([])


def test_merge_result_to_dict(dirs, merger):
    src_a, _, _ = dirs
    _write_records(src_a, [{"run_id": "1"}])
    result = merger.merge([str(src_a)])
    d = result.to_dict()
    assert "merged_count" in d
    assert "skipped_count" in d
    assert "source_files" in d
    assert "output_file" in d
