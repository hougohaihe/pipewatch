"""Tests for pipewatch.run_labeler."""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.run_labeler import LabelError, RunLabeler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_records(path: Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.log"


@pytest.fixture()
def labeler(log_file: Path) -> RunLabeler:
    _write_records(log_file, [
        {"run_id": "aaa", "pipeline": "etl", "labels": {"env": "prod"}},
        {"run_id": "bbb", "pipeline": "etl"},
        {"run_id": "ccc", "pipeline": "ingest", "labels": {"env": "staging"}},
    ])
    return RunLabeler(str(log_file))


# ---------------------------------------------------------------------------
# set_labels
# ---------------------------------------------------------------------------

def test_set_labels_adds_new_key(labeler: RunLabeler) -> None:
    labeler.set_labels("bbb", {"owner": "alice"})
    assert labeler.get_labels("bbb") == {"owner": "alice"}


def test_set_labels_merges_with_existing(labeler: RunLabeler) -> None:
    labeler.set_labels("aaa", {"team": "data"})
    labels = labeler.get_labels("aaa")
    assert labels["env"] == "prod"
    assert labels["team"] == "data"


def test_set_labels_overwrites_existing_key(labeler: RunLabeler) -> None:
    labeler.set_labels("aaa", {"env": "dev"})
    assert labeler.get_labels("aaa")["env"] == "dev"


def test_set_labels_raises_for_unknown_run(labeler: RunLabeler) -> None:
    with pytest.raises(LabelError, match="run_id not found"):
        labeler.set_labels("zzz", {"x": "y"})


def test_set_labels_raises_for_non_dict(labeler: RunLabeler) -> None:
    with pytest.raises(LabelError, match="labels must be a dict"):
        labeler.set_labels("aaa", ["not", "a", "dict"])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# remove_label
# ---------------------------------------------------------------------------

def test_remove_label_removes_key(labeler: RunLabeler) -> None:
    labeler.remove_label("aaa", "env")
    assert "env" not in labeler.get_labels("aaa")


def test_remove_label_ignores_missing_key(labeler: RunLabeler) -> None:
    labeler.remove_label("aaa", "nonexistent")  # should not raise


def test_remove_label_raises_for_unknown_run(labeler: RunLabeler) -> None:
    with pytest.raises(LabelError, match="run_id not found"):
        labeler.remove_label("zzz", "env")


# ---------------------------------------------------------------------------
# get_labels
# ---------------------------------------------------------------------------

def test_get_labels_returns_empty_dict_when_no_labels(labeler: RunLabeler) -> None:
    assert labeler.get_labels("bbb") == {}


def test_get_labels_raises_for_unknown_run(labeler: RunLabeler) -> None:
    with pytest.raises(LabelError, match="run_id not found"):
        labeler.get_labels("zzz")


# ---------------------------------------------------------------------------
# find_by_label
# ---------------------------------------------------------------------------

def test_find_by_label_returns_matching_records(labeler: RunLabeler) -> None:
    results = labeler.find_by_label("env", "prod")
    assert len(results) == 1
    assert results[0]["run_id"] == "aaa"


def test_find_by_label_returns_empty_list_when_no_match(labeler: RunLabeler) -> None:
    assert labeler.find_by_label("env", "nonexistent") == []


def test_find_by_label_returns_multiple_matches(labeler: RunLabeler, log_file: Path) -> None:
    _write_records(log_file, [
        {"run_id": "x1", "labels": {"tier": "gold"}},
        {"run_id": "x2", "labels": {"tier": "gold"}},
        {"run_id": "x3", "labels": {"tier": "silver"}},
    ])
    lb = RunLabeler(str(log_file))
    results = lb.find_by_label("tier", "gold")
    assert {r["run_id"] for r in results} == {"x1", "x2"}
