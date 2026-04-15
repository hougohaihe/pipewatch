import json
import os
import pytest

from pipewatch.run_bookmark import BookmarkEntry, BookmarkError, RunBookmark


@pytest.fixture
def bookmark_file(tmp_path):
    return str(tmp_path / "bookmarks.json")


@pytest.fixture
def bm(bookmark_file):
    return RunBookmark(bookmark_file=bookmark_file)


def test_add_creates_entry(bm):
    entry = bm.add(run_id="abc123", pipeline="etl", label="baseline")
    assert isinstance(entry, BookmarkEntry)
    assert entry.run_id == "abc123"
    assert entry.pipeline == "etl"
    assert entry.label == "baseline"


def test_add_persists_to_disk(bm, bookmark_file):
    bm.add(run_id="abc123", pipeline="etl", label="baseline")
    with open(bookmark_file) as fh:
        data = json.load(fh)
    assert "abc123" in data


def test_add_with_note_and_tags(bm):
    entry = bm.add(run_id="r1", pipeline="p1", label="v1", note="good run", tags=["prod", "fast"])
    assert entry.note == "good run"
    assert entry.tags == ["prod", "fast"]


def test_add_rejects_empty_run_id(bm):
    with pytest.raises(BookmarkError):
        bm.add(run_id="", pipeline="p", label="l")


def test_add_rejects_empty_label(bm):
    with pytest.raises(BookmarkError):
        bm.add(run_id="r1", pipeline="p", label="")


def test_get_returns_entry(bm):
    bm.add(run_id="r1", pipeline="p", label="l")
    result = bm.get("r1")
    assert result is not None
    assert result.run_id == "r1"


def test_get_returns_none_for_missing(bm):
    assert bm.get("missing") is None


def test_remove_deletes_entry(bm):
    bm.add(run_id="r1", pipeline="p", label="l")
    removed = bm.remove("r1")
    assert removed is True
    assert bm.get("r1") is None


def test_remove_returns_false_for_missing(bm):
    assert bm.remove("nonexistent") is False


def test_all_returns_all_entries(bm):
    bm.add(run_id="r1", pipeline="p1", label="l1")
    bm.add(run_id="r2", pipeline="p2", label="l2")
    assert len(bm.all()) == 2


def test_by_label_filters_correctly(bm):
    bm.add(run_id="r1", pipeline="p1", label="baseline")
    bm.add(run_id="r2", pipeline="p2", label="checkpoint")
    results = bm.by_label("baseline")
    assert len(results) == 1
    assert results[0].run_id == "r1"


def test_by_label_case_insensitive(bm):
    bm.add(run_id="r1", pipeline="p1", label="Baseline")
    results = bm.by_label("baseline")
    assert len(results) == 1


def test_by_pipeline_filters_correctly(bm):
    bm.add(run_id="r1", pipeline="etl", label="l1")
    bm.add(run_id="r2", pipeline="ml", label="l2")
    results = bm.by_pipeline("etl")
    assert len(results) == 1
    assert results[0].pipeline == "etl"


def test_loads_existing_file(bookmark_file):
    data = {
        "r1": {"run_id": "r1", "pipeline": "p", "label": "l", "note": "", "tags": []}
    }
    with open(bookmark_file, "w") as fh:
        json.dump(data, fh)
    bm2 = RunBookmark(bookmark_file=bookmark_file)
    assert bm2.get("r1") is not None
