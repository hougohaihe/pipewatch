"""Tests for pipewatch.run_tagger.TagIndex."""

import json
import pytest

from pipewatch.run_tagger import TagIndex


@pytest.fixture()
def index_file(tmp_path):
    return str(tmp_path / "tag_index.json")


@pytest.fixture()
def idx(index_file):
    return TagIndex(index_file)


def test_add_creates_tag_entry(idx):
    idx.add("nightly", "run-001")
    assert "run-001" in idx.runs_for_tag("nightly")


def test_add_persists_to_disk(index_file, idx):
    idx.add("nightly", "run-001")
    reloaded = TagIndex(index_file)
    assert "run-001" in reloaded.runs_for_tag("nightly")


def test_add_does_not_duplicate(idx):
    idx.add("nightly", "run-001")
    idx.add("nightly", "run-001")
    assert idx.runs_for_tag("nightly").count("run-001") == 1


def test_remove_returns_true_on_success(idx):
    idx.add("nightly", "run-001")
    assert idx.remove("nightly", "run-001") is True


def test_remove_returns_false_when_not_found(idx):
    assert idx.remove("nightly", "run-999") is False


def test_remove_cleans_up_empty_tag(idx):
    idx.add("nightly", "run-001")
    idx.remove("nightly", "run-001")
    assert "nightly" not in idx.all_tags()


def test_tags_for_run_returns_all_tags(idx):
    idx.add("nightly", "run-001")
    idx.add("critical", "run-001")
    tags = idx.tags_for_run("run-001")
    assert set(tags) == {"nightly", "critical"}


def test_tags_for_run_empty_when_unknown(idx):
    assert idx.tags_for_run("run-999") == []


def test_all_tags_lists_known_tags(idx):
    idx.add("alpha", "run-001")
    idx.add("beta", "run-002")
    assert set(idx.all_tags()) == {"alpha", "beta"}


def test_clear_tag_removes_all_runs(idx):
    idx.add("nightly", "run-001")
    idx.add("nightly", "run-002")
    removed = idx.clear_tag("nightly")
    assert removed == 2
    assert idx.runs_for_tag("nightly") == []


def test_clear_tag_returns_zero_for_unknown(idx):
    assert idx.clear_tag("nonexistent") == 0


def test_missing_index_file_starts_empty(index_file):
    idx = TagIndex(index_file + ".missing")
    assert idx.all_tags() == []
