"""Tests for pipewatch.run_annotator."""

import json
import os

import pytest

from pipewatch.run_annotator import AnnotationError, RunAnnotator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_records(path, records):
    with open(path, "w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture()
def log_file(tmp_path):
    p = tmp_path / "runs.jsonl"
    _write_records(
        p,
        [
            {"run_id": "aaa", "pipeline": "etl", "status": "success"},
            {"run_id": "bbb", "pipeline": "etl", "status": "failure"},
            {"run_id": "ccc", "pipeline": "ingest", "status": "success"},
        ],
    )
    return str(p)


@pytest.fixture()
def annotator(log_file):
    return RunAnnotator(log_file)


# ---------------------------------------------------------------------------
# annotate()
# ---------------------------------------------------------------------------

def test_annotate_adds_tags(annotator, log_file):
    annotator.annotate("aaa", tags=["critical", "reviewed"])
    with open(log_file) as fh:
        records = [json.loads(l) for l in fh]
    assert records[0]["tags"] == ["critical", "reviewed"]


def test_annotate_adds_note(annotator, log_file):
    annotator.annotate("bbb", note="needs retry")
    with open(log_file) as fh:
        records = [json.loads(l) for l in fh]
    assert records[1]["note"] == "needs retry"


def test_annotate_deduplicates_tags(annotator):
    annotator.annotate("aaa", tags=["critical"])
    annotator.annotate("aaa", tags=["critical", "new"])
    result = annotator.get_annotations("aaa")
    assert result["tags"].count("critical") == 1
    assert "new" in result["tags"]


def test_annotate_returns_updated_record(annotator):
    record = annotator.annotate("ccc", tags=["ok"], note="looks good")
    assert record["run_id"] == "ccc"
    assert "ok" in record["tags"]
    assert record["note"] == "looks good"


def test_annotate_unknown_run_id_raises(annotator):
    with pytest.raises(AnnotationError, match="not found"):
        annotator.annotate("zzz", tags=["x"])


# ---------------------------------------------------------------------------
# get_annotations()
# ---------------------------------------------------------------------------

def test_get_annotations_defaults(annotator):
    result = annotator.get_annotations("aaa")
    assert result == {"tags": [], "note": ""}


def test_get_annotations_unknown_raises(annotator):
    with pytest.raises(AnnotationError):
        annotator.get_annotations("missing")


# ---------------------------------------------------------------------------
# find_by_tag()
# ---------------------------------------------------------------------------

def test_find_by_tag_returns_matching(annotator):
    annotator.annotate("aaa", tags=["urgent"])
    annotator.annotate("bbb", tags=["urgent"])
    results = annotator.find_by_tag("urgent")
    assert len(results) == 2


def test_find_by_tag_returns_empty_when_none_match(annotator):
    assert annotator.find_by_tag("nonexistent") == []
