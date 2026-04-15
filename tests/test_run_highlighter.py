from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_highlighter import HighlightResult, RunHighlighter


def _write_records(path: Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    p = tmp_path / "runs.jsonl"
    _write_records(
        p,
        [
            {"run_id": "a1", "pipeline": "etl", "status": "success", "duration_seconds": 10.0},
            {"run_id": "b2", "pipeline": "etl", "status": "failure", "duration_seconds": 5.0},
            {"run_id": "c3", "pipeline": "load", "status": "success", "duration_seconds": 120.0},
            {"run_id": "d4", "pipeline": "load", "status": "failure", "duration_seconds": 3.0},
        ],
    )
    return p


@pytest.fixture()
def highlighter(log_file: Path) -> RunHighlighter:
    return RunHighlighter(log_file=str(log_file))


def test_by_status_returns_matching_records(highlighter: RunHighlighter) -> None:
    results = highlighter.by_status("failure")
    assert len(results) == 2
    assert all(r.reason == "status=failure" for r in results)


def test_by_status_case_insensitive(highlighter: RunHighlighter) -> None:
    results = highlighter.by_status("SUCCESS")
    assert len(results) == 2


def test_by_status_empty_for_unknown(highlighter: RunHighlighter) -> None:
    results = highlighter.by_status("running")
    assert results == []


def test_by_duration_above_returns_correct_runs(highlighter: RunHighlighter) -> None:
    results = highlighter.by_duration_above(50.0)
    assert len(results) == 1
    assert results[0].run_id == "c3"


def test_by_duration_above_reason_format(highlighter: RunHighlighter) -> None:
    results = highlighter.by_duration_above(9.0)
    assert results[0].reason == "duration>9.0s"


def test_by_duration_above_empty_for_high_threshold(highlighter: RunHighlighter) -> None:
    results = highlighter.by_duration_above(9999.0)
    assert results == []


def test_by_field_value_matches_pipeline(highlighter: RunHighlighter) -> None:
    results = highlighter.by_field_value("pipeline", "load")
    assert len(results) == 2
    assert all(r.pipeline == "load" for r in results)


def test_by_field_value_returns_empty_for_missing_file(tmp_path: Path) -> None:
    h = RunHighlighter(log_file=str(tmp_path / "missing.jsonl"))
    assert h.by_field_value("pipeline", "etl") == []


def test_highlight_result_to_dict() -> None:
    r = HighlightResult(run_id="x1", pipeline="p", reason="status=failure", fields={"status": "failure"})
    d = r.to_dict()
    assert d["run_id"] == "x1"
    assert d["pipeline"] == "p"
    assert d["reason"] == "status=failure"
    assert d["fields"] == {"status": "failure"}


def test_skips_invalid_json_lines(tmp_path: Path) -> None:
    p = tmp_path / "runs.jsonl"
    p.write_text('{"run_id": "ok", "pipeline": "etl", "status": "success", "duration_seconds": 1.0}\nnot-json\n')
    h = RunHighlighter(log_file=str(p))
    results = h.by_status("success")
    assert len(results) == 1
