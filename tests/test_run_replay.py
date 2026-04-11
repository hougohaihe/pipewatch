"""Tests for pipewatch.run_replay."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipewatch.run_replay import ReplayError, ReplayResult, RunReplay


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


_RECORDS = [
    {"run_id": "aaa", "pipeline": "etl", "status": "success", "duration": 1.2},
    {"run_id": "bbb", "pipeline": "etl", "status": "failure", "duration": 0.5},
    {"run_id": "ccc", "pipeline": "ingest", "status": "success", "duration": 3.0},
]


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    p = tmp_path / "runs.jsonl"
    _write_records(p, _RECORDS)
    return p


@pytest.fixture()
def rr(log_file: Path) -> RunReplay:
    return RunReplay(log_file=str(log_file))


# ---------------------------------------------------------------------------
# RunReplay.get
# ---------------------------------------------------------------------------

def test_get_returns_correct_record(rr: RunReplay) -> None:
    record = rr.get("bbb")
    assert record is not None
    assert record["pipeline"] == "etl"
    assert record["status"] == "failure"


def test_get_returns_none_for_missing_id(rr: RunReplay) -> None:
    assert rr.get("zzz") is None


def test_get_returns_none_for_missing_file(tmp_path: Path) -> None:
    rr = RunReplay(log_file=str(tmp_path / "nonexistent.jsonl"))
    assert rr.get("aaa") is None


# ---------------------------------------------------------------------------
# RunReplay.replay
# ---------------------------------------------------------------------------

def test_replay_calls_dispatcher(rr: RunReplay) -> None:
    dispatcher = MagicMock()
    result = rr.replay("aaa", dispatcher)
    dispatcher.send.assert_called_once()
    assert result.replayed is True
    assert result.error is None


def test_replay_raises_for_unknown_run_id(rr: RunReplay) -> None:
    dispatcher = MagicMock()
    with pytest.raises(ReplayError, match="not found"):
        rr.replay("zzz", dispatcher)


def test_replay_captures_dispatcher_exception(rr: RunReplay) -> None:
    dispatcher = MagicMock()
    dispatcher.send.side_effect = RuntimeError("webhook down")
    result = rr.replay("aaa", dispatcher)
    assert result.replayed is False
    assert "webhook down" in result.error


# ---------------------------------------------------------------------------
# RunReplay.replay_all
# ---------------------------------------------------------------------------

def test_replay_all_replays_pipeline_records(rr: RunReplay) -> None:
    dispatcher = MagicMock()
    results = rr.replay_all("etl", dispatcher)
    assert len(results) == 2
    assert all(r.replayed for r in results)
    assert dispatcher.send.call_count == 2


def test_replay_all_returns_empty_for_unknown_pipeline(rr: RunReplay) -> None:
    dispatcher = MagicMock()
    results = rr.replay_all("no_such_pipeline", dispatcher)
    assert results == []


# ---------------------------------------------------------------------------
# ReplayResult.to_dict
# ---------------------------------------------------------------------------

def test_replay_result_to_dict_keys() -> None:
    res = ReplayResult(run_id="x", pipeline="p", status="success", replayed=True)
    d = res.to_dict()
    assert set(d.keys()) == {"run_id", "pipeline", "status", "replayed", "error"}
