"""Tests for pipewatch.retention_policy."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.retention_policy import RetentionManager, RetentionPolicy


def _make_record(pipeline: str, days_ago: float = 0) -> dict:
    ts = datetime.now(tz=timezone.utc) - timedelta(days=days_ago)
    return {"pipeline": pipeline, "started_at": ts.isoformat(), "status": "success"}


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _read_records(path: Path) -> list:
    with path.open("r") as fh:
        return [json.loads(line) for line in fh if line.strip()]


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.jsonl"


def test_prune_by_age_removes_old_records(log_file):
    records = [
        _make_record("old_pipe", days_ago=10),
        _make_record("recent_pipe", days_ago=1),
    ]
    _write_records(log_file, records)
    manager = RetentionManager(log_file, RetentionPolicy(max_age_days=5))
    pruned = manager.prune()
    assert pruned == 1
    remaining = _read_records(log_file)
    assert len(remaining) == 1
    assert remaining[0]["pipeline"] == "recent_pipe"


def test_prune_by_count_keeps_latest(log_file):
    records = [_make_record(f"pipe_{i}", days_ago=i) for i in range(5)]
    _write_records(log_file, records)
    manager = RetentionManager(log_file, RetentionPolicy(max_runs=3))
    pruned = manager.prune()
    assert pruned == 2
    remaining = _read_records(log_file)
    assert len(remaining) == 3


def test_prune_combined_policy(log_file):
    records = [
        _make_record("very_old", days_ago=20),
        _make_record("old", days_ago=8),
        _make_record("mid", days_ago=3),
        _make_record("new", days_ago=0),
    ]
    _write_records(log_file, records)
    manager = RetentionManager(log_file, RetentionPolicy(max_age_days=7, max_runs=2))
    pruned = manager.prune()
    assert pruned == 3
    remaining = _read_records(log_file)
    assert len(remaining) == 1
    assert remaining[0]["pipeline"] == "new"


def test_prune_missing_file_returns_zero(log_file):
    manager = RetentionManager(log_file, RetentionPolicy(max_age_days=7))
    assert manager.prune() == 0


def test_prune_no_violation_returns_zero(log_file):
    records = [_make_record("pipe", days_ago=1)]
    _write_records(log_file, records)
    manager = RetentionManager(log_file, RetentionPolicy(max_age_days=30))
    assert manager.prune() == 0


def test_retention_policy_is_valid():
    assert RetentionPolicy(max_age_days=7).is_valid()
    assert RetentionPolicy(max_runs=100).is_valid()
    assert not RetentionPolicy().is_valid()
