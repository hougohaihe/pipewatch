"""Tests for pipewatch.run_snapshot."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from pipewatch.run_snapshot import RunSnapshot, SnapshotError, SnapshotManifest


def _write_records(log_file: Path, records: list) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture()
def dirs(tmp_path):
    log_file = tmp_path / "runs.jsonl"
    snap_dir = tmp_path / "snapshots"
    return log_file, snap_dir


@pytest.fixture()
def snap(dirs):
    log_file, snap_dir = dirs
    return RunSnapshot(str(log_file), str(snap_dir))


_RECORDS = [
    {"run_id": "a1", "pipeline": "etl", "status": "success", "duration": 1.2},
    {"run_id": "a2", "pipeline": "etl", "status": "failure", "duration": 0.5},
]


def test_capture_creates_gz_file(dirs, snap):
    log_file, snap_dir = dirs
    _write_records(log_file, _RECORDS)
    manifest = snap.capture(snapshot_id="snap1")
    assert (snap_dir / "snap1.jsonl.gz").exists()


def test_capture_creates_manifest_file(dirs, snap):
    log_file, snap_dir = dirs
    _write_records(log_file, _RECORDS)
    snap.capture(snapshot_id="snap1")
    assert (snap_dir / "snap1.manifest.json").exists()


def test_capture_returns_manifest_instance(dirs, snap):
    log_file, _ = dirs
    _write_records(log_file, _RECORDS)
    result = snap.capture(snapshot_id="snap1")
    assert isinstance(result, SnapshotManifest)


def test_capture_record_count_in_manifest(dirs, snap):
    log_file, _ = dirs
    _write_records(log_file, _RECORDS)
    manifest = snap.capture(snapshot_id="snap1")
    assert manifest.record_count == len(_RECORDS)


def test_capture_stores_tags(dirs, snap):
    log_file, _ = dirs
    _write_records(log_file, _RECORDS)
    manifest = snap.capture(snapshot_id="snap1", tags=["daily", "prod"])
    assert manifest.tags == ["daily", "prod"]


def test_capture_duplicate_id_raises(dirs, snap):
    log_file, _ = dirs
    _write_records(log_file, _RECORDS)
    snap.capture(snapshot_id="snap1")
    with pytest.raises(SnapshotError, match="already exists"):
        snap.capture(snapshot_id="snap1")


def test_capture_missing_log_file_creates_empty_snapshot(dirs, snap):
    _, snap_dir = dirs
    manifest = snap.capture(snapshot_id="empty")
    assert manifest.record_count == 0


def test_restore_overwrites_log_file(dirs, snap):
    log_file, _ = dirs
    _write_records(log_file, _RECORDS)
    snap.capture(snapshot_id="snap1")
    log_file.write_text("{\"run_id\": \"z9\"}\n")
    snap.restore("snap1")
    lines = [l for l in log_file.read_text().splitlines() if l.strip()]
    assert len(lines) == len(_RECORDS)


def test_restore_returns_record_count(dirs, snap):
    log_file, _ = dirs
    _write_records(log_file, _RECORDS)
    snap.capture(snapshot_id="snap1")
    count = snap.restore("snap1")
    assert count == len(_RECORDS)


def test_restore_missing_snapshot_raises(snap):
    with pytest.raises(SnapshotError, match="not found"):
        snap.restore("nonexistent")


def test_list_snapshots_returns_all(dirs, snap):
    log_file, _ = dirs
    _write_records(log_file, _RECORDS)
    snap.capture(snapshot_id="s1")
    snap.capture(snapshot_id="s2")
    listing = snap.list_snapshots()
    ids = [m.snapshot_id for m in listing]
    assert "s1" in ids and "s2" in ids


def test_list_snapshots_empty_dir(snap):
    assert snap.list_snapshots() == []
