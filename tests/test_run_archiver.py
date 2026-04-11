"""Tests for pipewatch.run_archiver."""

import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.run_archiver import ArchiveError, RunArchiver


def _make_record(pipeline: str, days_ago: int = 0) -> dict:
    ts = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return {"run_id": f"run-{pipeline}-{days_ago}", "pipeline": pipeline, "started_at": ts, "status": "success"}


def _write_records(path: Path, records: list) -> None:
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


@pytest.fixture
def dirs(tmp_path):
    log_file = tmp_path / "runs.jsonl"
    archive_dir = tmp_path / "archives"
    return log_file, archive_dir


@pytest.fixture
def archiver(dirs):
    log_file, archive_dir = dirs
    return RunArchiver(str(log_file), str(archive_dir))


def test_archive_creates_gz_file(dirs, archiver):
    log_file, archive_dir = dirs
    _write_records(log_file, [_make_record("etl", days_ago=5)])
    result = archiver.archive()
    assert result.suffix == ".gz"
    assert result.exists()


def test_archive_removes_records_from_log(dirs, archiver):
    log_file, archive_dir = dirs
    _write_records(log_file, [_make_record("etl", days_ago=5)])
    archiver.archive()
    assert log_file.read_text().strip() == ""


def test_archive_filters_by_pipeline(dirs, archiver):
    log_file, archive_dir = dirs
    records = [_make_record("etl", 5), _make_record("ingest", 5)]
    _write_records(log_file, records)
    archiver.archive(pipeline="etl")
    remaining = [json.loads(l) for l in log_file.read_text().splitlines() if l.strip()]
    assert len(remaining) == 1
    assert remaining[0]["pipeline"] == "ingest"


def test_archive_filters_by_before_days(dirs, archiver):
    log_file, archive_dir = dirs
    records = [_make_record("etl", days_ago=10), _make_record("etl", days_ago=1)]
    _write_records(log_file, records)
    archiver.archive(before_days=5)
    remaining = [json.loads(l) for l in log_file.read_text().splitlines() if l.strip()]
    assert len(remaining) == 1


def test_archive_gz_contains_correct_records(dirs, archiver):
    log_file, archive_dir = dirs
    rec = _make_record("etl", days_ago=5)
    _write_records(log_file, [rec])
    archive_path = archiver.archive()
    with gzip.open(archive_path, "rt") as gz:
        lines = [l.strip() for l in gz if l.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0])["run_id"] == rec["run_id"]


def test_archive_raises_when_no_records(dirs, archiver):
    log_file, _ = dirs
    log_file.write_text("")
    with pytest.raises(ArchiveError):
        archiver.archive()


def test_archive_raises_when_no_match(dirs, archiver):
    log_file, _ = dirs
    _write_records(log_file, [_make_record("etl", days_ago=1)])
    with pytest.raises(ArchiveError):
        archiver.archive(pipeline="nonexistent")


def test_list_archives_returns_sorted(dirs, archiver):
    log_file, archive_dir = dirs
    archive_dir.mkdir()
    (archive_dir / "archive_b.jsonl.gz").write_bytes(b"")
    (archive_dir / "archive_a.jsonl.gz").write_bytes(b"")
    result = archiver.list_archives()
    names = [p.name for p in result]
    assert names == sorted(names)


def test_restore_appends_records(dirs, archiver):
    log_file, archive_dir = dirs
    rec = _make_record("etl", days_ago=5)
    _write_records(log_file, [rec])
    archive_path = archiver.archive()
    count = archiver.restore(str(archive_path))
    assert count == 1
    lines = [l for l in log_file.read_text().splitlines() if l.strip()]
    assert len(lines) == 1


def test_restore_raises_for_missing_file(archiver):
    with pytest.raises(ArchiveError):
        archiver.restore("/nonexistent/path.jsonl.gz")
