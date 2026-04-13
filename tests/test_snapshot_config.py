"""Tests for pipewatch.snapshot_config."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_snapshot import RunSnapshot
from pipewatch.snapshot_config import build_snapshot_from_config, load_snapshot_from_file


def test_build_returns_run_snapshot_instance(tmp_path):
    config = {"log_file": str(tmp_path / "runs.jsonl"), "snapshot_dir": str(tmp_path / "snaps")}
    result = build_snapshot_from_config(config)
    assert isinstance(result, RunSnapshot)


def test_build_expands_user(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    config = {"log_file": "~/runs.jsonl", "snapshot_dir": "~/snaps"}
    result = build_snapshot_from_config(config)
    assert "~" not in str(result.log_file)
    assert "~" not in str(result.snapshot_dir)


def test_build_missing_log_file_raises():
    with pytest.raises(KeyError, match="log_file"):
        build_snapshot_from_config({"snapshot_dir": "/tmp/snaps"})


def test_build_missing_snapshot_dir_raises():
    with pytest.raises(KeyError, match="snapshot_dir"):
        build_snapshot_from_config({"log_file": "/tmp/runs.jsonl"})


def test_build_empty_log_file_raises():
    with pytest.raises(ValueError, match="log_file"):
        build_snapshot_from_config({"log_file": "  ", "snapshot_dir": "/tmp/snaps"})


def test_build_empty_snapshot_dir_raises():
    with pytest.raises(ValueError, match="snapshot_dir"):
        build_snapshot_from_config({"log_file": "/tmp/runs.jsonl", "snapshot_dir": ""})


def test_load_from_file_returns_instance(tmp_path):
    config = {"log_file": str(tmp_path / "runs.jsonl"), "snapshot_dir": str(tmp_path / "snaps")}
    cfg_file = tmp_path / "snap_config.json"
    cfg_file.write_text(json.dumps(config))
    result = load_snapshot_from_file(str(cfg_file))
    assert isinstance(result, RunSnapshot)


def test_load_from_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_snapshot_from_file(str(tmp_path / "no_such_file.json"))
