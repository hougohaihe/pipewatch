"""Tests for pipewatch.archiver_config."""

import json
from pathlib import Path

import pytest

from pipewatch.archiver_config import build_archiver_from_config, load_archiver_from_file
from pipewatch.run_archiver import RunArchiver


def test_build_returns_run_archiver_instance():
    config = {"log_file": "/tmp/runs.jsonl", "archive_dir": "/tmp/archives"}
    result = build_archiver_from_config(config)
    assert isinstance(result, RunArchiver)


def test_build_expands_user(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    config = {"log_file": "~/runs.jsonl", "archive_dir": "~/archives"}
    result = build_archiver_from_config(config)
    assert "~" not in str(result.log_file)
    assert "~" not in str(result.archive_dir)


def test_build_missing_log_file_raises():
    with pytest.raises(KeyError, match="log_file"):
        build_archiver_from_config({"archive_dir": "/tmp/archives"})


def test_build_missing_archive_dir_raises():
    with pytest.raises(KeyError, match="archive_dir"):
        build_archiver_from_config({"log_file": "/tmp/runs.jsonl"})


def test_build_empty_log_file_raises():
    with pytest.raises(ValueError, match="log_file"):
        build_archiver_from_config({"log_file": "  ", "archive_dir": "/tmp/archives"})


def test_build_empty_archive_dir_raises():
    with pytest.raises(ValueError, match="archive_dir"):
        build_archiver_from_config({"log_file": "/tmp/runs.jsonl", "archive_dir": ""})


def test_load_from_file_returns_instance(tmp_path):
    config = {"log_file": "/tmp/runs.jsonl", "archive_dir": "/tmp/archives"}
    config_file = tmp_path / "archiver.json"
    config_file.write_text(json.dumps(config))
    result = load_archiver_from_file(str(config_file))
    assert isinstance(result, RunArchiver)


def test_load_from_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        load_archiver_from_file("/nonexistent/archiver.json")
