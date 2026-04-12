"""Tests for pipewatch.watcher_config."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_watcher import RunWatcher, WatcherError
from pipewatch.watcher_config import build_watcher_from_config, load_watcher_from_file


def test_build_returns_run_watcher_instance() -> None:
    w = build_watcher_from_config({"log_file": "/tmp/runs.jsonl"})
    assert isinstance(w, RunWatcher)


def test_build_expands_user() -> None:
    w = build_watcher_from_config({"log_file": "~/runs.jsonl"})
    assert "~" not in str(w.log_file)


def test_build_sets_default_poll_interval() -> None:
    w = build_watcher_from_config({"log_file": "/tmp/runs.jsonl"})
    assert w.poll_interval == 1.0


def test_build_sets_custom_poll_interval() -> None:
    w = build_watcher_from_config({"log_file": "/tmp/runs.jsonl", "poll_interval": 5})
    assert w.poll_interval == 5.0


def test_build_missing_log_file_raises() -> None:
    with pytest.raises(WatcherError, match="log_file"):
        build_watcher_from_config({})


def test_build_empty_log_file_raises() -> None:
    with pytest.raises(WatcherError, match="log_file"):
        build_watcher_from_config({"log_file": ""})


def test_build_non_string_log_file_raises() -> None:
    with pytest.raises(WatcherError, match="string"):
        build_watcher_from_config({"log_file": 123})


def test_build_invalid_poll_interval_raises() -> None:
    with pytest.raises(WatcherError, match="number"):
        build_watcher_from_config({"log_file": "/tmp/r.jsonl", "poll_interval": "fast"})


def test_build_zero_poll_interval_raises() -> None:
    with pytest.raises(WatcherError, match="positive"):
        build_watcher_from_config({"log_file": "/tmp/r.jsonl", "poll_interval": 0})


def test_load_from_file_returns_watcher(tmp_path: Path) -> None:
    cfg = {"log_file": "/tmp/runs.jsonl", "poll_interval": 2.0}
    cfg_file = tmp_path / "watcher.json"
    cfg_file.write_text(json.dumps(cfg), encoding="utf-8")

    w = load_watcher_from_file(str(cfg_file))
    assert isinstance(w, RunWatcher)
    assert w.poll_interval == 2.0


def test_load_from_missing_file_raises() -> None:
    with pytest.raises(WatcherError, match="not found"):
        load_watcher_from_file("/nonexistent/watcher.json")
