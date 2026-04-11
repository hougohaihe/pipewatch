"""Tests for pipewatch.replay_config."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.replay_config import build_replay_from_config, load_replay_from_file
from pipewatch.run_replay import RunReplay


def test_build_returns_run_replay_instance(tmp_path: Path) -> None:
    log = tmp_path / "runs.jsonl"
    log.touch()
    instance = build_replay_from_config({"log_file": str(log)})
    assert isinstance(instance, RunReplay)


def test_build_expands_user(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    log = tmp_path / "runs.jsonl"
    log.touch()
    instance = build_replay_from_config({"log_file": "~/runs.jsonl"})
    assert isinstance(instance, RunReplay)


def test_build_missing_log_file_raises() -> None:
    with pytest.raises(KeyError, match="log_file"):
        build_replay_from_config({})


def test_build_non_string_log_file_raises() -> None:
    with pytest.raises(TypeError, match="string"):
        build_replay_from_config({"log_file": 42})


def test_build_empty_log_file_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        build_replay_from_config({"log_file": "   "})


def test_load_from_file_returns_instance(tmp_path: Path) -> None:
    log = tmp_path / "runs.jsonl"
    log.touch()
    config_file = tmp_path / "replay.json"
    config_file.write_text(json.dumps({"log_file": str(log)}))
    instance = load_replay_from_file(str(config_file))
    assert isinstance(instance, RunReplay)
