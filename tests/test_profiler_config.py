from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.profiler_config import build_profiler_from_config, load_profiler_from_file
from pipewatch.run_profiler import RunProfiler


def test_build_returns_run_profiler_instance(tmp_path):
    config = {"log_file": str(tmp_path / "runs.jsonl")}
    result = build_profiler_from_config(config)
    assert isinstance(result, RunProfiler)


def test_build_expands_user(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    config = {"log_file": "~/runs.jsonl"}
    profiler = build_profiler_from_config(config)
    assert "~" not in str(profiler._log_file)


def test_build_missing_log_file_raises():
    with pytest.raises(KeyError, match="log_file"):
        build_profiler_from_config({})


def test_build_non_string_log_file_raises():
    with pytest.raises(TypeError, match="string"):
        build_profiler_from_config({"log_file": 42})


def test_build_empty_log_file_raises():
    with pytest.raises(ValueError, match="empty"):
        build_profiler_from_config({"log_file": "   "})


def test_load_from_file_returns_instance(tmp_path):
    config = {"log_file": str(tmp_path / "runs.jsonl")}
    config_path = tmp_path / "profiler.json"
    config_path.write_text(json.dumps(config))
    result = load_profiler_from_file(str(config_path))
    assert isinstance(result, RunProfiler)


def test_load_from_file_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_profiler_from_file(str(tmp_path / "no_such_file.json"))
