from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.aggregator_config import build_aggregator_from_config, load_aggregator_from_file
from pipewatch.run_aggregator import RunAggregator


def test_build_returns_run_aggregator_instance(tmp_path):
    config = {"log_file": str(tmp_path / "runs.log")}
    result = build_aggregator_from_config(config)
    assert isinstance(result, RunAggregator)


def test_build_expands_user(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    config = {"log_file": "~/runs.log"}
    result = build_aggregator_from_config(config)
    assert isinstance(result, RunAggregator)


def test_build_missing_log_file_raises():
    with pytest.raises(KeyError):
        build_aggregator_from_config({})


def test_build_non_string_log_file_raises():
    with pytest.raises(TypeError):
        build_aggregator_from_config({"log_file": 123})


def test_build_empty_log_file_raises():
    with pytest.raises(ValueError):
        build_aggregator_from_config({"log_file": "   "})


def test_load_from_file_returns_instance(tmp_path):
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"log_file": str(tmp_path / "runs.log")}))
    result = load_aggregator_from_file(str(config_file))
    assert isinstance(result, RunAggregator)


def test_load_from_file_with_nested_key(tmp_path):
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"aggregator": {"log_file": str(tmp_path / "runs.log")}}))
    result = load_aggregator_from_file(str(config_file))
    assert isinstance(result, RunAggregator)


def test_load_from_file_missing_raises():
    with pytest.raises(FileNotFoundError):
        load_aggregator_from_file("/nonexistent/path/config.json")
