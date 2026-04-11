from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_summarizer import RunSummarizer
from pipewatch.summarizer_config import build_summarizer_from_config, load_summarizer_from_file


def test_build_returns_run_summarizer_instance(tmp_path):
    log = tmp_path / "runs.log"
    result = build_summarizer_from_config({"log_file": str(log)})
    assert isinstance(result, RunSummarizer)


def test_build_expands_user():
    result = build_summarizer_from_config({"log_file": "~/runs.log"})
    assert "~" not in str(result.log_file)


def test_build_missing_log_file_raises():
    with pytest.raises(KeyError, match="log_file"):
        build_summarizer_from_config({})


def test_build_non_string_log_file_raises():
    with pytest.raises(TypeError, match="string"):
        build_summarizer_from_config({"log_file": 42})


def test_build_empty_log_file_raises():
    with pytest.raises(ValueError, match="empty"):
        build_summarizer_from_config({"log_file": "   "})


def test_load_from_file(tmp_path):
    log = tmp_path / "runs.log"
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"log_file": str(log)}))
    result = load_summarizer_from_file(str(config_path))
    assert isinstance(result, RunSummarizer)


def test_load_from_file_nested_key(tmp_path):
    log = tmp_path / "runs.log"
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"summarizer": {"log_file": str(log)}}))
    result = load_summarizer_from_file(str(config_path))
    assert isinstance(result, RunSummarizer)


def test_load_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_summarizer_from_file(str(tmp_path / "no_such.json"))
