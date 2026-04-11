"""Tests for pipewatch.annotation_config."""

import pytest

from pipewatch.annotation_config import build_annotator_from_config
from pipewatch.run_annotator import RunAnnotator


def test_build_annotator_returns_instance(tmp_path):
    log = str(tmp_path / "runs.jsonl")
    result = build_annotator_from_config({"log_file": log})
    assert isinstance(result, RunAnnotator)
    assert result.log_file == log


def test_build_annotator_expands_user(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    result = build_annotator_from_config({"log_file": "~/runs.jsonl"})
    assert not result.log_file.startswith("~")


def test_build_annotator_missing_log_file_raises():
    with pytest.raises(ValueError, match="log_file"):
        build_annotator_from_config({})


def test_build_annotator_non_string_log_file_raises():
    with pytest.raises(TypeError, match="string"):
        build_annotator_from_config({"log_file": 42})


def test_build_annotator_empty_log_file_raises():
    with pytest.raises(ValueError, match="log_file"):
        build_annotator_from_config({"log_file": ""})
