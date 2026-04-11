"""Tests for pipewatch.tagger_config."""

import json
import pytest

from pipewatch.run_tagger import TagIndex
from pipewatch.tagger_config import build_tag_index_from_config, load_tag_index_from_file


def test_build_returns_tag_index_instance(tmp_path):
    config = {"index_file": str(tmp_path / "idx.json")}
    result = build_tag_index_from_config(config)
    assert isinstance(result, TagIndex)


def test_build_expands_user(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    config = {"index_file": "~/idx.json"}
    result = build_tag_index_from_config(config)
    assert isinstance(result, TagIndex)


def test_build_missing_index_file_raises():
    with pytest.raises(KeyError, match="index_file"):
        build_tag_index_from_config({})


def test_build_non_string_index_file_raises():
    with pytest.raises(TypeError, match="string"):
        build_tag_index_from_config({"index_file": 42})


def test_build_empty_index_file_raises():
    with pytest.raises(ValueError, match="empty"):
        build_tag_index_from_config({"index_file": "   "})


def test_load_from_file_returns_tag_index(tmp_path):
    index_path = tmp_path / "idx.json"
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"tagger": {"index_file": str(index_path)}})
    )
    result = load_tag_index_from_file(str(config_path))
    assert isinstance(result, TagIndex)


def test_load_from_file_missing_raises():
    with pytest.raises(FileNotFoundError):
        load_tag_index_from_file("/nonexistent/config.json")
