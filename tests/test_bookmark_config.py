import json
import os
import pytest

from pipewatch.bookmark_config import build_bookmark_from_config, load_bookmark_from_file
from pipewatch.run_bookmark import RunBookmark


def test_build_returns_run_bookmark_instance(tmp_path):
    config = {"bookmark_file": str(tmp_path / "bm.json")}
    result = build_bookmark_from_config(config)
    assert isinstance(result, RunBookmark)


def test_build_expands_user(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    config = {"bookmark_file": "~/bm.json"}
    result = build_bookmark_from_config(config)
    assert isinstance(result, RunBookmark)


def test_build_missing_bookmark_file_raises():
    with pytest.raises(KeyError):
        build_bookmark_from_config({})


def test_build_non_string_bookmark_file_raises():
    with pytest.raises(TypeError):
        build_bookmark_from_config({"bookmark_file": 123})


def test_build_empty_bookmark_file_raises():
    with pytest.raises(ValueError):
        build_bookmark_from_config({"bookmark_file": "   "})


def test_load_from_file(tmp_path):
    bm_path = str(tmp_path / "bm.json")
    config = {"bookmark_file": bm_path}
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))
    result = load_bookmark_from_file(str(config_file))
    assert isinstance(result, RunBookmark)
