from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.sampler_config import build_sampler_from_config, load_sampler_from_file
from pipewatch.run_sampler import RunSampler


def test_build_returns_run_sampler_instance(tmp_path):
    cfg = {"log_file": str(tmp_path / "runs.jsonl")}
    result = build_sampler_from_config(cfg)
    assert isinstance(result, RunSampler)


def test_build_expands_user(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg = {"log_file": "~/runs.jsonl"}
    result = build_sampler_from_config(cfg)
    assert isinstance(result, RunSampler)


def test_build_missing_log_file_raises():
    with pytest.raises(KeyError, match="log_file"):
        build_sampler_from_config({})


def test_build_non_string_log_file_raises():
    with pytest.raises(TypeError, match="log_file"):
        build_sampler_from_config({"log_file": 123})


def test_build_empty_log_file_raises():
    with pytest.raises(ValueError, match="log_file"):
        build_sampler_from_config({"log_file": "   "})


def test_load_from_file_returns_instance(tmp_path):
    cfg = {"log_file": str(tmp_path / "runs.jsonl")}
    config_path = tmp_path / "sampler.json"
    config_path.write_text(json.dumps(cfg))
    result = load_sampler_from_file(str(config_path))
    assert isinstance(result, RunSampler)


def test_load_from_file_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_sampler_from_file(str(tmp_path / "no_such_file.json"))
