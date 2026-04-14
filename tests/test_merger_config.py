"""Tests for pipewatch.merger_config."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.merger_config import build_merger_from_config, load_merger_from_file
from pipewatch.run_merger import RunMerger


def test_build_returns_run_merger_instance():
    cfg = {"output_file": "/tmp/merged.jsonl"}
    result = build_merger_from_config(cfg)
    assert isinstance(result, RunMerger)


def test_build_expands_user(tmp_path):
    cfg = {"output_file": "~/merged.jsonl"}
    result = build_merger_from_config(cfg)
    assert isinstance(result, RunMerger)


def test_build_missing_output_file_raises():
    with pytest.raises(KeyError):
        build_merger_from_config({})


def test_build_non_string_output_file_raises():
    with pytest.raises(TypeError):
        build_merger_from_config({"output_file": 123})


def test_build_empty_output_file_raises():
    with pytest.raises(ValueError):
        build_merger_from_config({"output_file": "   "})


def test_load_from_file_returns_instance(tmp_path):
    cfg_file = tmp_path / "merger.json"
    cfg_file.write_text(json.dumps({"output_file": str(tmp_path / "out.jsonl")}))
    result = load_merger_from_file(str(cfg_file))
    assert isinstance(result, RunMerger)
