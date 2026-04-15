from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.highlighter_config import build_highlighter_from_config, load_highlighter_from_file
from pipewatch.run_highlighter import RunHighlighter


def test_build_returns_run_highlighter_instance(tmp_path: Path) -> None:
    cfg = {"log_file": str(tmp_path / "runs.jsonl")}
    result = build_highlighter_from_config(cfg)
    assert isinstance(result, RunHighlighter)


def test_build_expands_user(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg = {"log_file": "~/runs.jsonl"}
    result = build_highlighter_from_config(cfg)
    assert isinstance(result, RunHighlighter)


def test_build_missing_log_file_raises() -> None:
    with pytest.raises(ValueError, match="log_file"):
        build_highlighter_from_config({})


def test_build_non_string_log_file_raises() -> None:
    with pytest.raises(TypeError, match="string"):
        build_highlighter_from_config({"log_file": 123})


def test_build_empty_log_file_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        build_highlighter_from_config({"log_file": "   "})


def test_load_from_file(tmp_path: Path) -> None:
    log_path = tmp_path / "runs.jsonl"
    config_path = tmp_path / "highlighter.json"
    config_path.write_text(json.dumps({"log_file": str(log_path)}))
    result = load_highlighter_from_file(str(config_path))
    assert isinstance(result, RunHighlighter)
