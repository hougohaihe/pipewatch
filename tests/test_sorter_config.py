from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_sorter import RunSorter
from pipewatch.sorter_config import build_sorter_from_config, load_sorter_from_file


def test_build_returns_run_sorter_instance(tmp_path: Path) -> None:
    log = tmp_path / "runs.log"
    result = build_sorter_from_config({"log_file": str(log)})
    assert isinstance(result, RunSorter)


def test_build_expands_user(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    result = build_sorter_from_config({"log_file": "~/runs.log"})
    assert isinstance(result, RunSorter)


def test_build_missing_log_file_raises() -> None:
    with pytest.raises(KeyError, match="log_file"):
        build_sorter_from_config({})


def test_build_non_string_log_file_raises() -> None:
    with pytest.raises(TypeError):
        build_sorter_from_config({"log_file": 42})


def test_build_empty_log_file_raises() -> None:
    with pytest.raises(ValueError):
        build_sorter_from_config({"log_file": "   "})


def test_load_from_file_returns_instance(tmp_path: Path) -> None:
    log = tmp_path / "runs.log"
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"log_file": str(log)}))
    result = load_sorter_from_file(str(config_file))
    assert isinstance(result, RunSorter)


def test_load_from_file_with_sorter_key(tmp_path: Path) -> None:
    log = tmp_path / "runs.log"
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"sorter": {"log_file": str(log)}}))
    result = load_sorter_from_file(str(config_file))
    assert isinstance(result, RunSorter)
