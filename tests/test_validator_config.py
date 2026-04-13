"""Tests for validator_config module."""

import json
import pytest
from pathlib import Path

from pipewatch.run_validator import RunValidator, ValidationError
from pipewatch.validator_config import build_validator_from_config, load_validator_from_file


def test_build_returns_run_validator_instance(tmp_path: Path) -> None:
    config = {"log_file": str(tmp_path / "runs.jsonl")}
    v = build_validator_from_config(config)
    assert isinstance(v, RunValidator)


def test_build_expands_user(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    config = {"log_file": "~/runs.jsonl"}
    v = build_validator_from_config(config)
    assert "~" not in str(v._log_file)


def test_build_missing_log_file_raises() -> None:
    with pytest.raises(ValidationError, match="log_file"):
        build_validator_from_config({})


def test_build_non_string_log_file_raises() -> None:
    with pytest.raises(ValidationError, match="string"):
        build_validator_from_config({"log_file": 123})


def test_build_empty_log_file_raises() -> None:
    with pytest.raises(ValidationError, match="empty"):
        build_validator_from_config({"log_file": "   "})


def test_build_sets_max_duration(tmp_path: Path) -> None:
    config = {"log_file": str(tmp_path / "r.jsonl"), "max_duration_seconds": 120}
    v = build_validator_from_config(config)
    assert v._rules["max_duration_seconds"] == 120


def test_build_invalid_max_duration_raises(tmp_path: Path) -> None:
    config = {"log_file": str(tmp_path / "r.jsonl"), "max_duration_seconds": -5}
    with pytest.raises(ValidationError, match="positive"):
        build_validator_from_config(config)


def test_build_sets_required_fields(tmp_path: Path) -> None:
    config = {"log_file": str(tmp_path / "r.jsonl"), "required_fields": ["env"]}
    v = build_validator_from_config(config)
    assert v._rules["required_fields"] == ["env"]


def test_build_invalid_required_fields_raises(tmp_path: Path) -> None:
    config = {"log_file": str(tmp_path / "r.jsonl"), "required_fields": "env"}
    with pytest.raises(ValidationError, match="list"):
        build_validator_from_config(config)


def test_load_from_file(tmp_path: Path) -> None:
    log_path = tmp_path / "runs.jsonl"
    config_path = tmp_path / "validator.json"
    config_path.write_text(json.dumps({"log_file": str(log_path)}))
    v = load_validator_from_file(str(config_path))
    assert isinstance(v, RunValidator)
