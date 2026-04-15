"""Tests for RunPinner and pinner_config."""

from __future__ import annotations

import json
import pytest

from pipewatch.run_pinner import PinEntry, PinError, RunPinner
from pipewatch.pinner_config import build_pinner_from_config, load_pinner_from_file


@pytest.fixture
def pin_file(tmp_path):
    return tmp_path / "pins.json"


@pytest.fixture
def pinner(pin_file):
    return RunPinner(pin_file=str(pin_file))


def test_pin_creates_entry(pinner):
    entry = pinner.pin("run-001", "baseline")
    assert isinstance(entry, PinEntry)
    assert entry.run_id == "run-001"
    assert entry.label == "baseline"


def test_pin_persists_to_disk(pinner, pin_file):
    pinner.pin("run-001", "baseline", pipeline="etl")
    data = json.loads(pin_file.read_text())
    assert "run-001" in data
    assert data["run-001"]["label"] == "baseline"
    assert data["run-001"]["pipeline"] == "etl"


def test_pin_rejects_empty_run_id(pinner):
    with pytest.raises(PinError, match="run_id"):
        pinner.pin("", "label")


def test_pin_rejects_empty_label(pinner):
    with pytest.raises(PinError, match="label"):
        pinner.pin("run-001", "")


def test_unpin_removes_entry(pinner):
    pinner.pin("run-001", "baseline")
    result = pinner.unpin("run-001")
    assert result is True
    assert pinner.get("run-001") is None


def test_unpin_returns_false_for_missing(pinner):
    assert pinner.unpin("nonexistent") is False


def test_get_returns_correct_entry(pinner):
    pinner.pin("run-002", "checkpoint", pipeline="loader")
    entry = pinner.get("run-002")
    assert entry is not None
    assert entry.pipeline == "loader"


def test_all_returns_all_pins(pinner):
    pinner.pin("run-001", "first")
    pinner.pin("run-002", "second")
    assert len(pinner.all()) == 2


def test_by_pipeline_filters_correctly(pinner):
    pinner.pin("run-001", "a", pipeline="etl")
    pinner.pin("run-002", "b", pipeline="loader")
    pinner.pin("run-003", "c", pipeline="etl")
    results = pinner.by_pipeline("etl")
    assert len(results) == 2
    assert all(p.pipeline == "etl" for p in results)


def test_load_from_existing_file(pin_file):
    data = {"run-abc": {"run_id": "run-abc", "label": "saved", "pipeline": None}}
    pin_file.write_text(json.dumps(data))
    p = RunPinner(str(pin_file))
    assert p.get("run-abc") is not None


def test_build_pinner_from_config(pin_file):
    pinner = build_pinner_from_config({"pin_file": str(pin_file)})
    assert isinstance(pinner, RunPinner)


def test_build_pinner_missing_pin_file_raises():
    with pytest.raises(PinError, match="pin_file"):
        build_pinner_from_config({})


def test_build_pinner_non_string_raises():
    with pytest.raises(PinError, match="string"):
        build_pinner_from_config({"pin_file": 123})


def test_build_pinner_empty_string_raises():
    with pytest.raises(PinError, match="empty"):
        build_pinner_from_config({"pin_file": "   "})


def test_load_pinner_from_file(tmp_path, pin_file):
    config_file = tmp_path / "pinner_config.json"
    config_file.write_text(json.dumps({"pin_file": str(pin_file)}))
    pinner = load_pinner_from_file(str(config_file))
    assert isinstance(pinner, RunPinner)


def test_load_pinner_missing_config_raises(tmp_path):
    with pytest.raises(PinError, match="not found"):
        load_pinner_from_file(str(tmp_path / "missing.json"))
