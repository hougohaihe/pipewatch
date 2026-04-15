from __future__ import annotations

import json
import os

import pytest

from pipewatch.anomaly_config import build_anomaly_from_config
from pipewatch.run_anomaly import AnomalyError, RunAnomaly


def test_build_returns_run_anomaly_instance(tmp_path):
    cfg = {"log_file": str(tmp_path / "runs.log")}
    result = build_anomaly_from_config(cfg)
    assert isinstance(result, RunAnomaly)


def test_build_expands_user(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg = {"log_file": "~/runs.log"}
    result = build_anomaly_from_config(cfg)
    assert not result.log_file.startswith("~")


def test_build_missing_log_file_raises():
    with pytest.raises(AnomalyError, match="log_file"):
        build_anomaly_from_config({})


def test_build_non_string_log_file_raises():
    with pytest.raises(AnomalyError, match="log_file"):
        build_anomaly_from_config({"log_file": 123})


def test_build_empty_log_file_raises():
    with pytest.raises(AnomalyError):
        build_anomaly_from_config({"log_file": "   "})


def test_build_custom_z_threshold(tmp_path):
    cfg = {"log_file": str(tmp_path / "runs.log"), "z_threshold": 3.0}
    result = build_anomaly_from_config(cfg)
    assert result.z_threshold == 3.0


def test_build_invalid_z_threshold_raises(tmp_path):
    cfg = {"log_file": str(tmp_path / "runs.log"), "z_threshold": -1}
    with pytest.raises(AnomalyError, match="z_threshold"):
        build_anomaly_from_config(cfg)


def test_build_non_numeric_z_threshold_raises(tmp_path):
    cfg = {"log_file": str(tmp_path / "runs.log"), "z_threshold": "high"}
    with pytest.raises(AnomalyError, match="z_threshold"):
        build_anomaly_from_config(cfg)
