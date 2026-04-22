from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.run_watchdog import RunWatchdog, WatchdogAlert, WatchdogError
from pipewatch.watchdog_config import build_watchdog_from_config, load_watchdog_from_file


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.jsonl"


@pytest.fixture
def watchdog(log_file):
    return RunWatchdog(str(log_file), default_threshold_seconds=60.0)


def test_check_returns_empty_for_missing_file(watchdog):
    alerts = watchdog.check()
    assert alerts == []


def test_check_returns_watchdog_alert_instances(log_file, watchdog):
    _write_records(log_file, [
        {"pipeline": "etl", "run_id": "abc", "duration_seconds": 120.0},
    ])
    alerts = watchdog.check()
    assert len(alerts) == 1
    assert isinstance(alerts[0], WatchdogAlert)


def test_check_no_alert_when_under_threshold(log_file, watchdog):
    _write_records(log_file, [
        {"pipeline": "etl", "run_id": "abc", "duration_seconds": 30.0},
    ])
    alerts = watchdog.check()
    assert alerts == []


def test_check_uses_per_pipeline_threshold(log_file, watchdog):
    _write_records(log_file, [
        {"pipeline": "slow_pipe", "run_id": "r1", "duration_seconds": 200.0},
        {"pipeline": "fast_pipe", "run_id": "r2", "duration_seconds": 200.0},
    ])
    alerts = watchdog.check(thresholds={"slow_pipe": 300.0, "fast_pipe": 100.0})
    assert len(alerts) == 1
    assert alerts[0].pipeline == "fast_pipe"


def test_check_skips_records_without_duration(log_file, watchdog):
    _write_records(log_file, [
        {"pipeline": "etl", "run_id": "abc"},
    ])
    alerts = watchdog.check()
    assert alerts == []


def test_alert_to_dict_has_expected_keys(log_file, watchdog):
    _write_records(log_file, [
        {"pipeline": "etl", "run_id": "abc", "duration_seconds": 120.0},
    ])
    alert = watchdog.check()[0]
    d = alert.to_dict()
    assert "pipeline" in d
    assert "run_id" in d
    assert "reason" in d
    assert "duration_seconds" in d
    assert "threshold_seconds" in d
    assert "timestamp" in d


def test_check_pipeline_filters_correctly(log_file, watchdog):
    _write_records(log_file, [
        {"pipeline": "etl", "run_id": "r1", "duration_seconds": 200.0},
        {"pipeline": "other", "run_id": "r2", "duration_seconds": 200.0},
    ])
    alerts = watchdog.check_pipeline("etl")
    assert all(a.pipeline == "etl" for a in alerts)


def test_invalid_log_file_raises():
    with pytest.raises(WatchdogError):
        RunWatchdog(log_file="", default_threshold_seconds=60.0)


def test_invalid_threshold_raises(tmp_path):
    with pytest.raises(WatchdogError):
        RunWatchdog(log_file=str(tmp_path / "r.jsonl"), default_threshold_seconds=-1.0)


def test_build_watchdog_from_config_returns_instance(tmp_path):
    cfg = {"log_file": str(tmp_path / "r.jsonl"), "default_threshold_seconds": 120.0}
    wd = build_watchdog_from_config(cfg)
    assert isinstance(wd, RunWatchdog)


def test_build_watchdog_missing_log_file_raises():
    with pytest.raises(WatchdogError):
        build_watchdog_from_config({})


def test_build_watchdog_expands_user(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg = {"log_file": "~/runs.jsonl"}
    wd = build_watchdog_from_config(cfg)
    assert "~" not in wd.log_file.as_posix()


def test_load_watchdog_from_file(tmp_path):
    log = tmp_path / "runs.jsonl"
    cfg_path = tmp_path / "watchdog.json"
    cfg_path.write_text(json.dumps({"log_file": str(log), "default_threshold_seconds": 90.0}))
    wd = load_watchdog_from_file(str(cfg_path))
    assert isinstance(wd, RunWatchdog)
    assert wd.default_threshold_seconds == 90.0
