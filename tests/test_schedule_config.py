"""Tests for pipewatch.schedule_config."""

import json
import pytest
from pathlib import Path

from pipewatch.schedule_config import ScheduleConfig, build_schedule_from_config, load_schedules_from_file


def test_build_schedule_basic():
    cfg = build_schedule_from_config({"pipeline": "etl", "cron": "0 * * * *"})
    assert cfg.pipeline == "etl"
    assert cfg.cron == "0 * * * *"
    assert cfg.timeout_seconds == 3600
    assert cfg.enabled is True


def test_build_schedule_custom_fields():
    cfg = build_schedule_from_config(
        {"pipeline": "sync", "cron": "*/5 * * * *", "timeout_seconds": 300, "enabled": False}
    )
    assert cfg.timeout_seconds == 300
    assert cfg.enabled is False


def test_build_schedule_missing_pipeline_raises():
    with pytest.raises(ValueError, match="pipeline"):
        build_schedule_from_config({"cron": "0 * * * *"})


def test_build_schedule_missing_cron_raises():
    with pytest.raises(ValueError, match="cron"):
        build_schedule_from_config({"pipeline": "etl"})


def test_build_schedule_invalid_timeout_raises():
    with pytest.raises(ValueError, match="timeout_seconds"):
        build_schedule_from_config({"pipeline": "etl", "cron": "0 * * * *", "timeout_seconds": -1})


def test_build_schedule_invalid_enabled_raises():
    with pytest.raises(ValueError, match="enabled"):
        build_schedule_from_config({"pipeline": "etl", "cron": "0 * * * *", "enabled": "yes"})


def test_to_dict_roundtrip():
    data = {"pipeline": "etl", "cron": "0 6 * * *", "timeout_seconds": 600, "enabled": True}
    cfg = build_schedule_from_config(data)
    assert cfg.to_dict() == data


def test_load_schedules_from_file(tmp_path: Path):
    schedules = [
        {"pipeline": "etl", "cron": "0 * * * *"},
        {"pipeline": "sync", "cron": "*/15 * * * *", "timeout_seconds": 120, "enabled": False},
    ]
    f = tmp_path / "schedules.json"
    f.write_text(json.dumps(schedules))
    result = load_schedules_from_file(f)
    assert len(result) == 2
    assert result[0].pipeline == "etl"
    assert result[1].enabled is False


def test_load_schedules_non_list_raises(tmp_path: Path):
    f = tmp_path / "bad.json"
    f.write_text(json.dumps({"pipeline": "etl", "cron": "0 * * * *"}))
    with pytest.raises(ValueError, match="array"):
        load_schedules_from_file(f)


def test_repr_contains_pipeline():
    cfg = ScheduleConfig(pipeline="my_pipe", cron="0 * * * *")
    assert "my_pipe" in repr(cfg)
