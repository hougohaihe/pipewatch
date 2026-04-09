"""Schedule configuration loader and validator for pipewatch."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ScheduleConfig:
    """Holds parsed schedule configuration for a pipeline."""

    def __init__(self, pipeline: str, cron: str, timeout_seconds: int = 3600, enabled: bool = True):
        self.pipeline = pipeline
        self.cron = cron
        self.timeout_seconds = timeout_seconds
        self.enabled = enabled

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "cron": self.cron,
            "timeout_seconds": self.timeout_seconds,
            "enabled": self.enabled,
        }

    def __repr__(self) -> str:
        return f"ScheduleConfig(pipeline={self.pipeline!r}, cron={self.cron!r}, enabled={self.enabled})"


def build_schedule_from_config(config: dict[str, Any]) -> ScheduleConfig:
    """Build a ScheduleConfig from a raw config dict."""
    pipeline = config.get("pipeline")
    if not pipeline or not isinstance(pipeline, str):
        raise ValueError("schedule config must include a non-empty 'pipeline' string")

    cron = config.get("cron")
    if not cron or not isinstance(cron, str):
        raise ValueError("schedule config must include a non-empty 'cron' string")

    timeout = config.get("timeout_seconds", 3600)
    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("'timeout_seconds' must be a positive integer")

    enabled = config.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError("'enabled' must be a boolean")

    return ScheduleConfig(pipeline=pipeline, cron=cron, timeout_seconds=timeout, enabled=enabled)


def load_schedules_from_file(path: str | Path) -> list[ScheduleConfig]:
    """Load a list of ScheduleConfig objects from a JSON file."""
    data = json.loads(Path(path).read_text())
    if not isinstance(data, list):
        raise ValueError("schedule config file must contain a JSON array")
    return [build_schedule_from_config(entry) for entry in data]
