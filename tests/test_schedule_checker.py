"""Tests for pipewatch.schedule_checker."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.schedule_config import ScheduleConfig
from pipewatch.schedule_checker import ScheduleChecker


def _make_checker(runs: list[dict], now: datetime | None = None) -> ScheduleChecker:
    mock_filter = MagicMock()
    mock_filter.by_pipeline.return_value = runs
    now = now or datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return ScheduleChecker(run_filter=mock_filter, now=now)


def _schedule(cron: str = "0 * * * *", enabled: bool = True) -> ScheduleConfig:
    return ScheduleConfig(pipeline="etl", cron=cron, enabled=enabled)


def test_overdue_when_no_runs():
    checker = _make_checker(runs=[])
    assert checker.is_overdue(_schedule()) is True


def test_not_overdue_when_recently_run():
    # cron every hour; last run was 30 min ago relative to now
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    runs = [{"started_at": "2024-06-01T11:45:00"}]  # 15 min after last tick
    checker = _make_checker(runs=runs, now=now)
    assert checker.is_overdue(_schedule(cron="0 * * * *")) is False


def test_overdue_when_last_run_before_last_tick():
    now = datetime(2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
    runs = [{"started_at": "2024-06-01T10:00:00"}]  # before 12:00 tick
    checker = _make_checker(runs=runs, now=now)
    assert checker.is_overdue(_schedule(cron="0 * * * *")) is True


def test_disabled_schedule_never_overdue():
    checker = _make_checker(runs=[])
    assert checker.is_overdue(_schedule(enabled=False)) is False


def test_overdue_schedules_returns_subset():
    now = datetime(2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
    mock_filter = MagicMock()

    def side_effect(pipeline):
        if pipeline == "late":
            return [{"started_at": "2024-06-01T10:00:00"}]
        return [{"started_at": "2024-06-01T12:05:00"}]

    mock_filter.by_pipeline.side_effect = side_effect
    checker = ScheduleChecker(run_filter=mock_filter, now=now)

    schedules = [
        ScheduleConfig(pipeline="late", cron="0 * * * *"),
        ScheduleConfig(pipeline="ontime", cron="0 * * * *"),
    ]
    overdue = checker.overdue_schedules(schedules)
    assert len(overdue) == 1
    assert overdue[0].pipeline == "late"


def test_invalid_cron_not_overdue():
    checker = _make_checker(runs=[])
    bad = ScheduleConfig(pipeline="etl", cron="not-a-cron")
    assert checker.is_overdue(bad) is False


def test_run_with_invalid_timestamp_ignored():
    now = datetime(2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
    runs = [{"started_at": "not-a-date"}, {"started_at": "2024-06-01T12:05:00"}]
    checker = _make_checker(runs=runs, now=now)
    assert checker.is_overdue(_schedule(cron="0 * * * *")) is False
