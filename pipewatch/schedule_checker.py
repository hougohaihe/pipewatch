"""Schedule checker: detects overdue pipeline runs based on schedule config."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from croniter import croniter

from pipewatch.schedule_config import ScheduleConfig
from pipewatch.run_filter import RunFilter


class ScheduleChecker:
    """Checks whether scheduled pipelines have run recently enough."""

    def __init__(self, run_filter: RunFilter, now: datetime | None = None):
        self._filter = run_filter
        self._now = now or datetime.now(tz=timezone.utc)

    def is_overdue(self, schedule: ScheduleConfig) -> bool:
        """Return True if the pipeline has missed its last scheduled run."""
        if not schedule.enabled:
            return False

        runs = self._filter.by_pipeline(schedule.pipeline)
        last_run_time = self._last_run_time(runs)
        expected = self._last_expected_run(schedule.cron)

        if expected is None:
            return False
        if last_run_time is None:
            return True
        return last_run_time < expected

    def overdue_schedules(self, schedules: list[ScheduleConfig]) -> list[ScheduleConfig]:
        """Return all schedules that are currently overdue."""
        return [s for s in schedules if self.is_overdue(s)]

    def _last_run_time(self, runs: list[dict[str, Any]]) -> datetime | None:
        if not runs:
            return None
        timestamps = []
        for r in runs:
            ts = r.get("started_at")
            if ts:
                try:
                    timestamps.append(datetime.fromisoformat(ts).replace(tzinfo=timezone.utc))
                except ValueError:
                    continue
        return max(timestamps) if timestamps else None

    def _last_expected_run(self, cron: str) -> datetime | None:
        try:
            it = croniter(cron, self._now)
            return it.get_prev(datetime).replace(tzinfo=timezone.utc)
        except Exception:
            return None
