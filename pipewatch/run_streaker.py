"""Track consecutive success/failure streaks per pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class StreakerError(Exception):
    """Raised when streak computation encounters invalid state."""


@dataclass
class PipelineStreak:
    pipeline: str
    current_streak: int
    streak_type: str  # "success" | "failure" | "none"
    longest_success_streak: int
    longest_failure_streak: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "current_streak": self.current_streak,
            "streak_type": self.streak_type,
            "longest_success_streak": self.longest_success_streak,
            "longest_failure_streak": self.longest_failure_streak,
        }


class RunStreaker:
    """Compute consecutive run streaks from a pipeline log file."""

    def __init__(self, log_file: str) -> None:
        self._log_file = Path(log_file)

    def _load_records(self) -> List[dict]:
        if not self._log_file.exists():
            return []
        records = []
        with self._log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def compute(self, pipeline: Optional[str] = None) -> Dict[str, PipelineStreak]:
        """Return a mapping of pipeline name -> PipelineStreak."""
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]

        # Group records by pipeline preserving insertion order
        grouped: Dict[str, List[str]] = {}
        for r in records:
            name = r.get("pipeline", "")
            status = r.get("status", "")
            if name:
                grouped.setdefault(name, []).append(status)

        result: Dict[str, PipelineStreak] = {}
        for name, statuses in grouped.items():
            result[name] = self._compute_streak(name, statuses)
        return result

    def _compute_streak(self, pipeline: str, statuses: List[str]) -> PipelineStreak:
        longest_success = 0
        longest_failure = 0
        cur_success = 0
        cur_failure = 0

        for status in statuses:
            if status == "success":
                cur_success += 1
                cur_failure = 0
            elif status == "failure":
                cur_failure += 1
                cur_success = 0
            else:
                cur_success = 0
                cur_failure = 0
            longest_success = max(longest_success, cur_success)
            longest_failure = max(longest_failure, cur_failure)

        if cur_success > 0:
            streak_type = "success"
            current = cur_success
        elif cur_failure > 0:
            streak_type = "failure"
            current = cur_failure
        else:
            streak_type = "none"
            current = 0

        return PipelineStreak(
            pipeline=pipeline,
            current_streak=current,
            streak_type=streak_type,
            longest_success_streak=longest_success,
            longest_failure_streak=longest_failure,
        )
