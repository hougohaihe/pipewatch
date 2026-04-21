from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class QuotaError(Exception):
    """Raised when quota operations fail."""


@dataclass
class PipelineQuota:
    pipeline: str
    max_runs_per_day: int
    runs_today: int
    exceeded: bool

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "max_runs_per_day": self.max_runs_per_day,
            "runs_today": self.runs_today,
            "exceeded": self.exceeded,
        }


class RunQuota:
    """Tracks and enforces per-pipeline daily run quotas."""

    def __init__(self, log_file: str, max_runs_per_day: int = 100) -> None:
        self._log_file = Path(log_file)
        if max_runs_per_day < 1:
            raise QuotaError("max_runs_per_day must be at least 1")
        self._max_runs_per_day = max_runs_per_day

    def _load_records(self) -> List[Dict]:
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

    def _runs_today(self, records: List[Dict], pipeline: str) -> int:
        from datetime import datetime, timezone

        today = datetime.now(timezone.utc).date().isoformat()
        count = 0
        for r in records:
            if r.get("pipeline") != pipeline:
                continue
            started = r.get("started_at", "")
            if started.startswith(today):
                count += 1
        return count

    def check(self, pipeline: str) -> PipelineQuota:
        """Return quota status for a single pipeline."""
        if not pipeline or not isinstance(pipeline, str):
            raise QuotaError("pipeline must be a non-empty string")
        records = self._load_records()
        runs_today = self._runs_today(records, pipeline)
        exceeded = runs_today >= self._max_runs_per_day
        return PipelineQuota(
            pipeline=pipeline,
            max_runs_per_day=self._max_runs_per_day,
            runs_today=runs_today,
            exceeded=exceeded,
        )

    def check_all(self) -> List[PipelineQuota]:
        """Return quota status for every pipeline seen in the log."""
        records = self._load_records()
        pipelines = {r.get("pipeline") for r in records if r.get("pipeline")}
        return [self.check(p) for p in sorted(pipelines)]
