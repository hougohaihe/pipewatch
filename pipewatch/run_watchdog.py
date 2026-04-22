from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


class WatchdogError(Exception):
    pass


@dataclass
class WatchdogAlert:
    pipeline: str
    run_id: str
    reason: str
    duration_seconds: Optional[float]
    threshold_seconds: float
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "reason": self.reason,
            "duration_seconds": self.duration_seconds,
            "threshold_seconds": self.threshold_seconds,
            "timestamp": self.timestamp,
        }


class RunWatchdog:
    """Scans run log for pipelines that exceeded a duration threshold."""

    def __init__(self, log_file: str, default_threshold_seconds: float = 300.0) -> None:
        if not log_file or not isinstance(log_file, str):
            raise WatchdogError("log_file must be a non-empty string")
        if default_threshold_seconds <= 0:
            raise WatchdogError("default_threshold_seconds must be positive")
        self.log_file = Path(log_file)
        self.default_threshold_seconds = default_threshold_seconds

    def _load_records(self) -> List[Dict]:
        if not self.log_file.exists():
            return []
        records = []
        with self.log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def check(
        self,
        thresholds: Optional[Dict[str, float]] = None,
    ) -> List[WatchdogAlert]:
        """Return alerts for runs that exceeded their duration threshold."""
        thresholds = thresholds or {}
        records = self._load_records()
        alerts: List[WatchdogAlert] = []
        for rec in records:
            pipeline = rec.get("pipeline", "")
            run_id = rec.get("run_id", "")
            duration = rec.get("duration_seconds")
            if duration is None:
                continue
            threshold = thresholds.get(pipeline, self.default_threshold_seconds)
            if duration > threshold:
                alerts.append(
                    WatchdogAlert(
                        pipeline=pipeline,
                        run_id=run_id,
                        reason="duration_exceeded",
                        duration_seconds=duration,
                        threshold_seconds=threshold,
                    )
                )
        return alerts

    def check_pipeline(
        self,
        pipeline: str,
        threshold_seconds: Optional[float] = None,
    ) -> List[WatchdogAlert]:
        """Return alerts for a specific pipeline."""
        t = threshold_seconds if threshold_seconds is not None else self.default_threshold_seconds
        return self.check(thresholds={pipeline: t})
