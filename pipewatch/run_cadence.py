from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev
from typing import Dict, List, Optional


class CadenceError(Exception):
    """Raised when cadence analysis fails."""


@dataclass
class PipelineCadence:
    pipeline: str
    run_count: int
    avg_interval_seconds: Optional[float]
    stddev_interval_seconds: Optional[float]
    min_interval_seconds: Optional[float]
    max_interval_seconds: Optional[float]
    is_regular: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "run_count": self.run_count,
            "avg_interval_seconds": self.avg_interval_seconds,
            "stddev_interval_seconds": self.stddev_interval_seconds,
            "min_interval_seconds": self.min_interval_seconds,
            "max_interval_seconds": self.max_interval_seconds,
            "is_regular": self.is_regular,
        }


class RunCadence:
    """Analyses the timing cadence of pipeline runs."""

    REGULARITY_THRESHOLD = 0.25  # CV threshold: stddev/mean

    def __init__(self, log_file: str, min_runs: int = 3) -> None:
        if not isinstance(log_file, str) or not log_file.strip():
            raise CadenceError("log_file must be a non-empty string")
        if min_runs < 2:
            raise CadenceError("min_runs must be at least 2")
        self._log_file = Path(log_file)
        self._min_runs = min_runs

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

    def _parse_ts(self, record: dict) -> Optional[float]:
        ts = record.get("start_time") or record.get("timestamp")
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp()
        except (ValueError, TypeError):
            return None

    def compute(self, pipeline: Optional[str] = None) -> Dict[str, PipelineCadence]:
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]

        groups: Dict[str, List[float]] = {}
        for r in records:
            name = r.get("pipeline", "unknown")
            ts = self._parse_ts(r)
            if ts is not None:
                groups.setdefault(name, []).append(ts)

        result: Dict[str, PipelineCadence] = {}
        for name, timestamps in groups.items():
            timestamps.sort()
            run_count = len(timestamps)
            if run_count < self._min_runs:
                result[name] = PipelineCadence(
                    pipeline=name,
                    run_count=run_count,
                    avg_interval_seconds=None,
                    stddev_interval_seconds=None,
                    min_interval_seconds=None,
                    max_interval_seconds=None,
                    is_regular=False,
                )
                continue

            intervals = [
                timestamps[i + 1] - timestamps[i]
                for i in range(len(timestamps) - 1)
            ]
            avg = mean(intervals)
            sd = stdev(intervals) if len(intervals) > 1 else 0.0
            cv = (sd / avg) if avg > 0 else float("inf")
            result[name] = PipelineCadence(
                pipeline=name,
                run_count=run_count,
                avg_interval_seconds=round(avg, 3),
                stddev_interval_seconds=round(sd, 3),
                min_interval_seconds=round(min(intervals), 3),
                max_interval_seconds=round(max(intervals), 3),
                is_regular=cv <= self.REGULARITY_THRESHOLD,
            )
        return result
