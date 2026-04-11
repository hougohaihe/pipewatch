"""Compute aggregate statistics across pipeline runs."""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class PipelineStats:
    pipeline: str
    run_count: int
    success_count: int
    failure_count: int
    avg_duration: Optional[float]
    min_duration: Optional[float]
    max_duration: Optional[float]
    p50_duration: Optional[float]
    p95_duration: Optional[float]

    @property
    def success_rate(self) -> float:
        if self.run_count == 0:
            return 0.0
        return round(self.success_count / self.run_count, 4)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": self.success_rate,
            "avg_duration": self.avg_duration,
            "min_duration": self.min_duration,
            "max_duration": self.max_duration,
            "p50_duration": self.p50_duration,
            "p95_duration": self.p95_duration,
        }


class RunStats:
    def __init__(self, log_file: str) -> None:
        self.log_file = Path(log_file)

    def _load_records(self) -> List[dict]:
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

    def _percentile(self, data: List[float], pct: float) -> Optional[float]:
        if not data:
            return None
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * pct / 100)
        idx = min(idx, len(sorted_data) - 1)
        return round(sorted_data[idx], 4)

    def compute(self, pipeline: Optional[str] = None) -> Dict[str, PipelineStats]:
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]

        grouped: Dict[str, List[dict]] = {}
        for rec in records:
            name = rec.get("pipeline", "unknown")
            grouped.setdefault(name, []).append(rec)

        result: Dict[str, PipelineStats] = {}
        for name, runs in grouped.items():
            durations = [
                float(r["duration_seconds"])
                for r in runs
                if r.get("duration_seconds") is not None
            ]
            successes = [r for r in runs if r.get("status") == "success"]
            failures = [r for r in runs if r.get("status") == "failure"]
            result[name] = PipelineStats(
                pipeline=name,
                run_count=len(runs),
                success_count=len(successes),
                failure_count=len(failures),
                avg_duration=round(statistics.mean(durations), 4) if durations else None,
                min_duration=round(min(durations), 4) if durations else None,
                max_duration=round(max(durations), 4) if durations else None,
                p50_duration=self._percentile(durations, 50),
                p95_duration=self._percentile(durations, 95),
            )
        return result
