from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import json


@dataclass
class PipelineSummary:
    pipeline: str
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    avg_duration_seconds: Optional[float] = None
    last_run_status: Optional[str] = None
    last_run_time: Optional[str] = None

    def success_rate(self) -> Optional[float]:
        if self.total_runs == 0:
            return None
        return round(self.successful_runs / self.total_runs * 100, 2)

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failed_runs": self.failed_runs,
            "avg_duration_seconds": self.avg_duration_seconds,
            "success_rate_pct": self.success_rate(),
            "last_run_status": self.last_run_status,
            "last_run_time": self.last_run_time,
        }


class RunSummarizer:
    def __init__(self, log_file: str) -> None:
        self.log_file = Path(log_file)

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

    def summarize_all(self) -> List[PipelineSummary]:
        records = self._load_records()
        grouped: Dict[str, List[Dict]] = {}
        for rec in records:
            name = rec.get("pipeline", "unknown")
            grouped.setdefault(name, []).append(rec)
        summaries = []
        for pipeline, runs in grouped.items():
            summaries.append(self._build_summary(pipeline, runs))
        return summaries

    def summarize_pipeline(self, pipeline: str) -> Optional[PipelineSummary]:
        records = [r for r in self._load_records() if r.get("pipeline") == pipeline]
        if not records:
            return None
        return self._build_summary(pipeline, records)

    def _build_summary(self, pipeline: str, runs: List[Dict]) -> PipelineSummary:
        total = len(runs)
        successful = sum(1 for r in runs if r.get("status") == "success")
        failed = sum(1 for r in runs if r.get("status") == "failure")
        durations = [
            r["duration_seconds"]
            for r in runs
            if isinstance(r.get("duration_seconds"), (int, float))
        ]
        avg_dur = round(sum(durations) / len(durations), 4) if durations else None
        last = max(runs, key=lambda r: r.get("started_at", ""), default=None)
        return PipelineSummary(
            pipeline=pipeline,
            total_runs=total,
            successful_runs=successful,
            failed_runs=failed,
            avg_duration_seconds=avg_dur,
            last_run_status=last.get("status") if last else None,
            last_run_time=last.get("started_at") if last else None,
        )
