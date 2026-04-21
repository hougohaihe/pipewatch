from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class CensusError(Exception):
    """Raised when census computation fails."""


@dataclass
class PipelineCensus:
    pipeline: str
    total_runs: int
    unique_statuses: List[str]
    status_counts: Dict[str, int]
    first_seen: Optional[str]
    last_seen: Optional[str]
    active_days: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total_runs": self.total_runs,
            "unique_statuses": self.unique_statuses,
            "status_counts": self.status_counts,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "active_days": self.active_days,
        }


class RunCensus:
    """Computes a census of pipeline runs across all pipelines."""

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

    def compute(self) -> Dict[str, PipelineCensus]:
        records = self._load_records()
        if not records:
            return {}

        grouped: Dict[str, List[dict]] = {}
        for rec in records:
            name = rec.get("pipeline", "unknown")
            grouped.setdefault(name, []).append(rec)

        result: Dict[str, PipelineCensus] = {}
        for pipeline, runs in grouped.items():
            status_counts: Dict[str, int] = {}
            timestamps = []
            active_dates = set()

            for run in runs:
                status = run.get("status", "unknown")
                status_counts[status] = status_counts.get(status, 0) + 1
                ts = run.get("started_at") or run.get("timestamp")
                if ts:
                    timestamps.append(ts)
                    active_dates.add(ts[:10])

            timestamps.sort()
            result[pipeline] = PipelineCensus(
                pipeline=pipeline,
                total_runs=len(runs),
                unique_statuses=sorted(status_counts.keys()),
                status_counts=status_counts,
                first_seen=timestamps[0] if timestamps else None,
                last_seen=timestamps[-1] if timestamps else None,
                active_days=len(active_dates),
            )
        return result

    def compute_for(self, pipeline: str) -> Optional[PipelineCensus]:
        return self.compute().get(pipeline)
