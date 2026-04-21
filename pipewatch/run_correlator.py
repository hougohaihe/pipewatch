from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class CorrelationError(Exception):
    """Raised when correlation cannot be computed."""


@dataclass
class PipelineCorrelation:
    pipeline_a: str
    pipeline_b: str
    shared_run_count: int
    co_failure_count: int
    co_success_count: int

    @property
    def co_failure_rate(self) -> float:
        if self.shared_run_count == 0:
            return 0.0
        return round(self.co_failure_count / self.shared_run_count, 4)

    def to_dict(self) -> Dict:
        return {
            "pipeline_a": self.pipeline_a,
            "pipeline_b": self.pipeline_b,
            "shared_run_count": self.shared_run_count,
            "co_failure_count": self.co_failure_count,
            "co_success_count": self.co_success_count,
            "co_failure_rate": self.co_failure_rate,
        }


class RunCorrelator:
    """Identifies co-failure and co-success patterns across pipelines."""

    def __init__(self, log_file: str) -> None:
        self._log_file = Path(log_file)

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

    def _group_by_window(self, records: List[Dict], window_seconds: float) -> List[List[Dict]]:
        """Group records that started within window_seconds of each other."""
        sorted_records = sorted(records, key=lambda r: r.get("start_time", ""))
        groups: List[List[Dict]] = []
        current: List[Dict] = []
        for rec in sorted_records:
            if not current:
                current.append(rec)
            else:
                try:
                    from datetime import datetime
                    t0 = datetime.fromisoformat(current[0]["start_time"])
                    t1 = datetime.fromisoformat(rec["start_time"])
                    if abs((t1 - t0).total_seconds()) <= window_seconds:
                        current.append(rec)
                    else:
                        groups.append(current)
                        current = [rec]
                except (KeyError, ValueError):
                    current.append(rec)
        if current:
            groups.append(current)
        return groups

    def correlate(self, window_seconds: float = 60.0) -> List[PipelineCorrelation]:
        records = self._load_records()
        if not records:
            return []

        groups = self._group_by_window(records, window_seconds)
        pair_stats: Dict[tuple, Dict] = {}

        for group in groups:
            if len(group) < 2:
                continue
            pipelines = list({r["pipeline"] for r in group if "pipeline" in r})
            for i in range(len(pipelines)):
                for j in range(i + 1, len(pipelines)):
                    pa, pb = sorted([pipelines[i], pipelines[j]])
                    key = (pa, pb)
                    if key not in pair_stats:
                        pair_stats[key] = {"shared": 0, "co_fail": 0, "co_success": 0}
                    pair_stats[key]["shared"] += 1
                    statuses = {r["pipeline"]: r.get("status", "") for r in group}
                    if statuses.get(pa) == "failed" and statuses.get(pb) == "failed":
                        pair_stats[key]["co_fail"] += 1
                    if statuses.get(pa) == "success" and statuses.get(pb) == "success":
                        pair_stats[key]["co_success"] += 1

        return [
            PipelineCorrelation(
                pipeline_a=k[0],
                pipeline_b=k[1],
                shared_run_count=v["shared"],
                co_failure_count=v["co_fail"],
                co_success_count=v["co_success"],
            )
            for k, v in pair_stats.items()
        ]
