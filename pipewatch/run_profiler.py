from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class PipelineProfile:
    pipeline: str
    run_count: int
    avg_duration_seconds: Optional[float]
    min_duration_seconds: Optional[float]
    max_duration_seconds: Optional[float]
    p50_duration_seconds: Optional[float]
    p95_duration_seconds: Optional[float]
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "run_count": self.run_count,
            "avg_duration_seconds": self.avg_duration_seconds,
            "min_duration_seconds": self.min_duration_seconds,
            "max_duration_seconds": self.max_duration_seconds,
            "p50_duration_seconds": self.p50_duration_seconds,
            "p95_duration_seconds": self.p95_duration_seconds,
            "tags": self.tags,
        }


class RunProfiler:
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

    def _percentile(self, values: List[float], pct: float) -> Optional[float]:
        if not values:
            return None
        sorted_vals = sorted(values)
        idx = int(len(sorted_vals) * pct / 100)
        idx = min(idx, len(sorted_vals) - 1)
        return round(sorted_vals[idx], 4)

    def profile(self, pipeline: str) -> Optional[PipelineProfile]:
        records = [
            r for r in self._load_records()
            if r.get("pipeline") == pipeline
        ]
        if not records:
            return None
        durations = [
            r["duration_seconds"]
            for r in records
            if isinstance(r.get("duration_seconds"), (int, float))
        ]
        tags: List[str] = []
        for r in records:
            tags.extend(r.get("tags") or [])
        unique_tags = sorted(set(tags))
        avg = round(sum(durations) / len(durations), 4) if durations else None
        return PipelineProfile(
            pipeline=pipeline,
            run_count=len(records),
            avg_duration_seconds=avg,
            min_duration_seconds=round(min(durations), 4) if durations else None,
            max_duration_seconds=round(max(durations), 4) if durations else None,
            p50_duration_seconds=self._percentile(durations, 50),
            p95_duration_seconds=self._percentile(durations, 95),
            tags=unique_tags,
        )

    def profile_all(self) -> Dict[str, PipelineProfile]:
        records = self._load_records()
        pipelines = {r["pipeline"] for r in records if "pipeline" in r}
        return {p: self.profile(p) for p in sorted(pipelines) if self.profile(p)}
