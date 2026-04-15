from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class BaselineError(Exception):
    pass


@dataclass
class PipelineBaseline:
    pipeline: str
    avg_duration_seconds: Optional[float]
    success_rate: float
    sample_size: int
    fields: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "avg_duration_seconds": self.avg_duration_seconds,
            "success_rate": self.success_rate,
            "sample_size": self.sample_size,
            "fields": self.fields,
        }


class RunBaseline:
    """Compute baseline statistics for each pipeline from historical run logs."""

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

    def compute(self, pipeline: str) -> Optional[PipelineBaseline]:
        records = [
            r for r in self._load_records() if r.get("pipeline") == pipeline
        ]
        if not records:
            return None
        return self._build_baseline(pipeline, records)

    def compute_all(self) -> Dict[str, PipelineBaseline]:
        records = self._load_records()
        pipelines: Dict[str, List[dict]] = {}
        for r in records:
            name = r.get("pipeline", "")
            if name:
                pipelines.setdefault(name, []).append(r)
        return {
            name: self._build_baseline(name, runs)
            for name, runs in pipelines.items()
        }

    def _build_baseline(self, pipeline: str, records: List[dict]) -> PipelineBaseline:
        durations = [
            r["duration_seconds"]
            for r in records
            if isinstance(r.get("duration_seconds"), (int, float))
        ]
        avg_duration = sum(durations) / len(durations) if durations else None
        successes = sum(1 for r in records if r.get("status") == "success")
        success_rate = successes / len(records) if records else 0.0
        return PipelineBaseline(
            pipeline=pipeline,
            avg_duration_seconds=round(avg_duration, 4) if avg_duration is not None else None,
            success_rate=round(success_rate, 4),
            sample_size=len(records),
        )
