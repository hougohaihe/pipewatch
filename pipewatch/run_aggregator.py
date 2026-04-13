from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class AggregatedBucket:
    key: str
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    total_duration: float = 0.0
    pipelines: List[str] = field(default_factory=list)

    @property
    def avg_duration(self) -> Optional[float]:
        if self.run_count == 0:
            return None
        return round(self.total_duration / self.run_count, 4)

    @property
    def success_rate(self) -> Optional[float]:
        if self.run_count == 0:
            return None
        return round(self.success_count / self.run_count, 4)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "avg_duration_seconds": self.avg_duration,
            "success_rate": self.success_rate,
            "pipelines": sorted(set(self.pipelines)),
        }


class AggregationError(Exception):
    pass


class RunAggregator:
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

    def aggregate_by(self, field_name: str) -> Dict[str, AggregatedBucket]:
        if not field_name or not isinstance(field_name, str):
            raise AggregationError("field_name must be a non-empty string")
        records = self._load_records()
        buckets: Dict[str, AggregatedBucket] = {}
        for record in records:
            key = record.get(field_name)
            if key is None:
                continue
            key = str(key)
            if key not in buckets:
                buckets[key] = AggregatedBucket(key=key)
            bucket = buckets[key]
            bucket.run_count += 1
            status = record.get("status", "")
            if status == "success":
                bucket.success_count += 1
            elif status == "failure":
                bucket.failure_count += 1
            duration = record.get("duration_seconds")
            if isinstance(duration, (int, float)):
                bucket.total_duration += duration
            pipeline = record.get("pipeline")
            if pipeline:
                bucket.pipelines.append(pipeline)
        return buckets

    def summary(self, field_name: str) -> List[dict]:
        buckets = self.aggregate_by(field_name)
        return [b.to_dict() for b in sorted(buckets.values(), key=lambda b: b.key)]
