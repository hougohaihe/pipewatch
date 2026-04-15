from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class HeatmapError(Exception):
    pass


@dataclass
class HeatmapBucket:
    label: str
    total: int = 0
    success: int = 0
    failure: int = 0

    @property
    def error_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return round(self.failure / self.total, 4)

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "total": self.total,
            "success": self.success,
            "failure": self.failure,
            "error_rate": self.error_rate,
        }


class RunHeatmap:
    """Aggregate run records into time-bucketed heatmap data."""

    VALID_GRANULARITIES = ("hour", "day", "weekday")

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

    def build(
        self,
        granularity: str = "hour",
        pipeline: Optional[str] = None,
    ) -> List[HeatmapBucket]:
        if granularity not in self.VALID_GRANULARITIES:
            raise HeatmapError(
                f"Invalid granularity '{granularity}'. "
                f"Choose from {self.VALID_GRANULARITIES}."
            )
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]

        buckets: Dict[str, HeatmapBucket] = defaultdict(
            lambda label=None: None
        )
        bucket_map: Dict[str, HeatmapBucket] = {}

        for record in records:
            start = record.get("start_time", "")
            if not start:
                continue
            label = self._extract_label(start, granularity)
            if label not in bucket_map:
                bucket_map[label] = HeatmapBucket(label=label)
            bucket = bucket_map[label]
            bucket.total += 1
            status = record.get("status", "").lower()
            if status == "success":
                bucket.success += 1
            elif status in ("failure", "failed", "error"):
                bucket.failure += 1

        return sorted(bucket_map.values(), key=lambda b: b.label)

    @staticmethod
    def _extract_label(iso_timestamp: str, granularity: str) -> str:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(iso_timestamp)
            if granularity == "hour":
                return dt.strftime("%Y-%m-%dT%H")
            elif granularity == "day":
                return dt.strftime("%Y-%m-%d")
            elif granularity == "weekday":
                return dt.strftime("%A")
        except (ValueError, TypeError):
            return "unknown"
        return "unknown"
