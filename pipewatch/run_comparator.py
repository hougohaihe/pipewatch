"""Compare pipeline run metrics across time windows or between pipelines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class ComparisonResult:
    pipeline: str
    baseline_avg_duration: Optional[float]
    current_avg_duration: Optional[float]
    baseline_success_rate: Optional[float]
    current_success_rate: Optional[float]
    duration_delta: Optional[float] = field(init=False)
    success_rate_delta: Optional[float] = field(init=False)

    def __post_init__(self) -> None:
        if self.baseline_avg_duration is not None and self.current_avg_duration is not None:
            self.duration_delta = self.current_avg_duration - self.baseline_avg_duration
        else:
            self.duration_delta = None

        if self.baseline_success_rate is not None and self.current_success_rate is not None:
            self.success_rate_delta = self.current_success_rate - self.baseline_success_rate
        else:
            self.success_rate_delta = None

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "baseline_avg_duration": self.baseline_avg_duration,
            "current_avg_duration": self.current_avg_duration,
            "duration_delta": self.duration_delta,
            "baseline_success_rate": self.baseline_success_rate,
            "current_success_rate": self.current_success_rate,
            "success_rate_delta": self.success_rate_delta,
        }


class RunComparator:
    """Compare pipeline run statistics between two sets of records."""

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
                    records.append(json.loads(line))
        return records

    def _filter(self, records: List[Dict], pipeline: Optional[str]) -> List[Dict]:
        if pipeline is None:
            return records
        return [r for r in records if r.get("pipeline") == pipeline]

    def _avg_duration(self, records: List[Dict]) -> Optional[float]:
        durations = [r["duration_seconds"] for r in records if "duration_seconds" in r and r["duration_seconds"] is not None]
        return sum(durations) / len(durations) if durations else None

    def _success_rate(self, records: List[Dict]) -> Optional[float]:
        if not records:
            return None
        successes = sum(1 for r in records if r.get("status") == "success")
        return successes / len(records)

    def compare(self, pipeline: str, baseline_records: List[Dict], current_records: List[Dict]) -> ComparisonResult:
        """Compare two pre-filtered record sets for a given pipeline."""
        return ComparisonResult(
            pipeline=pipeline,
            baseline_avg_duration=self._avg_duration(baseline_records),
            current_avg_duration=self._avg_duration(current_records),
            baseline_success_rate=self._success_rate(baseline_records),
            current_success_rate=self._success_rate(current_records),
        )

    def compare_last_n(self, pipeline: str, baseline_n: int, current_n: int) -> ComparisonResult:
        """Compare the last current_n runs against the preceding baseline_n runs."""
        all_records = self._filter(self._load_records(), pipeline)
        current_records = all_records[-current_n:] if len(all_records) >= current_n else all_records
        baseline_start = max(0, len(all_records) - current_n - baseline_n)
        baseline_end = max(0, len(all_records) - current_n)
        baseline_records = all_records[baseline_start:baseline_end]
        return self.compare(pipeline, baseline_records, current_records)
