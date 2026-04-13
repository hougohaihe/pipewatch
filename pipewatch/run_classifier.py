from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class ClassificationError(Exception):
    """Raised when classification cannot be performed."""


@dataclass
class PipelineClass:
    pipeline: str
    label: str
    run_count: int
    failure_rate: float
    avg_duration: Optional[float]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "label": self.label,
            "run_count": self.run_count,
            "failure_rate": round(self.failure_rate, 4),
            "avg_duration": self.avg_duration,
        }


class RunClassifier:
    """Classifies pipelines based on failure rate and run volume thresholds."""

    LABEL_HEALTHY = "healthy"
    LABEL_FLAKY = "flaky"
    LABEL_FAILING = "failing"
    LABEL_INACTIVE = "inactive"

    def __init__(
        self,
        log_file: str,
        flaky_threshold: float = 0.25,
        failing_threshold: float = 0.60,
        min_runs: int = 2,
    ) -> None:
        self.log_file = Path(log_file)
        self.flaky_threshold = flaky_threshold
        self.failing_threshold = failing_threshold
        self.min_runs = min_runs

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

    def _label(self, failure_rate: float, run_count: int) -> str:
        if run_count < self.min_runs:
            return self.LABEL_INACTIVE
        if failure_rate >= self.failing_threshold:
            return self.LABEL_FAILING
        if failure_rate >= self.flaky_threshold:
            return self.LABEL_FLAKY
        return self.LABEL_HEALTHY

    def classify_all(self) -> List[PipelineClass]:
        records = self._load_records()
        buckets: Dict[str, List[dict]] = {}
        for r in records:
            name = r.get("pipeline", "unknown")
            buckets.setdefault(name, []).append(r)

        results = []
        for pipeline, runs in buckets.items():
            run_count = len(runs)
            failures = sum(1 for r in runs if r.get("status") == "failure")
            failure_rate = failures / run_count if run_count else 0.0
            durations = [
                r["duration_seconds"]
                for r in runs
                if isinstance(r.get("duration_seconds"), (int, float))
            ]
            avg_duration = sum(durations) / len(durations) if durations else None
            label = self._label(failure_rate, run_count)
            results.append(
                PipelineClass(
                    pipeline=pipeline,
                    label=label,
                    run_count=run_count,
                    failure_rate=failure_rate,
                    avg_duration=avg_duration,
                )
            )
        return results

    def classify_pipeline(self, pipeline: str) -> Optional[PipelineClass]:
        for pc in self.classify_all():
            if pc.pipeline == pipeline:
                return pc
        return None
