from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class PipelineScore:
    pipeline: str
    run_count: int
    success_count: int
    failure_count: int
    avg_duration: Optional[float]
    score: float

    def __post_init__(self) -> None:
        if not (0.0 <= self.score <= 100.0):
            raise ValueError(f"Score must be between 0 and 100, got {self.score}")

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "avg_duration": self.avg_duration,
            "score": round(self.score, 2),
        }


class RunScorer:
    """Computes a health score (0–100) for each pipeline based on run history."""

    def __init__(
        self,
        log_file: str,
        success_weight: float = 0.7,
        duration_weight: float = 0.3,
        max_expected_duration: float = 300.0,
    ) -> None:
        self._log_file = Path(log_file)
        self._success_weight = success_weight
        self._duration_weight = duration_weight
        self._max_expected_duration = max_expected_duration

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

    def score_all(self) -> Dict[str, PipelineScore]:
        records = self._load_records()
        buckets: Dict[str, List[dict]] = {}
        for r in records:
            name = r.get("pipeline", "unknown")
            buckets.setdefault(name, []).append(r)
        return {name: self._score(name, runs) for name, runs in buckets.items()}

    def score_pipeline(self, pipeline: str) -> Optional[PipelineScore]:
        records = [r for r in self._load_records() if r.get("pipeline") == pipeline]
        if not records:
            return None
        return self._score(pipeline, records)

    def _score(self, pipeline: str, runs: List[dict]) -> PipelineScore:
        total = len(runs)
        successes = sum(1 for r in runs if r.get("status") == "success")
        failures = total - successes
        success_rate = successes / total if total else 0.0

        durations = [
            r["duration_seconds"]
            for r in runs
            if isinstance(r.get("duration_seconds"), (int, float))
        ]
        avg_duration: Optional[float] = sum(durations) / len(durations) if durations else None

        duration_score = 1.0
        if avg_duration is not None and self._max_expected_duration > 0:
            duration_score = max(
                0.0, 1.0 - avg_duration / self._max_expected_duration
            )

        raw_score = (
            self._success_weight * success_rate
            + self._duration_weight * duration_score
        ) * 100.0
        score = min(100.0, max(0.0, raw_score))

        return PipelineScore(
            pipeline=pipeline,
            run_count=total,
            success_count=successes,
            failure_count=failures,
            avg_duration=avg_duration,
            score=score,
        )
