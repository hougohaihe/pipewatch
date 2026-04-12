"""Assigns a health score to pipeline runs based on success rate, duration trends, and failure streaks."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class PipelineScore:
    pipeline: str
    score: float  # 0.0 (worst) to 100.0 (best)
    success_rate: float
    avg_duration: Optional[float]
    failure_streak: int
    grade: str = field(init=False)

    def __post_init__(self) -> None:
        if self.score >= 90:
            self.grade = "A"
        elif self.score >= 75:
            self.grade = "B"
        elif self.score >= 55:
            self.grade = "C"
        elif self.score >= 35:
            self.grade = "D"
        else:
            self.grade = "F"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "score": round(self.score, 2),
            "success_rate": round(self.success_rate, 4),
            "avg_duration": round(self.avg_duration, 3) if self.avg_duration is not None else None,
            "failure_streak": self.failure_streak,
            "grade": self.grade,
        }


class RunScorer:
    """Computes health scores for pipelines from a run log file."""

    STREAK_PENALTY = 5.0
    MAX_STREAK_PENALTY = 40.0

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

    def _failure_streak(self, runs: List[dict]) -> int:
        streak = 0
        for run in reversed(runs):
            if run.get("status") != "success":
                streak += 1
            else:
                break
        return streak

    def score_pipeline(self, pipeline: str) -> Optional[PipelineScore]:
        records = [r for r in self._load_records() if r.get("pipeline") == pipeline]
        if not records:
            return None
        total = len(records)
        successes = sum(1 for r in records if r.get("status") == "success")
        success_rate = successes / total
        durations = [r["duration_seconds"] for r in records if r.get("duration_seconds") is not None]
        avg_duration = sum(durations) / len(durations) if durations else None
        streak = self._failure_streak(records)
        streak_penalty = min(streak * self.STREAK_PENALTY, self.MAX_STREAK_PENALTY)
        score = max(0.0, success_rate * 100.0 - streak_penalty)
        return PipelineScore(
            pipeline=pipeline,
            score=score,
            success_rate=success_rate,
            avg_duration=avg_duration,
            failure_streak=streak,
        )

    def score_all(self) -> List[PipelineScore]:
        records = self._load_records()
        pipelines = {r["pipeline"] for r in records if "pipeline" in r}
        results = []
        for pipeline in sorted(pipelines):
            ps = self.score_pipeline(pipeline)
            if ps is not None:
                results.append(ps)
        return results
