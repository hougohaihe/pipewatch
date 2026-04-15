from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


class RankError(Exception):
    """Raised when ranking cannot be performed."""


@dataclass
class RankedPipeline:
    pipeline: str
    rank: int
    score: float
    run_count: int
    success_rate: float
    avg_duration: Optional[float]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rank": self.rank,
            "score": self.score,
            "run_count": self.run_count,
            "success_rate": self.success_rate,
            "avg_duration": self.avg_duration,
        }


class RunRanker:
    """Ranks pipelines by a composite score based on success rate and throughput."""

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

    def _compute_score(self, success_rate: float, run_count: int, avg_duration: Optional[float]) -> float:
        """Higher success rate and more runs = higher score; longer duration penalises."""
        duration_penalty = 0.0
        if avg_duration is not None and avg_duration > 0:
            duration_penalty = min(avg_duration / 3600.0, 1.0) * 10
        return round(success_rate * 100 + run_count * 0.5 - duration_penalty, 4)

    def rank(self, pipeline: Optional[str] = None) -> List[RankedPipeline]:
        records = self._load_records()
        if not records:
            return []

        buckets: dict[str, dict] = {}
        for rec in records:
            name = rec.get("pipeline", "unknown")
            if pipeline is not None and name != pipeline:
                continue
            b = buckets.setdefault(name, {"total": 0, "success": 0, "durations": []})
            b["total"] += 1
            if rec.get("status") == "success":
                b["success"] += 1
            dur = rec.get("duration_seconds")
            if dur is not None:
                b["durations"].append(float(dur))

        ranked: List[RankedPipeline] = []
        for name, b in buckets.items():
            total = b["total"]
            success_rate = b["success"] / total if total else 0.0
            avg_dur = sum(b["durations"]) / len(b["durations"]) if b["durations"] else None
            score = self._compute_score(success_rate, total, avg_dur)
            ranked.append(
                RankedPipeline(
                    pipeline=name,
                    rank=0,
                    score=score,
                    run_count=total,
                    success_rate=round(success_rate, 4),
                    avg_duration=round(avg_dur, 4) if avg_dur is not None else None,
                )
            )

        ranked.sort(key=lambda r: r.score, reverse=True)
        for i, rp in enumerate(ranked, start=1):
            rp.rank = i
        return ranked
