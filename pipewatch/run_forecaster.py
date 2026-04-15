from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
import json
import statistics
from pathlib import Path


class ForecastError(Exception):
    pass


@dataclass
class PipelineForecast:
    pipeline: str
    sample_size: int
    avg_duration_seconds: float
    predicted_duration_seconds: float
    predicted_success_rate: float
    trend: str  # "improving", "degrading", "stable"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_size": self.sample_size,
            "avg_duration_seconds": self.avg_duration_seconds,
            "predicted_duration_seconds": self.predicted_duration_seconds,
            "predicted_success_rate": self.predicted_success_rate,
            "trend": self.trend,
        }


class RunForecaster:
    def __init__(self, log_file: str, window: int = 10) -> None:
        if not isinstance(log_file, str) or not log_file.strip():
            raise ForecastError("log_file must be a non-empty string")
        if window < 2:
            raise ForecastError("window must be at least 2")
        self._log_file = Path(log_file)
        self._window = window

    def _load_records(self) -> List[dict]:
        if not self._log_file.exists():
            return []
        records = []
        with open(self._log_file) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def _detect_trend(self, durations: List[float]) -> str:
        if len(durations) < 4:
            return "stable"
        mid = len(durations) // 2
        first_half = statistics.mean(durations[:mid])
        second_half = statistics.mean(durations[mid:])
        delta = second_half - first_half
        threshold = first_half * 0.1
        if delta > threshold:
            return "degrading"
        if delta < -threshold:
            return "improving"
        return "stable"

    def forecast(self, pipeline: str) -> Optional[PipelineForecast]:
        records = self._load_records()
        runs = [
            r for r in records
            if r.get("pipeline") == pipeline and r.get("duration_seconds") is not None
        ]
        if not runs:
            return None
        recent = runs[-self._window:]
        durations = [r["duration_seconds"] for r in recent]
        statuses = [r.get("status", "") for r in recent]
        avg_dur = statistics.mean(durations)
        success_rate = statuses.count("success") / len(statuses) if statuses else 0.0
        trend = self._detect_trend(durations)
        if trend == "degrading":
            predicted_dur = avg_dur * 1.1
        elif trend == "improving":
            predicted_dur = avg_dur * 0.9
        else:
            predicted_dur = avg_dur
        return PipelineForecast(
            pipeline=pipeline,
            sample_size=len(recent),
            avg_duration_seconds=round(avg_dur, 4),
            predicted_duration_seconds=round(predicted_dur, 4),
            predicted_success_rate=round(success_rate, 4),
            trend=trend,
        )

    def forecast_all(self) -> List[PipelineForecast]:
        records = self._load_records()
        pipelines = {r["pipeline"] for r in records if "pipeline" in r}
        results = []
        for p in sorted(pipelines):
            fc = self.forecast(p)
            if fc is not None:
                results.append(fc)
        return results
