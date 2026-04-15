from __future__ import annotations

import json
import os
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class AnomalyError(Exception):
    pass


@dataclass
class PipelineAnomaly:
    pipeline: str
    run_id: str
    duration_seconds: float
    mean_duration: float
    stddev: float
    z_score: float
    is_anomaly: bool

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "duration_seconds": self.duration_seconds,
            "mean_duration": self.mean_duration,
            "stddev": self.stddev,
            "z_score": round(self.z_score, 4),
            "is_anomaly": self.is_anomaly,
        }


class RunAnomaly:
    def __init__(self, log_file: str, z_threshold: float = 2.0) -> None:
        if not isinstance(log_file, str) or not log_file.strip():
            raise AnomalyError("log_file must be a non-empty string")
        if z_threshold <= 0:
            raise AnomalyError("z_threshold must be positive")
        self.log_file = os.path.expanduser(log_file)
        self.z_threshold = z_threshold

    def _load_records(self) -> List[Dict]:
        if not os.path.exists(self.log_file):
            return []
        with open(self.log_file, "r") as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def detect(self, pipeline: str) -> List[PipelineAnomaly]:
        records = [
            r for r in self._load_records()
            if r.get("pipeline") == pipeline
            and r.get("duration_seconds") is not None
        ]
        if len(records) < 3:
            return []

        durations = [r["duration_seconds"] for r in records]
        mean = statistics.mean(durations)
        stddev = statistics.stdev(durations)

        results = []
        for r in records:
            d = r["duration_seconds"]
            z = (d - mean) / stddev if stddev > 0 else 0.0
            results.append(
                PipelineAnomaly(
                    pipeline=pipeline,
                    run_id=r.get("run_id", ""),
                    duration_seconds=d,
                    mean_duration=round(mean, 4),
                    stddev=round(stddev, 4),
                    z_score=z,
                    is_anomaly=abs(z) >= self.z_threshold,
                )
            )
        return results

    def detect_all(self) -> Dict[str, List[PipelineAnomaly]]:
        records = self._load_records()
        pipelines = {r.get("pipeline") for r in records if r.get("pipeline")}
        return {p: self.detect(p) for p in sorted(pipelines)}
