"""Metrics collector for pipeline run statistics."""

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RunMetrics:
    """Holds timing and resource metrics for a single pipeline run."""

    run_id: str
    pipeline_name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    records_processed: int = 0
    errors_encountered: int = 0
    extra: dict = field(default_factory=dict)

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time is None:
            return None
        return round(self.end_time - self.start_time, 4)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "records_processed": self.records_processed,
            "errors_encountered": self.errors_encountered,
            "extra": self.extra,
        }


class MetricsCollector:
    """Collects and stores metrics across pipeline runs."""

    def __init__(self):
        self._runs: dict[str, RunMetrics] = {}

    def start(self, run_id: str, pipeline_name: str) -> RunMetrics:
        metrics = RunMetrics(run_id=run_id, pipeline_name=pipeline_name)
        self._runs[run_id] = metrics
        return metrics

    def finish(self, run_id: str, records_processed: int = 0,
               errors_encountered: int = 0, extra: Optional[dict] = None) -> RunMetrics:
        metrics = self._get(run_id)
        metrics.end_time = time.time()
        metrics.records_processed = records_processed
        metrics.errors_encountered = errors_encountered
        if extra:
            metrics.extra.update(extra)
        return metrics

    def get(self, run_id: str) -> Optional[RunMetrics]:
        return self._runs.get(run_id)

    def _get(self, run_id: str) -> RunMetrics:
        metrics = self._runs.get(run_id)
        if metrics is None:
            raise KeyError(f"No metrics found for run_id: {run_id}")
        return metrics

    def all_metrics(self) -> list[dict]:
        return [m.to_dict() for m in self._runs.values()]

    def summary(self) -> dict:
        runs = list(self._runs.values())
        completed = [r for r in runs if r.end_time is not None]
        durations = [r.duration_seconds for r in completed if r.duration_seconds is not None]
        return {
            "total_runs": len(runs),
            "completed_runs": len(completed),
            "avg_duration_seconds": round(sum(durations) / len(durations), 4) if durations else None,
            "total_records_processed": sum(r.records_processed for r in completed),
            "total_errors": sum(r.errors_encountered for r in completed),
        }
