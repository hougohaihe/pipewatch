from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class ClusterError(Exception):
    """Raised when clustering fails."""


@dataclass
class PipelineCluster:
    key: str
    pipelines: List[str]
    run_count: int
    avg_duration: Optional[float]
    success_rate: float

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "pipelines": self.pipelines,
            "run_count": self.run_count,
            "avg_duration": self.avg_duration,
            "success_rate": round(self.success_rate, 4),
        }


class RunCluster:
    """Groups pipelines into clusters based on a shared field value."""

    def __init__(self, log_file: str) -> None:
        if not isinstance(log_file, str) or not log_file.strip():
            raise ClusterError("log_file must be a non-empty string")
        self._log_file = Path(log_file).expanduser()

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

    def cluster_by(self, field: str) -> Dict[str, PipelineCluster]:
        """Cluster pipelines by a shared field value.

        Returns a dict mapping each unique field value to a PipelineCluster
        that summarises all pipelines sharing that value.
        """
        if not isinstance(field, str) or not field.strip():
            raise ClusterError("field must be a non-empty string")

        records = self._load_records()
        buckets: Dict[str, Dict[str, list]] = {}

        for rec in records:
            key = str(rec.get(field, ""))
            pipeline = rec.get("pipeline", "")
            if not key:
                continue
            buckets.setdefault(key, {})
            buckets[key].setdefault(pipeline, [])
            buckets[key][pipeline].append(rec)

        result: Dict[str, PipelineCluster] = {}
        for key, pipelines_map in buckets.items():
            all_recs = [r for runs in pipelines_map.values() for r in runs]
            durations = [
                r["duration_seconds"]
                for r in all_recs
                if isinstance(r.get("duration_seconds"), (int, float))
            ]
            successes = sum(
                1 for r in all_recs if r.get("status") == "success"
            )
            result[key] = PipelineCluster(
                key=key,
                pipelines=sorted(pipelines_map.keys()),
                run_count=len(all_recs),
                avg_duration=round(sum(durations) / len(durations), 4) if durations else None,
                success_rate=successes / len(all_recs) if all_recs else 0.0,
            )
        return result
