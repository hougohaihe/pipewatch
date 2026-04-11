"""Replay recorded pipeline run metadata for inspection or re-alerting."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


class ReplayError(Exception):
    """Raised when a replay operation fails."""


@dataclass
class ReplayResult:
    run_id: str
    pipeline: str
    status: str
    replayed: bool = False
    error: Optional[str] = None
    record: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "status": self.status,
            "replayed": self.replayed,
            "error": self.error,
        }


class RunReplay:
    """Load and replay pipeline run records through an alert dispatcher."""

    def __init__(self, log_file: str) -> None:
        self.log_file = Path(log_file)

    def _load_records(self) -> List[Dict[str, Any]]:
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

    def get(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Return the record for *run_id*, or None if not found."""
        for record in self._load_records():
            if record.get("run_id") == run_id:
                return record
        return None

    def replay(self, run_id: str, dispatcher: Any) -> ReplayResult:
        """Re-dispatch alerts for the given *run_id*."""
        record = self.get(run_id)
        if record is None:
            raise ReplayError(f"Run ID not found: {run_id}")

        result = ReplayResult(
            run_id=run_id,
            pipeline=record.get("pipeline", ""),
            status=record.get("status", ""),
            record=record,
        )
        try:
            dispatcher.send(record)
            result.replayed = True
        except Exception as exc:  # noqa: BLE001
            result.error = str(exc)
        return result

    def replay_all(self, pipeline: str, dispatcher: Any) -> List[ReplayResult]:
        """Replay all records for *pipeline*."""
        records = [
            r for r in self._load_records() if r.get("pipeline") == pipeline
        ]
        results = []
        for record in records:
            run_id = record.get("run_id", "")
            res = ReplayResult(
                run_id=run_id,
                pipeline=pipeline,
                status=record.get("status", ""),
                record=record,
            )
            try:
                dispatcher.send(record)
                res.replayed = True
            except Exception as exc:  # noqa: BLE001
                res.error = str(exc)
            results.append(res)
        return results
