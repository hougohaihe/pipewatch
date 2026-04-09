"""Filtering utilities for querying run log records."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, List, Optional


class RunFilter:
    """Loads and filters structured run log records from a log file."""

    def __init__(self, log_path: str | Path) -> None:
        self.log_path = Path(log_path)

    def _load_records(self) -> List[dict]:
        if not self.log_path.exists():
            return []
        records = []
        with self.log_path.open("r") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def all(self) -> List[dict]:
        """Return all records."""
        return self._load_records()

    def by_pipeline(self, pipeline_name: str) -> List[dict]:
        """Return records matching the given pipeline name."""
        return [
            r for r in self._load_records()
            if r.get("pipeline") == pipeline_name
        ]

    def by_status(self, status: str) -> List[dict]:
        """Return records matching the given status string."""
        return [
            r for r in self._load_records()
            if r.get("status") == status
        ]

    def by_run_id(self, run_id: str) -> Optional[dict]:
        """Return the single record with the given run_id, or None."""
        for r in self._load_records():
            if r.get("run_id") == run_id:
                return r
        return None

    def where(self, predicate: Callable[[dict], bool]) -> List[dict]:
        """Return records satisfying an arbitrary predicate function."""
        return [r for r in self._load_records() if predicate(r)]

    def latest(self, n: int = 10) -> List[dict]:
        """Return the n most recent records (by file order)."""
        records = self._load_records()
        return records[-n:] if len(records) >= n else records
