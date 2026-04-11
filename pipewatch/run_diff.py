"""Diff two pipeline runs and highlight field-level changes."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class FieldDiff:
    key: str
    old_value: Any
    new_value: Any

    @property
    def changed(self) -> bool:
        return self.old_value != self.new_value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "old": self.old_value,
            "new": self.new_value,
            "changed": self.changed,
        }


@dataclass
class RunDiffResult:
    run_id_a: str
    run_id_b: str
    diffs: List[FieldDiff] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(d.changed for d in self.diffs)

    @property
    def changed_fields(self) -> List[str]:
        return [d.key for d in self.diffs if d.changed]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id_a": self.run_id_a,
            "run_id_b": self.run_id_b,
            "has_changes": self.has_changes,
            "changed_fields": self.changed_fields,
            "diffs": [d.to_dict() for d in self.diffs],
        }


class RunDiff:
    """Compare two run records from a log file by their run IDs."""

    IGNORED_KEYS = {"run_id", "start_time", "end_time"}

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
                    records.append(json.loads(line))
        return records

    def _find_run(self, records: List[Dict[str, Any]], run_id: str) -> Optional[Dict[str, Any]]:
        for record in records:
            if record.get("run_id") == run_id:
                return record
        return None

    def diff(self, run_id_a: str, run_id_b: str, ignore_keys: Optional[List[str]] = None) -> RunDiffResult:
        """Return a RunDiffResult comparing the two specified runs."""
        ignored = self.IGNORED_KEYS | set(ignore_keys or [])
        records = self._load_records()
        record_a = self._find_run(records, run_id_a)
        record_b = self._find_run(records, run_id_b)

        if record_a is None:
            raise KeyError(f"Run not found: {run_id_a}")
        if record_b is None:
            raise KeyError(f"Run not found: {run_id_b}")

        all_keys = (set(record_a) | set(record_b)) - ignored
        diffs = [
            FieldDiff(
                key=key,
                old_value=record_a.get(key),
                new_value=record_b.get(key),
            )
            for key in sorted(all_keys)
        ]
        return RunDiffResult(run_id_a=run_id_a, run_id_b=run_id_b, diffs=diffs)
