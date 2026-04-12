"""Attach and manage labels (key-value metadata) on pipeline run records."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class LabelError(Exception):
    """Raised when a labeling operation fails."""


class RunLabeler:
    """Read and write arbitrary key-value labels on run records."""

    def __init__(self, log_file: str) -> None:
        self.log_file = Path(log_file)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_records(self) -> list[dict[str, Any]]:
        if not self.log_file.exists():
            return []
        records: list[dict[str, Any]] = []
        with self.log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def _save_records(self, records: list[dict[str, Any]]) -> None:
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("w") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_labels(self, run_id: str, labels: dict[str, str]) -> None:
        """Merge *labels* into the record identified by *run_id*.

        Raises LabelError if the run_id is not found.
        """
        if not isinstance(labels, dict):
            raise LabelError("labels must be a dict")
        records = self._load_records()
        for record in records:
            if record.get("run_id") == run_id:
                existing: dict[str, str] = record.get("labels", {})
                existing.update(labels)
                record["labels"] = existing
                self._save_records(records)
                return
        raise LabelError(f"run_id not found: {run_id}")

    def remove_label(self, run_id: str, key: str) -> None:
        """Remove a single label *key* from the record. Silently ignores missing keys."""
        records = self._load_records()
        for record in records:
            if record.get("run_id") == run_id:
                record.get("labels", {}).pop(key, None)
                self._save_records(records)
                return
        raise LabelError(f"run_id not found: {run_id}")

    def get_labels(self, run_id: str) -> dict[str, str]:
        """Return the labels dict for *run_id*, or {} if the run has none."""
        for record in self._load_records():
            if record.get("run_id") == run_id:
                return record.get("labels", {})
        raise LabelError(f"run_id not found: {run_id}")

    def find_by_label(self, key: str, value: str) -> list[dict[str, Any]]:
        """Return all records whose labels contain *key*=*value*."""
        return [
            r for r in self._load_records()
            if r.get("labels", {}).get(key) == value
        ]
