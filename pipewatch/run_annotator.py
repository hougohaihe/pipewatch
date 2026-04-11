"""Attach user-defined tags and notes to pipeline run records."""

from __future__ import annotations

import json
import os
from typing import Any


class AnnotationError(Exception):
    """Raised when an annotation operation fails."""


class RunAnnotator:
    """Read and write annotations (tags + notes) on run log records."""

    def __init__(self, log_file: str) -> None:
        self.log_file = log_file

    def _load_records(self) -> list[dict[str, Any]]:
        if not os.path.exists(self.log_file):
            return []
        with open(self.log_file, "r") as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def _save_records(self, records: list[dict[str, Any]]) -> None:
        with open(self.log_file, "w") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")

    def annotate(
        self,
        run_id: str,
        *,
        tags: list[str] | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Add tags and/or a note to the run identified by *run_id*.

        Returns the updated record.
        Raises AnnotationError if *run_id* is not found.
        """
        records = self._load_records()
        for record in records:
            if record.get("run_id") == run_id:
                if tags is not None:
                    existing = record.get("tags", [])
                    record["tags"] = list(dict.fromkeys(existing + tags))
                if note is not None:
                    record["note"] = note
                self._save_records(records)
                return record
        raise AnnotationError(f"Run ID not found: {run_id}")

    def get_annotations(self, run_id: str) -> dict[str, Any]:
        """Return the tags and note for *run_id*, or empty defaults."""
        for record in self._load_records():
            if record.get("run_id") == run_id:
                return {"tags": record.get("tags", []), "note": record.get("note", "")}
        raise AnnotationError(f"Run ID not found: {run_id}")

    def find_by_tag(self, tag: str) -> list[dict[str, Any]]:
        """Return all records that carry *tag*."""
        return [
            r for r in self._load_records() if tag in r.get("tags", [])
        ]
