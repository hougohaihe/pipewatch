"""Deduplication support for pipeline run records."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class DeduplicationError(Exception):
    """Raised when deduplication encounters an unrecoverable problem."""


@dataclass
class DeduplicationResult:
    total_records: int
    duplicates_removed: int
    unique_records: List[dict] = field(default_factory=list)

    @property
    def duplicate_count(self) -> int:
        return self.duplicates_removed

    def to_dict(self) -> dict:
        return {
            "total_records": self.total_records,
            "duplicates_removed": self.duplicates_removed,
            "unique_count": len(self.unique_records),
        }


class RunDeduplicator:
    """Detects and removes duplicate run records from a log file.

    Two records are considered duplicates if they share the same
    ``run_id``. When duplicates exist the first occurrence is kept.
    """

    def __init__(self, log_file: str) -> None:
        self.log_file = Path(log_file).expanduser()

    def _load_records(self) -> List[dict]:
        if not self.log_file.exists():
            return []
        records: List[dict] = []
        with self.log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        raise DeduplicationError(
                            f"Malformed record in {self.log_file}: {exc}"
                        ) from exc
        return records

    def _save_records(self, records: List[dict]) -> None:
        with self.log_file.open("w") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")

    def find_duplicates(self) -> Dict[str, List[dict]]:
        """Return a mapping of run_id -> list of duplicate records (>1 entry)."""
        records = self._load_records()
        seen: Dict[str, List[dict]] = {}
        for record in records:
            run_id = record.get("run_id")
            if run_id is None:
                continue
            seen.setdefault(run_id, []).append(record)
        return {rid: recs for rid, recs in seen.items() if len(recs) > 1}

    def deduplicate(self, dry_run: bool = False) -> DeduplicationResult:
        """Remove duplicate records, keeping the first occurrence of each run_id.

        Args:
            dry_run: If *True* the log file is not modified.

        Returns:
            A :class:`DeduplicationResult` describing what was (or would be) removed.
        """
        records = self._load_records()
        seen_ids: Dict[str, bool] = {}
        unique: List[dict] = []
        duplicates_removed = 0

        for record in records:
            run_id = record.get("run_id")
            if run_id is None or run_id not in seen_ids:
                unique.append(record)
                if run_id is not None:
                    seen_ids[run_id] = True
            else:
                duplicates_removed += 1

        if not dry_run and duplicates_removed > 0:
            self._save_records(unique)

        return DeduplicationResult(
            total_records=len(records),
            duplicates_removed=duplicates_removed,
            unique_records=unique,
        )
