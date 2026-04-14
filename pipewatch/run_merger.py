"""Merge multiple pipeline run log files into a single unified log."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


class MergeError(Exception):
    """Raised when a merge operation fails."""


@dataclass
class MergeResult:
    merged_count: int
    skipped_count: int
    source_files: List[str]
    output_file: str

    def to_dict(self) -> dict:
        return {
            "merged_count": self.merged_count,
            "skipped_count": self.skipped_count,
            "source_files": self.source_files,
            "output_file": self.output_file,
        }


class RunMerger:
    """Merges run records from multiple log files into one output file."""

    def __init__(self, output_file: str) -> None:
        self._output = Path(output_file).expanduser()

    def _load_records(self, path: Path) -> List[dict]:
        if not path.exists():
            return []
        records = []
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    def _write_records(self, records: List[dict]) -> None:
        self._output.parent.mkdir(parents=True, exist_ok=True)
        with self._output.open("w") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")

    def merge(self, source_files: List[str], deduplicate: bool = True) -> MergeResult:
        """Merge records from *source_files* into the output file.

        If *deduplicate* is True, records with the same ``run_id`` are
        included only once (first occurrence wins).
        """
        if not source_files:
            raise MergeError("No source files provided for merge.")

        seen_ids: set = set()
        merged: List[dict] = []
        skipped = 0

        for src in source_files:
            for record in self._load_records(Path(src).expanduser()):
                run_id = record.get("run_id")
                if deduplicate and run_id and run_id in seen_ids:
                    skipped += 1
                    continue
                if run_id:
                    seen_ids.add(run_id)
                merged.append(record)

        self._write_records(merged)
        return MergeResult(
            merged_count=len(merged),
            skipped_count=skipped,
            source_files=[str(s) for s in source_files],
            output_file=str(self._output),
        )
