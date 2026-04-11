"""Archive completed pipeline run logs to a compressed file."""

import gzip
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


class ArchiveError(Exception):
    """Raised when an archive operation fails."""


class RunArchiver:
    """Archives run log records to a gzip-compressed JSONL file."""

    def __init__(self, log_file: str, archive_dir: str):
        self.log_file = Path(log_file)
        self.archive_dir = Path(archive_dir)

    def _load_records(self) -> List[dict]:
        if not self.log_file.exists():
            return []
        records = []
        with open(self.log_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def _write_records(self, records: List[dict]) -> None:
        with open(self.log_file, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

    def archive(self, pipeline: Optional[str] = None, before_days: Optional[int] = None) -> Path:
        """Archive matching records and remove them from the log file.

        Returns the path to the created archive file.
        """
        records = self._load_records()
        if not records:
            raise ArchiveError("No records found to archive.")

        cutoff = None
        if before_days is not None:
            from datetime import timedelta
            cutoff = (datetime.now(timezone.utc) - timedelta(days=before_days)).isoformat()

        to_archive = []
        to_keep = []
        for record in records:
            match = True
            if pipeline and record.get("pipeline") != pipeline:
                match = False
            if cutoff and record.get("started_at", "") >= cutoff:
                match = False
            (to_archive if match else to_keep).append(record)

        if not to_archive:
            raise ArchiveError("No records matched the archive criteria.")

        self.archive_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = f"_{pipeline}" if pipeline else ""
        archive_path = self.archive_dir / f"archive{suffix}_{timestamp}.jsonl.gz"

        with gzip.open(archive_path, "wt", encoding="utf-8") as gz:
            for record in to_archive:
                gz.write(json.dumps(record) + "\n")

        self._write_records(to_keep)
        return archive_path

    def list_archives(self) -> List[Path]:
        """Return sorted list of archive files in archive_dir."""
        if not self.archive_dir.exists():
            return []
        return sorted(self.archive_dir.glob("*.jsonl.gz"))

    def restore(self, archive_path: str) -> int:
        """Restore records from an archive back into the log file. Returns record count."""
        path = Path(archive_path)
        if not path.exists():
            raise ArchiveError(f"Archive not found: {archive_path}")

        records = []
        with gzip.open(path, "rt", encoding="utf-8") as gz:
            for line in gz:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

        with open(self.log_file, "a") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

        return len(records)
