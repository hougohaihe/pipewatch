"""Snapshot module: capture and restore point-in-time copies of run log state."""

from __future__ import annotations

import gzip
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


class SnapshotError(Exception):
    """Raised when a snapshot operation fails."""


@dataclass
class SnapshotManifest:
    snapshot_id: str
    created_at: str
    source_log: str
    record_count: int
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "snapshot_id": self.snapshot_id,
            "created_at": self.created_at,
            "source_log": self.source_log,
            "record_count": self.record_count,
            "tags": self.tags,
        }


class RunSnapshot:
    """Captures and restores snapshots of the run log."""

    def __init__(self, log_file: str, snapshot_dir: str) -> None:
        self.log_file = Path(log_file)
        self.snapshot_dir = Path(snapshot_dir)

    def _snapshot_path(self, snapshot_id: str) -> Path:
        return self.snapshot_dir / f"{snapshot_id}.jsonl.gz"

    def _manifest_path(self, snapshot_id: str) -> Path:
        return self.snapshot_dir / f"{snapshot_id}.manifest.json"

    def _load_records(self) -> List[dict]:
        if not self.log_file.exists():
            return []
        records = []
        with self.log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def capture(self, snapshot_id: Optional[str] = None, tags: Optional[List[str]] = None) -> SnapshotManifest:
        """Write a compressed snapshot of the current log file."""
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        if snapshot_id is None:
            snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        records = self._load_records()
        snap_path = self._snapshot_path(snapshot_id)
        if snap_path.exists():
            raise SnapshotError(f"Snapshot '{snapshot_id}' already exists.")
        with gzip.open(snap_path, "wt", encoding="utf-8") as gz:
            for record in records:
                gz.write(json.dumps(record) + "\n")
        manifest = SnapshotManifest(
            snapshot_id=snapshot_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            source_log=str(self.log_file),
            record_count=len(records),
            tags=tags or [],
        )
        with self._manifest_path(snapshot_id).open("w") as mf:
            json.dump(manifest.to_dict(), mf, indent=2)
        return manifest

    def restore(self, snapshot_id: str) -> int:
        """Overwrite the log file with records from the snapshot. Returns record count."""
        snap_path = self._snapshot_path(snapshot_id)
        if not snap_path.exists():
            raise SnapshotError(f"Snapshot '{snapshot_id}' not found.")
        records = []
        with gzip.open(snap_path, "rt", encoding="utf-8") as gz:
            for line in gz:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("w") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")
        return len(records)

    def list_snapshots(self) -> List[SnapshotManifest]:
        """Return all available snapshot manifests sorted by creation time."""
        if not self.snapshot_dir.exists():
            return []
        manifests = []
        for path in self.snapshot_dir.glob("*.manifest.json"):
            with path.open() as fh:
                data = json.load(fh)
            manifests.append(SnapshotManifest(**data))
        return sorted(manifests, key=lambda m: m.created_at)
