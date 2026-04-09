"""Retention policy for pruning old pipeline run logs."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional


@dataclass
class RetentionPolicy:
    """Defines how long run log entries should be kept."""

    max_age_days: Optional[int] = None
    max_runs: Optional[int] = None

    def is_valid(self) -> bool:
        return self.max_age_days is not None or self.max_runs is not None


class RetentionManager:
    """Applies a RetentionPolicy to a JSONL run log file."""

    def __init__(self, log_path: str | Path, policy: RetentionPolicy) -> None:
        self.log_path = Path(log_path)
        self.policy = policy

    def _read_records(self) -> List[dict]:
        if not self.log_path.exists():
            return []
        records = []
        with self.log_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    def _write_records(self, records: List[dict]) -> None:
        with self.log_path.open("w", encoding="utf-8") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")

    def _filter_by_age(self, records: List[dict]) -> List[dict]:
        if self.policy.max_age_days is None:
            return records
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=self.policy.max_age_days)
        kept = []
        for rec in records:
            started_at = rec.get("started_at", "")
            try:
                ts = datetime.fromisoformat(started_at)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    kept.append(rec)
            except (ValueError, TypeError):
                kept.append(rec)
        return kept

    def _filter_by_count(self, records: List[dict]) -> List[dict]:
        if self.policy.max_runs is None:
            return records
        return records[-self.policy.max_runs :]

    def prune(self) -> int:
        """Remove records that violate the policy. Returns number of pruned records."""
        original = self._read_records()
        after_age = self._filter_by_age(original)
        after_count = self._filter_by_count(after_age)
        pruned = len(original) - len(after_count)
        if pruned > 0:
            self._write_records(after_count)
        return pruned
