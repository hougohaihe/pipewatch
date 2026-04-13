"""RunTrimmer: truncate run log fields to enforce size constraints."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class TrimError(Exception):
    """Raised when trimming fails."""


@dataclass
class TrimResult:
    trimmed_count: int
    total_records: int
    fields_truncated: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "trimmed_count": self.trimmed_count,
            "total_records": self.total_records,
            "fields_truncated": self.fields_truncated,
        }


class RunTrimmer:
    """Truncates string fields in run log records beyond a maximum length."""

    DEFAULT_MAX_LENGTH = 256

    def __init__(
        self,
        log_file: str,
        max_length: int = DEFAULT_MAX_LENGTH,
        fields: Optional[List[str]] = None,
    ) -> None:
        if not isinstance(log_file, str) or not log_file.strip():
            raise TrimError("log_file must be a non-empty string")
        if max_length < 1:
            raise TrimError("max_length must be at least 1")
        self.log_file = os.path.expanduser(log_file)
        self.max_length = max_length
        self.fields = fields  # None means all string fields

    def _load_records(self) -> List[Dict]:
        if not os.path.exists(self.log_file):
            return []
        with open(self.log_file, "r") as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def _save_records(self, records: List[Dict]) -> None:
        with open(self.log_file, "w") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")

    def trim(self) -> TrimResult:
        """Truncate oversized string fields in all records and persist."""
        records = self._load_records()
        trimmed_count = 0
        affected_fields: set = set()

        for record in records:
            record_modified = False
            target_keys = self.fields if self.fields is not None else list(record.keys())
            for key in target_keys:
                value = record.get(key)
                if isinstance(value, str) and len(value) > self.max_length:
                    record[key] = value[: self.max_length]
                    affected_fields.add(key)
                    record_modified = True
            if record_modified:
                trimmed_count += 1

        if trimmed_count > 0:
            self._save_records(records)

        return TrimResult(
            trimmed_count=trimmed_count,
            total_records=len(records),
            fields_truncated=sorted(affected_fields),
        )
