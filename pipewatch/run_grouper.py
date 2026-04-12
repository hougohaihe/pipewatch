"""Group pipeline run records by a specified field."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional


class GroupError(Exception):
    """Raised when grouping fails due to invalid input."""


class RunGrouper:
    """Groups run log records by a specified field value."""

    ALLOWED_FIELDS = {"pipeline", "status", "run_id"}

    def __init__(self, log_file: str) -> None:
        self.log_file = Path(log_file)

    def _load_records(self) -> List[dict]:
        if not self.log_file.exists():
            return []
        records = []
        with self.log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def group_by(self, field: str) -> Dict[str, List[dict]]:
        """Return records grouped by the given field.

        Args:
            field: The record field to group by.

        Returns:
            A dict mapping each unique field value to a list of records.

        Raises:
            GroupError: If *field* is not an allowed grouping field.
        """
        if field not in self.ALLOWED_FIELDS:
            raise GroupError(
                f"Cannot group by '{field}'. "
                f"Allowed fields: {sorted(self.ALLOWED_FIELDS)}"
            )
        records = self._load_records()
        groups: Dict[str, List[dict]] = defaultdict(list)
        for record in records:
            key = str(record.get(field, "unknown"))
            groups[key].append(record)
        return dict(groups)

    def group_counts(self, field: str) -> Dict[str, int]:
        """Return the count of records per group for *field*."""
        return {k: len(v) for k, v in self.group_by(field).items()}

    def largest_group(self, field: str) -> Optional[str]:
        """Return the key of the group with the most records, or None."""
        counts = self.group_counts(field)
        if not counts:
            return None
        return max(counts, key=lambda k: counts[k])
