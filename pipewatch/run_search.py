"""Full-text and field-based search over pipeline run records."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable


class RunSearch:
    """Search pipeline run records by field values or free-text query."""

    def __init__(self, log_file: str) -> None:
        self._log_file = Path(log_file)

    def _load_records(self) -> list[dict[str, Any]]:
        if not self._log_file.exists():
            return []
        records: list[dict[str, Any]] = []
        with self._log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def by_field(self, field: str, value: Any) -> list[dict[str, Any]]:
        """Return records where *field* equals *value* (case-insensitive for strings)."""
        results = []
        for record in self._load_records():
            record_val = record.get(field)
            if isinstance(record_val, str) and isinstance(value, str):
                if record_val.lower() == value.lower():
                    results.append(record)
            elif record_val == value:
                results.append(record)
        return results

    def by_fields(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Return records matching ALL provided field=value pairs."""
        records = self._load_records()
        for field, value in kwargs.items():
            records = [
                r for r in records
                if (
                    r.get(field, "").lower() == value.lower()
                    if isinstance(r.get(field), str) and isinstance(value, str)
                    else r.get(field) == value
                )
            ]
        return records

    def text_search(self, query: str) -> list[dict[str, Any]]:
        """Return records where any string field contains *query* (case-insensitive)."""
        query_lower = query.lower()
        results = []
        for record in self._load_records():
            if any(
                query_lower in str(v).lower()
                for v in record.values()
            ):
                results.append(record)
        return results

    def where(self, predicate: Callable[[dict[str, Any]], bool]) -> list[dict[str, Any]]:
        """Return records for which *predicate* returns True."""
        return [r for r in self._load_records() if predicate(r)]
