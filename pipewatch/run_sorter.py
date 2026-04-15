from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class SortError(Exception):
    """Raised when sorting fails due to invalid input."""


@dataclass
class SortResult:
    records: list[dict[str, Any]]
    field: str
    reverse: bool
    total: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "reverse": self.reverse,
            "total": self.total,
            "records": self.records,
        }


class RunSorter:
    """Sort pipeline run records by a given field."""

    def __init__(self, log_file: str) -> None:
        self._log_file = Path(log_file)

    def _load_records(self) -> list[dict[str, Any]]:
        if not self._log_file.exists():
            return []
        records = []
        with self._log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def sort_by(
        self,
        field: str,
        reverse: bool = False,
        pipeline: str | None = None,
    ) -> SortResult:
        if not field or not isinstance(field, str):
            raise SortError("field must be a non-empty string")

        records = self._load_records()

        if pipeline is not None:
            records = [r for r in records if r.get("pipeline") == pipeline]

        def _key(record: dict[str, Any]) -> Any:
            value = record.get(field)
            # Push missing values to the end regardless of sort direction
            if value is None:
                return (1, "")
            return (0, value)

        try:
            sorted_records = sorted(records, key=_key, reverse=reverse)
        except TypeError as exc:
            raise SortError(f"Cannot sort by field '{field}': {exc}") from exc

        return SortResult(
            records=sorted_records,
            field=field,
            reverse=reverse,
            total=len(sorted_records),
        )
