from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


class PivotError(Exception):
    """Raised when a pivot operation fails."""


@dataclass
class PivotTable:
    row_field: str
    col_field: str
    value_field: str
    rows: List[str] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    cells: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "row_field": self.row_field,
            "col_field": self.col_field,
            "value_field": self.value_field,
            "rows": self.rows,
            "columns": self.columns,
            "cells": self.cells,
        }


class RunPivot:
    """Builds pivot tables from pipeline run logs."""

    def __init__(self, log_file: str) -> None:
        self._log_file = Path(log_file)

    def _load_records(self) -> List[dict]:
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

    def build(
        self,
        row_field: str,
        col_field: str,
        value_field: str,
        agg: str = "count",
    ) -> PivotTable:
        """Build a pivot table aggregating *value_field* by *row_field* x *col_field*.

        Supported aggregation modes: ``count``, ``sum``, ``avg``.
        """
        if agg not in ("count", "sum", "avg"):
            raise PivotError(f"Unsupported aggregation: {agg!r}. Use 'count', 'sum', or 'avg'.")

        records = self._load_records()
        # accumulate raw values per (row, col) pair
        buckets: Dict[str, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))

        for rec in records:
            row_val = str(rec.get(row_field, ""))
            col_val = str(rec.get(col_field, ""))
            raw = rec.get(value_field)
            if raw is not None:
                buckets[row_val][col_val].append(raw)

        all_rows = sorted(buckets.keys())
        all_cols: List[str] = sorted({c for row in buckets.values() for c in row})

        cells: Dict[str, Dict[str, Any]] = {}
        for row_val in all_rows:
            cells[row_val] = {}
            for col_val in all_cols:
                vals = buckets[row_val].get(col_val, [])
                if agg == "count":
                    cells[row_val][col_val] = len(vals)
                elif agg == "sum":
                    cells[row_val][col_val] = sum(float(v) for v in vals) if vals else 0.0
                elif agg == "avg":
                    cells[row_val][col_val] = (
                        sum(float(v) for v in vals) / len(vals) if vals else None
                    )

        return PivotTable(
            row_field=row_field,
            col_field=col_field,
            value_field=value_field,
            rows=all_rows,
            columns=all_cols,
            cells=cells,
        )
