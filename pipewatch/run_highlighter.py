from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class HighlightError(Exception):
    """Raised when highlighting fails."""


@dataclass
class HighlightResult:
    run_id: str
    pipeline: str
    reason: str
    fields: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "reason": self.reason,
            "fields": self.fields,
        }


class RunHighlighter:
    """Highlights notable runs based on field thresholds or status."""

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

    def by_status(self, status: str) -> list[HighlightResult]:
        """Return all runs matching the given status."""
        results: list[HighlightResult] = []
        for rec in self._load_records():
            if rec.get("status", "").lower() == status.lower():
                results.append(
                    HighlightResult(
                        run_id=rec.get("run_id", ""),
                        pipeline=rec.get("pipeline", ""),
                        reason=f"status={status}",
                        fields={"status": rec.get("status")},
                    )
                )
        return results

    def by_duration_above(self, threshold_seconds: float) -> list[HighlightResult]:
        """Return runs whose duration exceeds the threshold."""
        results: list[HighlightResult] = []
        for rec in self._load_records():
            duration = rec.get("duration_seconds")
            if duration is not None and duration > threshold_seconds:
                results.append(
                    HighlightResult(
                        run_id=rec.get("run_id", ""),
                        pipeline=rec.get("pipeline", ""),
                        reason=f"duration>{threshold_seconds}s",
                        fields={"duration_seconds": duration},
                    )
                )
        return results

    def by_field_value(self, field_name: str, value: Any) -> list[HighlightResult]:
        """Return runs where a specific field matches the given value."""
        results: list[HighlightResult] = []
        for rec in self._load_records():
            if rec.get(field_name) == value:
                results.append(
                    HighlightResult(
                        run_id=rec.get("run_id", ""),
                        pipeline=rec.get("pipeline", ""),
                        reason=f"{field_name}={value}",
                        fields={field_name: rec.get(field_name)},
                    )
                )
        return results
