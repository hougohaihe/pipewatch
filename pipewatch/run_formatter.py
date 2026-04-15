from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class FormatError(Exception):
    """Raised when formatting fails."""


@dataclass
class FormattedRun:
    run_id: str
    pipeline: str
    status: str
    fields: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "status": self.status,
            **self.fields,
        }


class RunFormatter:
    """Formats run records into human-readable or structured output."""

    REQUIRED_FIELDS = ("run_id", "pipeline", "status")

    def __init__(self, log_file: str, template: Optional[str] = None) -> None:
        if not isinstance(log_file, str) or not log_file.strip():
            raise ValueError("log_file must be a non-empty string")
        self.log_file = log_file
        self.template = template or "{run_id} | {pipeline} | {status}"

    def _load_records(self) -> List[Dict[str, Any]]:
        try:
            with open(self.log_file, "r") as fh:
                return [json.loads(line) for line in fh if line.strip()]
        except FileNotFoundError:
            return []

    def _validate(self, record: Dict[str, Any]) -> None:
        missing = [f for f in self.REQUIRED_FIELDS if f not in record]
        if missing:
            raise FormatError(f"Record missing required fields: {missing}")

    def format_record(self, record: Dict[str, Any]) -> FormattedRun:
        self._validate(record)
        extra = {k: v for k, v in record.items() if k not in self.REQUIRED_FIELDS}
        return FormattedRun(
            run_id=record["run_id"],
            pipeline=record["pipeline"],
            status=record["status"],
            fields=extra,
        )

    def render(self, record: Dict[str, Any]) -> str:
        fmt = self.format_record(record)
        try:
            return self.template.format(**fmt.to_dict())
        except KeyError as exc:
            raise FormatError(f"Template references unknown field: {exc}") from exc

    def format_all(self, pipeline: Optional[str] = None) -> List[FormattedRun]:
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]
        return [self.format_record(r) for r in records]

    def render_all(self, pipeline: Optional[str] = None) -> List[str]:
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]
        return [self.render(r) for r in records]
