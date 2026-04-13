"""Validates run records against configurable rules."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class ValidationError(Exception):
    """Raised when validation configuration is invalid."""


@dataclass
class ValidationResult:
    run_id: str
    pipeline: str
    passed: bool
    violations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "passed": self.passed,
            "violations": self.violations,
        }


class RunValidator:
    """Validates run log records against a set of rules."""

    VALID_STATUSES = {"success", "failure", "running"}

    def __init__(self, log_file: str, rules: dict[str, Any] | None = None) -> None:
        self._log_file = Path(log_file)
        self._rules = rules or {}

    def _load_records(self) -> list[dict[str, Any]]:
        if not self._log_file.exists():
            return []
        records = []
        with self._log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def validate_record(self, record: dict[str, Any]) -> ValidationResult:
        violations: list[str] = []
        run_id = record.get("run_id", "<unknown>")
        pipeline = record.get("pipeline", "<unknown>")

        if not record.get("run_id"):
            violations.append("missing required field: run_id")
        if not record.get("pipeline"):
            violations.append("missing required field: pipeline")

        status = record.get("status")
        if status and status not in self.VALID_STATUSES:
            violations.append(f"invalid status value: {status!r}")

        max_duration = self._rules.get("max_duration_seconds")
        if max_duration is not None:
            duration = record.get("duration_seconds")
            if duration is not None and duration > max_duration:
                violations.append(
                    f"duration {duration}s exceeds max {max_duration}s"
                )

        required_fields = self._rules.get("required_fields", [])
        for rf in required_fields:
            if rf not in record:
                violations.append(f"missing required field: {rf}")

        return ValidationResult(
            run_id=run_id,
            pipeline=pipeline,
            passed=len(violations) == 0,
            violations=violations,
        )

    def validate_all(self) -> list[ValidationResult]:
        return [self.validate_record(r) for r in self._load_records()]

    def validate_pipeline(self, pipeline: str) -> list[ValidationResult]:
        return [
            self.validate_record(r)
            for r in self._load_records()
            if r.get("pipeline") == pipeline
        ]
