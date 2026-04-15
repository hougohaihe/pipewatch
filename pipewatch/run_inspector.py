from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


class InspectorError(Exception):
    pass


@dataclass
class InspectionResult:
    run_id: str
    pipeline: str
    fields: Dict[str, Any]
    missing_fields: List[str]
    extra_fields: List[str]
    is_valid: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "fields": self.fields,
            "missing_fields": self.missing_fields,
            "extra_fields": self.extra_fields,
            "is_valid": self.is_valid,
        }


REQUIRED_FIELDS = {"run_id", "pipeline", "status", "start_time"}
KNOWN_FIELDS = REQUIRED_FIELDS | {"end_time", "duration_seconds", "error", "tags", "labels", "note"}


class RunInspector:
    def __init__(self, log_file: str) -> None:
        self._log_file = Path(log_file)

    def _load_records(self) -> List[Dict[str, Any]]:
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

    def inspect(self, run_id: str) -> Optional[InspectionResult]:
        for record in self._load_records():
            if record.get("run_id") == run_id:
                return self._build_result(record)
        return None

    def inspect_all(self) -> List[InspectionResult]:
        return [self._build_result(r) for r in self._load_records()]

    def inspect_pipeline(self, pipeline: str) -> List[InspectionResult]:
        return [
            self._build_result(r)
            for r in self._load_records()
            if r.get("pipeline") == pipeline
        ]

    def _build_result(self, record: Dict[str, Any]) -> InspectionResult:
        present = set(record.keys())
        missing = sorted(REQUIRED_FIELDS - present)
        extra = sorted(present - KNOWN_FIELDS)
        return InspectionResult(
            run_id=record.get("run_id", ""),
            pipeline=record.get("pipeline", ""),
            fields=dict(record),
            missing_fields=missing,
            extra_fields=extra,
            is_valid=len(missing) == 0,
        )
