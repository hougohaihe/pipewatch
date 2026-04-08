"""Run logger module for recording pipeline run events with structured output."""

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional


class RunStatus(str, Enum):
    STARTED = "started"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunLogger:
    """Logs pipeline run events to a structured JSONL file."""

    def __init__(self, log_path: str = "pipewatch.log"):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def _write(self, record: dict) -> None:
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def _build_record(
        self,
        pipeline: str,
        status: RunStatus,
        run_id: Optional[str] = None,
        duration_seconds: Optional[float] = None,
        message: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> dict:
        return {
            "run_id": run_id or str(uuid.uuid4()),
            "pipeline": pipeline,
            "status": status.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": duration_seconds,
            "message": message,
            "metadata": metadata or {},
        }

    def log_start(self, pipeline: str, run_id: Optional[str] = None, metadata: Optional[dict] = None) -> str:
        run_id = run_id or str(uuid.uuid4())
        record = self._build_record(pipeline, RunStatus.STARTED, run_id=run_id, metadata=metadata)
        self._write(record)
        return run_id

    def log_success(
        self,
        pipeline: str,
        run_id: str,
        duration_seconds: Optional[float] = None,
        message: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        record = self._build_record(
            pipeline, RunStatus.SUCCESS, run_id=run_id,
            duration_seconds=duration_seconds, message=message, metadata=metadata
        )
        self._write(record)

    def log_failure(
        self,
        pipeline: str,
        run_id: str,
        duration_seconds: Optional[float] = None,
        message: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        record = self._build_record(
            pipeline, RunStatus.FAILED, run_id=run_id,
            duration_seconds=duration_seconds, message=message, metadata=metadata
        )
        self._write(record)

    def read_runs(self) -> list[dict]:
        """Return all logged runs as a list of dicts."""
        if not self.log_path.exists():
            return []
        runs = []
        with self.log_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    runs.append(json.loads(line))
        return runs
