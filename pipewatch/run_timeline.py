from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class TimelineError(Exception):
    pass


@dataclass
class TimelineEvent:
    run_id: str
    pipeline: str
    status: str
    started_at: str
    ended_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_seconds": self.duration_seconds,
            "tags": self.tags,
        }


class RunTimeline:
    def __init__(self, log_file: str) -> None:
        self._log_file = Path(log_file)

    def _load_records(self) -> List[Dict]:
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

    def for_pipeline(self, pipeline: str) -> List[TimelineEvent]:
        records = self._load_records()
        events = []
        for r in records:
            if r.get("pipeline") != pipeline:
                continue
            events.append(self._to_event(r))
        events.sort(key=lambda e: e.started_at)
        return events

    def all_events(self) -> List[TimelineEvent]:
        records = self._load_records()
        events = [self._to_event(r) for r in records]
        events.sort(key=lambda e: e.started_at)
        return events

    def between(self, start: str, end: str) -> List[TimelineEvent]:
        if start > end:
            raise TimelineError(f"start '{start}' must not be after end '{end}'")
        events = self.all_events()
        return [e for e in events if start <= e.started_at <= end]

    @staticmethod
    def _to_event(record: Dict) -> TimelineEvent:
        return TimelineEvent(
            run_id=record.get("run_id", ""),
            pipeline=record.get("pipeline", ""),
            status=record.get("status", ""),
            started_at=record.get("started_at", ""),
            ended_at=record.get("ended_at"),
            duration_seconds=record.get("duration_seconds"),
            tags=record.get("tags", []),
        )
