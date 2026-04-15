from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class BookmarkError(Exception):
    pass


@dataclass
class BookmarkEntry:
    run_id: str
    pipeline: str
    label: str
    note: str = ""
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "label": self.label,
            "note": self.note,
            "tags": self.tags,
        }


class RunBookmark:
    def __init__(self, bookmark_file: str) -> None:
        self._path = bookmark_file
        self._entries: Dict[str, BookmarkEntry] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path, "r") as fh:
            raw = json.load(fh)
        for run_id, data in raw.items():
            self._entries[run_id] = BookmarkEntry(**data)

    def _save(self) -> None:
        with open(self._path, "w") as fh:
            json.dump({k: v.to_dict() for k, v in self._entries.items()}, fh, indent=2)

    def add(self, run_id: str, pipeline: str, label: str, note: str = "", tags: Optional[List[str]] = None) -> BookmarkEntry:
        if not run_id or not run_id.strip():
            raise BookmarkError("run_id must not be empty")
        if not label or not label.strip():
            raise BookmarkError("label must not be empty")
        entry = BookmarkEntry(
            run_id=run_id,
            pipeline=pipeline,
            label=label,
            note=note,
            tags=tags or [],
        )
        self._entries[run_id] = entry
        self._save()
        return entry

    def remove(self, run_id: str) -> bool:
        if run_id not in self._entries:
            return False
        del self._entries[run_id]
        self._save()
        return True

    def get(self, run_id: str) -> Optional[BookmarkEntry]:
        return self._entries.get(run_id)

    def all(self) -> List[BookmarkEntry]:
        return list(self._entries.values())

    def by_label(self, label: str) -> List[BookmarkEntry]:
        return [e for e in self._entries.values() if e.label.lower() == label.lower()]

    def by_pipeline(self, pipeline: str) -> List[BookmarkEntry]:
        return [e for e in self._entries.values() if e.pipeline == pipeline]
