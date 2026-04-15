from __future__ import annotations

import json
from typing import List, Optional

from pipewatch.run_bookmark import BookmarkEntry, RunBookmark


class BookmarkReport:
    def __init__(self, bookmark: RunBookmark) -> None:
        self._bookmark = bookmark

    def print_summary(self, pipeline: Optional[str] = None) -> None:
        entries: List[BookmarkEntry] = (
            self._bookmark.by_pipeline(pipeline)
            if pipeline
            else self._bookmark.all()
        )
        total = len(entries)
        print(f"Bookmarks: {total}")
        if not entries:
            print("  (none)")
            return
        for entry in entries:
            tags_str = ", ".join(entry.tags) if entry.tags else "-"
            print(f"  [{entry.label}] {entry.run_id} ({entry.pipeline}) tags={tags_str}")
            if entry.note:
                print(f"    note: {entry.note}")

    def print_json(self, pipeline: Optional[str] = None) -> None:
        entries: List[BookmarkEntry] = (
            self._bookmark.by_pipeline(pipeline)
            if pipeline
            else self._bookmark.all()
        )
        print(json.dumps([e.to_dict() for e in entries], indent=2))

    def print_entry(self, run_id: str) -> None:
        entry = self._bookmark.get(run_id)
        if entry is None:
            print(f"No bookmark found for run_id: {run_id}")
            return
        print(json.dumps(entry.to_dict(), indent=2))
