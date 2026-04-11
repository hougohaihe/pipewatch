"""Tag-based grouping and lookup for pipeline runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional


class TagIndex:
    """Maintains an index of tags -> run_ids for fast lookup."""

    def __init__(self, index_file: str) -> None:
        self.index_file = Path(index_file)
        self._index: Dict[str, List[str]] = self._load()

    def _load(self) -> Dict[str, List[str]]:
        if not self.index_file.exists():
            return {}
        with self.index_file.open() as fh:
            return json.load(fh)

    def _save(self) -> None:
        self.index_file.parent.mkdir(parents=True, exist_ok=True)
        with self.index_file.open("w") as fh:
            json.dump(self._index, fh, indent=2)

    def add(self, tag: str, run_id: str) -> None:
        """Associate a run_id with a tag."""
        if tag not in self._index:
            self._index[tag] = []
        if run_id not in self._index[tag]:
            self._index[tag].append(run_id)
        self._save()

    def remove(self, tag: str, run_id: str) -> bool:
        """Remove a run_id from a tag. Returns True if removed."""
        if tag not in self._index or run_id not in self._index[tag]:
            return False
        self._index[tag].remove(run_id)
        if not self._index[tag]:
            del self._index[tag]
        self._save()
        return True

    def runs_for_tag(self, tag: str) -> List[str]:
        """Return all run_ids associated with a tag."""
        return list(self._index.get(tag, []))

    def tags_for_run(self, run_id: str) -> List[str]:
        """Return all tags associated with a run_id."""
        return [tag for tag, ids in self._index.items() if run_id in ids]

    def all_tags(self) -> List[str]:
        """Return all known tags."""
        return list(self._index.keys())

    def clear_tag(self, tag: str) -> int:
        """Remove a tag entirely. Returns number of run_ids removed."""
        removed = len(self._index.pop(tag, []))
        if removed:
            self._save()
        return removed
