"""RunPinner — pin specific run IDs for quick reference and retrieval."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


class PinError(Exception):
    """Raised when a pinning operation fails."""


@dataclass
class PinEntry:
    run_id: str
    label: str
    pipeline: Optional[str] = None

    def to_dict(self) -> Dict:
        return {"run_id": self.run_id, "label": self.label, "pipeline": self.pipeline}


class RunPinner:
    """Manages a persistent set of pinned run IDs stored in a JSON file."""

    def __init__(self, pin_file: str) -> None:
        self.pin_file = Path(os.path.expanduser(pin_file))
        self._pins: Dict[str, PinEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self.pin_file.exists():
            return
        try:
            raw = json.loads(self.pin_file.read_text())
            self._pins = {
                k: PinEntry(**v) for k, v in raw.items()
            }
        except (json.JSONDecodeError, TypeError) as exc:
            raise PinError(f"Failed to load pin file: {exc}") from exc

    def _save(self) -> None:
        self.pin_file.parent.mkdir(parents=True, exist_ok=True)
        self.pin_file.write_text(
            json.dumps({k: v.to_dict() for k, v in self._pins.items()}, indent=2)
        )

    def pin(self, run_id: str, label: str, pipeline: Optional[str] = None) -> PinEntry:
        """Pin a run ID with an optional label and pipeline name."""
        if not run_id or not isinstance(run_id, str):
            raise PinError("run_id must be a non-empty string.")
        if not label or not isinstance(label, str):
            raise PinError("label must be a non-empty string.")
        entry = PinEntry(run_id=run_id, label=label, pipeline=pipeline)
        self._pins[run_id] = entry
        self._save()
        return entry

    def unpin(self, run_id: str) -> bool:
        """Remove a pinned run. Returns True if it existed, False otherwise."""
        if run_id in self._pins:
            del self._pins[run_id]
            self._save()
            return True
        return False

    def get(self, run_id: str) -> Optional[PinEntry]:
        """Retrieve a pinned entry by run_id."""
        return self._pins.get(run_id)

    def all(self) -> List[PinEntry]:
        """Return all pinned entries."""
        return list(self._pins.values())

    def by_pipeline(self, pipeline: str) -> List[PinEntry]:
        """Return pins filtered by pipeline name."""
        return [p for p in self._pins.values() if p.pipeline == pipeline]
