"""Watches a log file for new pipeline run records and triggers callbacks."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Callable, List, Optional


class WatcherError(Exception):
    """Raised when the watcher encounters an unrecoverable error."""


class RunWatcher:
    """Tails a JSONL run log and calls registered handlers on each new record.

    Parameters
    ----------
    log_file:
        Path to the JSONL run log produced by RunLogger.
    poll_interval:
        Seconds to wait between file-size checks (default 1.0).
    """

    def __init__(self, log_file: str, poll_interval: float = 1.0) -> None:
        self.log_file = Path(log_file).expanduser()
        self.poll_interval = poll_interval
        self._handlers: List[Callable[[dict], None]] = []
        self._offset: int = 0
        self._running: bool = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(self, handler: Callable[[dict], None]) -> None:
        """Register a callable that receives each new record as a dict."""
        if not callable(handler):
            raise WatcherError("handler must be callable")
        self._handlers.append(handler)

    def start(self, max_iterations: Optional[int] = None) -> None:
        """Begin polling the log file.  Blocks until *stop* is called.

        Parameters
        ----------
        max_iterations:
            If provided, stop after this many poll cycles (useful for tests).
        """
        self._running = True
        iterations = 0
        while self._running:
            self._poll()
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                break
            time.sleep(self.poll_interval)

    def stop(self) -> None:
        """Signal the polling loop to exit after the current iteration."""
        self._running = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _poll(self) -> None:
        """Read any new lines appended since the last poll."""
        if not self.log_file.exists():
            return
        with self.log_file.open("r", encoding="utf-8") as fh:
            fh.seek(self._offset)
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                self._dispatch(record)
            self._offset = fh.tell()

    def _dispatch(self, record: dict) -> None:
        for handler in self._handlers:
            try:
                handler(record)
            except Exception:  # noqa: BLE001
                pass
