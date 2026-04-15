from __future__ import annotations

import json
from typing import Any

from pipewatch.run_highlighter import HighlightResult


class HighlighterReport:
    """Formats and prints highlight results."""

    def __init__(self, results: list[HighlightResult]) -> None:
        self._results = results

    def print_summary(self) -> None:
        """Print a human-readable summary of highlighted runs."""
        if not self._results:
            print("No highlighted runs found.")
            return
        print(f"Highlighted runs ({len(self._results)}):")
        for r in self._results:
            print(f"  [{r.pipeline}] run_id={r.run_id}  reason={r.reason}")

    def print_json(self) -> None:
        """Print highlighted runs as JSON."""
        data: list[dict[str, Any]] = [r.to_dict() for r in self._results]
        print(json.dumps(data, indent=2))

    def print_run(self, result: HighlightResult) -> None:
        """Print a single highlight result."""
        print(
            f"run_id={result.run_id}  pipeline={result.pipeline}  "
            f"reason={result.reason}  fields={result.fields}"
        )
