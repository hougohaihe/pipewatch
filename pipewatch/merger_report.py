"""Human-readable reporting for RunMerger results."""

from __future__ import annotations

import json
from typing import Optional

from pipewatch.run_merger import MergeResult


class MergerReport:
    """Print a summary of a :class:`MergeResult`."""

    def __init__(self, result: MergeResult) -> None:
        self._result = result

    def print_summary(self) -> None:
        r = self._result
        print(f"Merge complete")
        print(f"  Output file   : {r.output_file}")
        print(f"  Source files  : {len(r.source_files)}")
        for src in r.source_files:
            print(f"    - {src}")
        print(f"  Records merged: {r.merged_count}")
        print(f"  Duplicates skipped: {r.skipped_count}")

    def print_json(self) -> None:
        print(json.dumps(self._result.to_dict(), indent=2))
