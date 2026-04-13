from __future__ import annotations

import json
from typing import Dict, List

from pipewatch.run_aggregator import AggregatedBucket, RunAggregator


class AggregatorReport:
    """Renders aggregated pipeline run data as human-readable or JSON output."""

    def __init__(self, aggregator: RunAggregator) -> None:
        self._aggregator = aggregator

    def print_summary(self, field_name: str) -> None:
        buckets = self._aggregator.aggregate_by(field_name)
        if not buckets:
            print(f"No records found to aggregate by '{field_name}'.")
            return
        print(f"\nAggregation by '{field_name}':")
        print(f"  {'Key':<25} {'Runs':>6} {'Success':>8} {'Failure':>8} {'Avg Dur(s)':>12} {'Rate':>8}")
        print("  " + "-" * 72)
        for key in sorted(buckets):
            b = buckets[key]
            avg = f"{b.avg_duration:.2f}" if b.avg_duration is not None else "N/A"
            rate = f"{b.success_rate:.2%}" if b.success_rate is not None else "N/A"
            print(f"  {key:<25} {b.run_count:>6} {b.success_count:>8} {b.failure_count:>8} {avg:>12} {rate:>8}")
        print()

    def print_json(self, field_name: str) -> None:
        summary = self._aggregator.summary(field_name)
        print(json.dumps(summary, indent=2))

    def print_bucket(self, field_name: str, key: str) -> None:
        buckets = self._aggregator.aggregate_by(field_name)
        if key not in buckets:
            print(f"No data found for {field_name}='{key}'.")
            return
        bucket = buckets[key]
        data = bucket.to_dict()
        print(f"\nBucket: {field_name}='{key}'")
        for k, v in data.items():
            print(f"  {k}: {v}")
        print()
