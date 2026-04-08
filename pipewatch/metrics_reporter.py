"""Formats and outputs metrics summaries for CLI display."""

import json
from typing import IO
from pipewatch.metrics_collector import MetricsCollector


class MetricsReporter:
    """Renders metrics from a MetricsCollector to various output formats."""

    def __init__(self, collector: MetricsCollector, output: IO = None):
        import sys
        self._collector = collector
        self._output = output or sys.stdout

    def print_summary(self) -> None:
        summary = self._collector.summary()
        lines = [
            "=== Pipeline Run Summary ===",
            f"  Total runs       : {summary['total_runs']}",
            f"  Completed runs   : {summary['completed_runs']}",
            f"  Avg duration (s) : {summary['avg_duration_seconds'] or 'N/A'}",
            f"  Records processed: {summary['total_records_processed']}",
            f"  Total errors     : {summary['total_errors']}",
        ]
        self._output.write("\n".join(lines) + "\n")

    def print_run(self, run_id: str) -> None:
        metrics = self._collector.get(run_id)
        if metrics is None:
            self._output.write(f"No metrics found for run_id: {run_id}\n")
            return
        d = metrics.to_dict()
        lines = [
            f"--- Run: {run_id} ---",
            f"  Pipeline         : {d['pipeline_name']}",
            f"  Duration (s)     : {d['duration_seconds'] or 'in-progress'}",
            f"  Records processed: {d['records_processed']}",
            f"  Errors           : {d['errors_encountered']}",
        ]
        if d["extra"]:
            lines.append(f"  Extra            : {d['extra']}")
        self._output.write("\n".join(lines) + "\n")

    def print_all_json(self) -> None:
        data = self._collector.all_metrics()
        self._output.write(json.dumps(data, indent=2) + "\n")

    def print_summary_json(self) -> None:
        summary = self._collector.summary()
        self._output.write(json.dumps(summary, indent=2) + "\n")
