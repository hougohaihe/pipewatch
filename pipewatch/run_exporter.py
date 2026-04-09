"""Export pipeline run records to various formats (CSV, JSON)."""

import csv
import json
import io
from typing import List, Dict, Any, Optional


class RunExporter:
    """Exports run records from a log file to structured formats."""

    def __init__(self, log_path: str):
        self.log_path = log_path

    def _load_records(self) -> List[Dict[str, Any]]:
        records = []
        try:
            with open(self.log_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        except FileNotFoundError:
            pass
        return records

    def to_json(self, pipeline: Optional[str] = None, indent: int = 2) -> str:
        """Return records as a JSON string, optionally filtered by pipeline."""
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]
        return json.dumps(records, indent=indent)

    def to_csv(self, pipeline: Optional[str] = None) -> str:
        """Return records as a CSV string, optionally filtered by pipeline."""
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]
        if not records:
            return ""

        fieldnames = list(records[0].keys())
        for record in records[1:]:
            for key in record.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        output = io.StringIO()
        writer = csv.DictWriter(
            output, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(records)
        return output.getvalue()

    def write_json(self, dest_path: str, pipeline: Optional[str] = None) -> int:
        """Write JSON export to a file. Returns number of records written."""
        records = self._load_records()
        if pipeline:
            records = [r for r in records if r.get("pipeline") == pipeline]
        with open(dest_path, "w") as f:
            json.dump(records, f, indent=2)
        return len(records)

    def write_csv(self, dest_path: str, pipeline: Optional[str] = None) -> int:
        """Write CSV export to a file. Returns number of records written."""
        csv_content = self.to_csv(pipeline=pipeline)
        record_count = len(self._load_records()) if not pipeline else len(
            [r for r in self._load_records() if r.get("pipeline") == pipeline]
        )
        with open(dest_path, "w") as f:
            f.write(csv_content)
        return record_count
