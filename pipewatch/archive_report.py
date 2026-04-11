"""Generate human-readable reports about archived pipeline run data."""

import gzip
import json
from pathlib import Path
from typing import List

from pipewatch.run_archiver import RunArchiver


class ArchiveReport:
    """Summarise contents of archive files produced by RunArchiver."""

    def __init__(self, archiver: RunArchiver):
        self.archiver = archiver

    def _read_archive(self, path: Path) -> List[dict]:
        records = []
        with gzip.open(path, "rt", encoding="utf-8") as gz:
            for line in gz:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def summary(self) -> dict:
        """Return a summary dict: total archives, total records, pipelines seen."""
        archives = self.archiver.list_archives()
        total_records = 0
        pipelines: set = set()
        for path in archives:
            records = self._read_archive(path)
            total_records += len(records)
            for r in records:
                if "pipeline" in r:
                    pipelines.add(r["pipeline"])
        return {
            "total_archives": len(archives),
            "total_records": total_records,
            "pipelines": sorted(pipelines),
        }

    def print_summary(self) -> None:
        """Print a human-readable summary to stdout."""
        info = self.summary()
        print(f"Archives : {info['total_archives']}")
        print(f"Records  : {info['total_records']}")
        pipelines = ", ".join(info["pipelines"]) if info["pipelines"] else "(none)"
        print(f"Pipelines: {pipelines}")

    def list_archive_info(self) -> List[dict]:
        """Return per-archive metadata: filename, record count, pipelines."""
        result = []
        for path in self.archiver.list_archives():
            records = self._read_archive(path)
            pipelines = sorted({r["pipeline"] for r in records if "pipeline" in r})
            result.append({
                "file": path.name,
                "records": len(records),
                "pipelines": pipelines,
            })
        return result
