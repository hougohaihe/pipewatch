from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


class PagerError(Exception):
    """Raised when pagination encounters an invalid argument."""


@dataclass
class PageResult:
    records: List[dict] = field(default_factory=list)
    page: int = 1
    page_size: int = 20
    total_records: int = 0
    total_pages: int = 1

    def to_dict(self) -> dict:
        return {
            "page": self.page,
            "page_size": self.page_size,
            "total_records": self.total_records,
            "total_pages": self.total_pages,
            "records": self.records,
        }


class RunPager:
    """Paginates run log records from a JSONL log file."""

    def __init__(self, log_file: str, page_size: int = 20) -> None:
        if page_size < 1:
            raise PagerError("page_size must be at least 1")
        self._log_file = Path(log_file)
        self._page_size = page_size

    def _load_records(self) -> List[dict]:
        if not self._log_file.exists():
            return []
        records = []
        with self._log_file.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def get_page(
        self,
        page: int = 1,
        pipeline: Optional[str] = None,
    ) -> PageResult:
        if page < 1:
            raise PagerError("page must be at least 1")

        records = self._load_records()

        if pipeline is not None:
            records = [
                r for r in records
                if r.get("pipeline") == pipeline
            ]

        total_records = len(records)
        total_pages = max(1, -(-total_records // self._page_size))  # ceiling div

        start = (page - 1) * self._page_size
        end = start + self._page_size
        page_records = records[start:end]

        return PageResult(
            records=page_records,
            page=page,
            page_size=self._page_size,
            total_records=total_records,
            total_pages=total_pages,
        )
