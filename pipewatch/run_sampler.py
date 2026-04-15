from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


class SamplerError(Exception):
    pass


@dataclass
class SampleResult:
    pipeline: Optional[str]
    total_records: int
    sample_size: int
    records: List[dict] = field(default_factory=list)
    seed: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total_records": self.total_records,
            "sample_size": self.sample_size,
            "seed": self.seed,
            "records": self.records,
        }


class RunSampler:
    def __init__(self, log_file: str) -> None:
        self._log_file = Path(log_file)

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
                        pass
        return records

    def sample(
        self,
        n: int,
        pipeline: Optional[str] = None,
        seed: Optional[int] = None,
    ) -> SampleResult:
        if n < 1:
            raise SamplerError(f"Sample size must be at least 1, got {n}")

        records = self._load_records()
        if pipeline is not None:
            records = [r for r in records if r.get("pipeline") == pipeline]

        total = len(records)
        rng = random.Random(seed)
        chosen = rng.sample(records, min(n, total)) if total > 0 else []

        return SampleResult(
            pipeline=pipeline,
            total_records=total,
            sample_size=len(chosen),
            records=chosen,
            seed=seed,
        )
