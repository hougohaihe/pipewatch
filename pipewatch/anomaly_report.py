from __future__ import annotations

import json
from typing import Dict, List

from pipewatch.run_anomaly import PipelineAnomaly


class AnomalyReport:
    def __init__(self, results: Dict[str, List[PipelineAnomaly]]) -> None:
        self.results = results

    def print_summary(self) -> None:
        total = sum(len(v) for v in self.results.values())
        flagged = sum(
            sum(1 for a in v if a.is_anomaly) for v in self.results.values()
        )
        print(f"Pipelines analysed : {len(self.results)}")
        print(f"Total runs checked : {total}")
        print(f"Anomalies flagged  : {flagged}")

    def print_json(self) -> None:
        out = {
            pipeline: [a.to_dict() for a in anomalies]
            for pipeline, anomalies in self.results.items()
        }
        print(json.dumps(out, indent=2))

    def print_anomalies(self) -> None:
        for pipeline, anomalies in self.results.items():
            flagged = [a for a in anomalies if a.is_anomaly]
            if not flagged:
                continue
            print(f"\nPipeline: {pipeline}")
            for a in flagged:
                print(
                    f"  run_id={a.run_id}  "
                    f"duration={a.duration_seconds:.2f}s  "
                    f"z={a.z_score:.2f}"
                )
