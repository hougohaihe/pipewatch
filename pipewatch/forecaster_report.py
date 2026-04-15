from __future__ import annotations

import json
from typing import List

from pipewatch.run_forecaster import PipelineForecast

_TREND_SYMBOLS = {"improving": "↑", "degrading": "↓", "stable": "→"}


class ForecasterReport:
    def __init__(self, forecasts: List[PipelineForecast]) -> None:
        self._forecasts = forecasts

    def print_summary(self) -> None:
        if not self._forecasts:
            print("No forecast data available.")
            return
        print(f"{'Pipeline':<30} {'Samples':>7} {'Avg Dur(s)':>12} {'Pred Dur(s)':>12} {'Success%':>10} {'Trend':>10}")
        print("-" * 85)
        for fc in self._forecasts:
            symbol = _TREND_SYMBOLS.get(fc.trend, "?")
            print(
                f"{fc.pipeline:<30} {fc.sample_size:>7} "
                f"{fc.avg_duration_seconds:>12.2f} {fc.predicted_duration_seconds:>12.2f} "
                f"{fc.predicted_success_rate * 100:>9.1f}% {symbol:>10}"
            )

    def print_json(self) -> None:
        print(json.dumps([fc.to_dict() for fc in self._forecasts], indent=2))

    def print_forecast(self, fc: PipelineForecast) -> None:
        symbol = _TREND_SYMBOLS.get(fc.trend, "?")
        print(f"Pipeline          : {fc.pipeline}")
        print(f"Sample size       : {fc.sample_size}")
        print(f"Avg duration (s)  : {fc.avg_duration_seconds:.4f}")
        print(f"Pred duration (s) : {fc.predicted_duration_seconds:.4f}")
        print(f"Pred success rate : {fc.predicted_success_rate * 100:.1f}%")
        print(f"Trend             : {fc.trend} {symbol}")
