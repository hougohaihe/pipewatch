from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipewatch.notification_throttle import NotificationThrottle


class ThrottleReporter:
    """Formats and prints throttle state information."""

    def __init__(self, throttle: "NotificationThrottle") -> None:
        self._throttle = throttle

    def _build_rows(self) -> list[dict]:
        rows = []
        state = self._throttle._load_state()
        policy = self._throttle.policy
        for key, last_sent in state.items():
            rows.append(
                {
                    "key": key,
                    "last_sent": last_sent,
                    "min_interval_seconds": policy.min_interval_seconds,
                }
            )
        return rows

    def print_summary(self) -> None:
        rows = self._build_rows()
        if not rows:
            print("No throttle state recorded.")
            return
        print(f"{'Key':<40} {'Last Sent':<30} {'Interval (s)'}")
        print("-" * 84)
        for row in rows:
            print(
                f"{row['key']:<40} {row['last_sent']:<30} {row['min_interval_seconds']}"
            )
        print(f"\nTotal entries: {len(rows)}")

    def print_json(self) -> None:
        rows = self._build_rows()
        print(json.dumps(rows, indent=2))

    def print_entry(self, key: str) -> None:
        state = self._throttle._load_state()
        if key not in state:
            print(f"No throttle entry found for key: {key!r}")
            return
        entry = {
            "key": key,
            "last_sent": state[key],
            "min_interval_seconds": self._throttle.policy.min_interval_seconds,
        }
        print(json.dumps(entry, indent=2))
