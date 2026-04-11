"""Throttle alert notifications to avoid flooding on repeated failures."""

import json
import time
from pathlib import Path
from typing import Optional


class ThrottlePolicy:
    """Defines how often alerts may fire for a given pipeline."""

    def __init__(self, min_interval_seconds: int = 300):
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be non-negative")
        self.min_interval_seconds = min_interval_seconds

    def to_dict(self) -> dict:
        return {"min_interval_seconds": self.min_interval_seconds}

    def __repr__(self) -> str:
        return f"ThrottlePolicy(min_interval_seconds={self.min_interval_seconds})"


class NotificationThrottle:
    """Tracks last alert times per pipeline and suppresses rapid re-alerts."""

    def __init__(self, state_path: Path, policy: ThrottlePolicy):
        self.state_path = state_path
        self.policy = policy
        self._state: dict = self._load_state()

    def _load_state(self) -> dict:
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text())
            except (json.JSONDecodeError, OSError):
                return {}
        return {}

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self._state, indent=2))

    def should_alert(self, pipeline: str) -> bool:
        """Return True if enough time has passed since the last alert."""
        last_alerted: Optional[float] = self._state.get(pipeline)
        if last_alerted is None:
            return True
        elapsed = time.time() - last_alerted
        return elapsed >= self.policy.min_interval_seconds

    def record_alert(self, pipeline: str) -> None:
        """Record that an alert was sent for the given pipeline."""
        self._state[pipeline] = time.time()
        self._save_state()

    def reset(self, pipeline: str) -> None:
        """Clear the throttle state for a pipeline (e.g. after a success)."""
        if pipeline in self._state:
            del self._state[pipeline]
            self._save_state()

    def last_alert_time(self, pipeline: str) -> Optional[float]:
        """Return the Unix timestamp of the last alert, or None."""
        return self._state.get(pipeline)

    def seconds_until_next_alert(self, pipeline: str) -> float:
        """Return the number of seconds to wait before the next alert is allowed.

        Returns 0.0 if an alert may be sent immediately.
        """
        last_alerted: Optional[float] = self._state.get(pipeline)
        if last_alerted is None:
            return 0.0
        elapsed = time.time() - last_alerted
        remaining = self.policy.min_interval_seconds - elapsed
        return max(0.0, remaining)
