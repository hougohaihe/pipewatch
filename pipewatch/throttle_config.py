"""Build a NotificationThrottle from a config dict or YAML file."""

from pathlib import Path

import yaml

from pipewatch.notification_throttle import NotificationThrottle, ThrottlePolicy

_DEFAULT_STATE_PATH = Path(".pipewatch/throttle_state.json")
_DEFAULT_INTERVAL = 300


def build_throttle_from_config(
    config: dict,
    state_path: Path = _DEFAULT_STATE_PATH,
) -> NotificationThrottle:
    """Construct a NotificationThrottle from a plain config dict.

    Recognised keys:
        min_interval_seconds (int): seconds between alerts per pipeline.
        state_path (str): path to persist throttle state.
    """
    if not isinstance(config, dict):
        raise TypeError("config must be a dict")

    interval = config.get("min_interval_seconds", _DEFAULT_INTERVAL)
    if not isinstance(interval, int):
        raise TypeError("min_interval_seconds must be an int")
    if interval < 0:
        raise ValueError("min_interval_seconds must be non-negative")

    raw_path = config.get("state_path")
    if raw_path is not None:
        state_path = Path(raw_path)

    policy = ThrottlePolicy(min_interval_seconds=interval)
    return NotificationThrottle(state_path=state_path, policy=policy)


def load_throttle_from_file(
    path: Path,
    state_path: Path = _DEFAULT_STATE_PATH,
) -> NotificationThrottle:
    """Load throttle config from a YAML file and return a NotificationThrottle."""
    raw = yaml.safe_load(Path(path).read_text()) or {}
    throttle_cfg = raw.get("throttle", {})
    return build_throttle_from_config(throttle_cfg, state_path=state_path)
