"""Config builder for RunPinner."""

from __future__ import annotations

import json
import os
from pathlib import Path

from pipewatch.run_pinner import PinError, RunPinner


def build_pinner_from_config(config: dict) -> RunPinner:
    """Build a RunPinner from a configuration dictionary.

    Required keys:
        pin_file (str): Path to the JSON file that stores pinned runs.
    """
    if "pin_file" not in config:
        raise PinError("'pin_file' is required in pinner config.")

    pin_file = config["pin_file"]

    if not isinstance(pin_file, str):
        raise PinError("'pin_file' must be a string.")

    pin_file = pin_file.strip()
    if not pin_file:
        raise PinError("'pin_file' must not be empty.")

    return RunPinner(pin_file=os.path.expanduser(pin_file))


def load_pinner_from_file(path: str) -> RunPinner:
    """Load a RunPinner from a JSON config file."""
    config_path = Path(os.path.expanduser(path))
    if not config_path.exists():
        raise PinError(f"Config file not found: {config_path}")
    try:
        config = json.loads(config_path.read_text())
    except json.JSONDecodeError as exc:
        raise PinError(f"Invalid JSON in config file: {exc}") from exc
    return build_pinner_from_config(config)
