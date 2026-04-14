"""Config helpers for RunMerger."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_merger import RunMerger


def build_merger_from_config(config: Dict[str, Any]) -> RunMerger:
    """Build a :class:`RunMerger` from a plain-dict configuration.

    Required keys
    -------------
    ``output_file`` : str
        Destination file that merged records are written to.
    """
    if "output_file" not in config:
        raise KeyError("merger config must include 'output_file'")

    output_file = config["output_file"]
    if not isinstance(output_file, str):
        raise TypeError("'output_file' must be a string")
    if not output_file.strip():
        raise ValueError("'output_file' must not be empty")

    return RunMerger(output_file=output_file)


def load_merger_from_file(config_path: str) -> RunMerger:
    """Load merger config from a JSON file and return a :class:`RunMerger`."""
    path = Path(config_path).expanduser()
    with path.open() as fh:
        config = json.load(fh)
    return build_merger_from_config(config)
