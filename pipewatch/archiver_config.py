"""Build a RunArchiver from a config dict or file."""

import json
import os
from pathlib import Path

from pipewatch.run_archiver import RunArchiver


def build_archiver_from_config(config: dict) -> RunArchiver:
    """Construct a RunArchiver from a configuration dictionary.

    Required keys:
        log_file (str): path to the JSONL run log.
        archive_dir (str): directory where archives will be stored.
    """
    if "log_file" not in config:
        raise KeyError("archiver config must include 'log_file'")
    if "archive_dir" not in config:
        raise KeyError("archiver config must include 'archive_dir'")

    log_file = config["log_file"]
    archive_dir = config["archive_dir"]

    if not isinstance(log_file, str) or not log_file.strip():
        raise ValueError("'log_file' must be a non-empty string")
    if not isinstance(archive_dir, str) or not archive_dir.strip():
        raise ValueError("'archive_dir' must be a non-empty string")

    return RunArchiver(
        log_file=os.path.expanduser(log_file),
        archive_dir=os.path.expanduser(archive_dir),
    )


def load_archiver_from_file(path: str) -> RunArchiver:
    """Load archiver config from a JSON file and return a RunArchiver."""
    config_path = Path(os.path.expanduser(path))
    if not config_path.exists():
        raise FileNotFoundError(f"Archiver config file not found: {path}")
    with open(config_path, "r") as f:
        config = json.load(f)
    return build_archiver_from_config(config)
