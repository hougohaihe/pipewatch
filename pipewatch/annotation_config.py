"""Load RunAnnotator configuration from a dict or YAML file."""

from __future__ import annotations

import os
from typing import Any

from pipewatch.run_annotator import RunAnnotator


def build_annotator_from_config(config: dict[str, Any]) -> RunAnnotator:
    """Construct a RunAnnotator from a plain config dict.

    Expected keys:
        log_file (str, required): path to the JSONL run log.
    """
    log_file = config.get("log_file")
    if not log_file:
        raise ValueError("annotation config must include 'log_file'")
    if not isinstance(log_file, str):
        raise TypeError("'log_file' must be a string")
    return RunAnnotator(log_file=os.path.expanduser(log_file))


def load_annotator_from_file(path: str) -> RunAnnotator:
    """Load annotator config from a YAML file and return a RunAnnotator."""
    try:
        import yaml  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise ImportError("PyYAML is required to load config files") from exc

    with open(path, "r") as fh:
        raw = yaml.safe_load(fh) or {}

    section = raw.get("annotation", raw)
    return build_annotator_from_config(section)
