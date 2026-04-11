"""Config loader for RunTagger / TagIndex."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_tagger import TagIndex


def build_tag_index_from_config(config: Dict[str, Any]) -> TagIndex:
    """Build a TagIndex from a config dict.

    Expected keys:
        index_file (str, required): path to the JSON tag index file.

    Raises:
        KeyError: if 'index_file' is missing from the config.
        TypeError: if 'index_file' is not a string.
        ValueError: if 'index_file' is an empty or whitespace-only string.
        FileNotFoundError: if the resolved index file path does not exist.
    """
    if "index_file" not in config:
        raise KeyError("tagger config must include 'index_file'")

    index_file = config["index_file"]
    if not isinstance(index_file, str):
        raise TypeError("'index_file' must be a string")
    if not index_file.strip():
        raise ValueError("'index_file' must not be empty")

    resolved = Path(index_file).expanduser()
    if not resolved.exists():
        raise FileNotFoundError(f"Tag index file not found: {resolved}")

    return TagIndex(index_file=resolved)


def load_tag_index_from_file(config_path: str) -> TagIndex:
    """Load tagger config from a JSON file and return a TagIndex."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with path.open() as fh:
        config = json.load(fh)
    tagger_config = config.get("tagger", {})
    return build_tag_index_from_config(tagger_config)
