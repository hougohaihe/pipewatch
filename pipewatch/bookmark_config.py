from __future__ import annotations

import json
import os
from typing import Any, Dict

from pipewatch.run_bookmark import RunBookmark


def build_bookmark_from_config(config: Dict[str, Any]) -> RunBookmark:
    bookmark_file = config.get("bookmark_file")
    if bookmark_file is None:
        raise KeyError("bookmark_file is required")
    if not isinstance(bookmark_file, str):
        raise TypeError("bookmark_file must be a string")
    if not bookmark_file.strip():
        raise ValueError("bookmark_file must not be empty")
    bookmark_file = os.path.expanduser(bookmark_file)
    return RunBookmark(bookmark_file=bookmark_file)


def load_bookmark_from_file(config_path: str) -> RunBookmark:
    with open(config_path, "r") as fh:
        config = json.load(fh)
    return build_bookmark_from_config(config)
