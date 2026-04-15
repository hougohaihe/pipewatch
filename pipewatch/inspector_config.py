from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from pipewatch.run_inspector import InspectorError, RunInspector


def build_inspector_from_config(config: Dict[str, Any]) -> RunInspector:
    log_file = config.get("log_file")
    if log_file is None:
        raise InspectorError("'log_file' is required in inspector config")
    if not isinstance(log_file, str):
        raise InspectorError("'log_file' must be a string")
    if not log_file.strip():
        raise InspectorError("'log_file' must not be empty")
    resolved = str(Path(log_file).expanduser())
    return RunInspector(log_file=resolved)


def load_inspector_from_file(config_path: str) -> RunInspector:
    path = Path(config_path).expanduser()
    if not path.exists():
        raise InspectorError(f"Config file not found: {config_path}")
    with path.open() as fh:
        config = json.load(fh)
    return build_inspector_from_config(config.get("inspector", config))
