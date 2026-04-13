"""Config loader for RunValidator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipewatch.run_validator import RunValidator, ValidationError


def build_validator_from_config(config: dict[str, Any]) -> RunValidator:
    """Build a RunValidator from a config dict."""
    log_file = config.get("log_file")
    if not log_file:
        raise ValidationError("'log_file' is required in validator config")
    if not isinstance(log_file, str):
        raise ValidationError("'log_file' must be a string")
    if not log_file.strip():
        raise ValidationError("'log_file' must not be empty")

    log_file = str(Path(log_file).expanduser())

    rules: dict[str, Any] = {}

    max_duration = config.get("max_duration_seconds")
    if max_duration is not None:
        if not isinstance(max_duration, (int, float)) or max_duration <= 0:
            raise ValidationError(
                "'max_duration_seconds' must be a positive number"
            )
        rules["max_duration_seconds"] = max_duration

    required_fields = config.get("required_fields")
    if required_fields is not None:
        if not isinstance(required_fields, list):
            raise ValidationError("'required_fields' must be a list")
        rules["required_fields"] = required_fields

    return RunValidator(log_file=log_file, rules=rules)


def load_validator_from_file(path: str) -> RunValidator:
    """Load validator config from a JSON file and return a RunValidator."""
    config_path = Path(path).expanduser()
    with config_path.open() as fh:
        config = json.load(fh)
    return build_validator_from_config(config)
