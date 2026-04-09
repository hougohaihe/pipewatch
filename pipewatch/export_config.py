"""Configuration loader for run export settings."""

import json
from typing import Any, Dict, Optional
from pipewatch.run_exporter import RunExporter


DEFAULT_FORMAT = "json"
SUPPORTED_FORMATS = ("json", "csv")


def build_exporter_from_config(
    config: Dict[str, Any], log_path: str
) -> RunExporter:
    """Validate export config and return a configured RunExporter.

    Expected config keys:
      - format (str): 'json' or 'csv'  [optional, default: 'json']
      - pipeline (str): filter to a specific pipeline  [optional]
      - output_path (str): destination file path  [required]
    """
    fmt = config.get("format", DEFAULT_FORMAT)
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported export format '{fmt}'. Choose from: {SUPPORTED_FORMATS}"
        )

    output_path = config.get("output_path")
    if not output_path:
        raise ValueError("Export config must include 'output_path'.")

    return RunExporter(log_path)


def run_export_from_config(
    config: Dict[str, Any], log_path: str
) -> int:
    """Execute an export based on a config dict. Returns number of records exported."""
    exporter = build_exporter_from_config(config, log_path)
    fmt = config.get("format", DEFAULT_FORMAT)
    output_path = config["output_path"]
    pipeline: Optional[str] = config.get("pipeline")

    if fmt == "csv":
        return exporter.write_csv(output_path, pipeline=pipeline)
    return exporter.write_json(output_path, pipeline=pipeline)


def load_export_config_from_file(config_path: str) -> Dict[str, Any]:
    """Load export configuration from a JSON file."""
    with open(config_path, "r") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Export config file must contain a JSON object.")
    return data
