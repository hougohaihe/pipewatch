"""Tests for RunComparator and ComparisonResult."""

import json
import pytest
from pathlib import Path

from pipewatch.run_comparator import RunComparator, ComparisonResult
from pipewatch.comparator_config import build_comparator_from_config, load_comparator_from_file


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.jsonl"


@pytest.fixture
def comparator(log_file):
    return RunComparator(log_file=str(log_file))


def test_comparison_result_duration_delta():
    result = ComparisonResult(
        pipeline="etl",
        baseline_avg_duration=10.0,
        current_avg_duration=12.5,
        baseline_success_rate=1.0,
        current_success_rate=0.8,
    )
    assert result.duration_delta == pytest.approx(2.5)
    assert result.success_rate_delta == pytest.approx(-0.2)


def test_comparison_result_none_when_missing_data():
    result = ComparisonResult(
        pipeline="etl",
        baseline_avg_duration=None,
        current_avg_duration=5.0,
        baseline_success_rate=None,
        current_success_rate=1.0,
    )
    assert result.duration_delta is None
    assert result.success_rate_delta is None


def test_comparison_result_to_dict():
    result = ComparisonResult(
        pipeline="etl",
        baseline_avg_duration=5.0,
        current_avg_duration=6.0,
        baseline_success_rate=1.0,
        current_success_rate=0.5,
    )
    d = result.to_dict()
    assert d["pipeline"] == "etl"
    assert d["duration_delta"] == pytest.approx(1.0)
    assert d["success_rate_delta"] == pytest.approx(-0.5)


def test_compare_last_n_returns_result(log_file, comparator):
    records = [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 12.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 20.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 8.0},
    ]
    _write_records(log_file, records)
    result = comparator.compare_last_n("etl", baseline_n=2, current_n=2)
    assert isinstance(result, ComparisonResult)
    assert result.pipeline == "etl"
    assert result.current_avg_duration == pytest.approx(14.0)
    assert result.baseline_avg_duration == pytest.approx(11.0)


def test_compare_last_n_empty_log(log_file, comparator):
    result = comparator.compare_last_n("etl", baseline_n=3, current_n=3)
    assert result.baseline_avg_duration is None
    assert result.current_avg_duration is None


def test_compare_last_n_missing_file(tmp_path):
    comp = RunComparator(log_file=str(tmp_path / "missing.jsonl"))
    result = comp.compare_last_n("etl", baseline_n=2, current_n=2)
    assert result.baseline_success_rate is None
    assert result.current_success_rate is None


def test_build_comparator_from_config(log_file):
    comp = build_comparator_from_config({"log_file": str(log_file)})
    assert isinstance(comp, RunComparator)


def test_build_comparator_missing_log_file_raises():
    with pytest.raises(KeyError, match="log_file"):
        build_comparator_from_config({})


def test_build_comparator_non_string_raises():
    with pytest.raises(TypeError):
        build_comparator_from_config({"log_file": 123})


def test_build_comparator_empty_string_raises():
    with pytest.raises(ValueError):
        build_comparator_from_config({"log_file": "   "})


def test_load_comparator_from_file(tmp_path, log_file):
    config_file = tmp_path / "comparator.json"
    config_file.write_text(json.dumps({"log_file": str(log_file)}))
    comp = load_comparator_from_file(str(config_file))
    assert isinstance(comp, RunComparator)
