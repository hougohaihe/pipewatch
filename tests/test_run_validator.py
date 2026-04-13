"""Tests for RunValidator."""

import json
import pytest
from pathlib import Path

from pipewatch.run_validator import RunValidator, ValidationResult


def _write_records(path: Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


@pytest.fixture
def validator(log_file: Path) -> RunValidator:
    return RunValidator(str(log_file))


def test_validate_all_empty_file(validator: RunValidator) -> None:
    results = validator.validate_all()
    assert results == []


def test_validate_all_missing_file(tmp_path: Path) -> None:
    v = RunValidator(str(tmp_path / "missing.jsonl"))
    assert v.validate_all() == []


def test_validate_record_passes_valid(validator: RunValidator) -> None:
    record = {"run_id": "abc", "pipeline": "etl", "status": "success"}
    result = validator.validate_record(record)
    assert result.passed is True
    assert result.violations == []


def test_validate_record_missing_run_id(validator: RunValidator) -> None:
    record = {"pipeline": "etl", "status": "success"}
    result = validator.validate_record(record)
    assert result.passed is False
    assert any("run_id" in v for v in result.violations)


def test_validate_record_missing_pipeline(validator: RunValidator) -> None:
    record = {"run_id": "abc", "status": "success"}
    result = validator.validate_record(record)
    assert result.passed is False
    assert any("pipeline" in v for v in result.violations)


def test_validate_record_invalid_status(validator: RunValidator) -> None:
    record = {"run_id": "abc", "pipeline": "etl", "status": "unknown_status"}
    result = validator.validate_record(record)
    assert result.passed is False
    assert any("status" in v for v in result.violations)


def test_validate_record_duration_exceeded(tmp_path: Path) -> None:
    v = RunValidator(str(tmp_path / "runs.jsonl"), rules={"max_duration_seconds": 10})
    record = {"run_id": "abc", "pipeline": "etl", "status": "success", "duration_seconds": 30}
    result = v.validate_record(record)
    assert result.passed is False
    assert any("duration" in v for v in result.violations)


def test_validate_record_duration_within_limit(tmp_path: Path) -> None:
    v = RunValidator(str(tmp_path / "runs.jsonl"), rules={"max_duration_seconds": 60})
    record = {"run_id": "abc", "pipeline": "etl", "status": "success", "duration_seconds": 5}
    result = v.validate_record(record)
    assert result.passed is True


def test_validate_record_required_fields_missing(tmp_path: Path) -> None:
    v = RunValidator(str(tmp_path / "r.jsonl"), rules={"required_fields": ["env", "owner"]})
    record = {"run_id": "abc", "pipeline": "etl", "status": "success"}
    result = v.validate_record(record)
    assert result.passed is False
    assert any("env" in viol for viol in result.violations)
    assert any("owner" in viol for viol in result.violations)


def test_validate_pipeline_filters_correctly(log_file: Path) -> None:
    records = [
        {"run_id": "1", "pipeline": "etl", "status": "success"},
        {"run_id": "2", "pipeline": "ingest", "status": "success"},
        {"run_id": "3", "pipeline": "etl", "status": "failure"},
    ]
    _write_records(log_file, records)
    v = RunValidator(str(log_file))
    results = v.validate_pipeline("etl")
    assert len(results) == 2
    assert all(r.pipeline == "etl" for r in results)


def test_validation_result_to_dict() -> None:
    r = ValidationResult(run_id="x", pipeline="p", passed=False, violations=["bad"])
    d = r.to_dict()
    assert d["run_id"] == "x"
    assert d["passed"] is False
    assert d["violations"] == ["bad"]
