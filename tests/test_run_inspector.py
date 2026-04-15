import json
import pytest
from pathlib import Path

from pipewatch.run_inspector import (
    InspectionResult,
    InspectorError,
    RunInspector,
    REQUIRED_FIELDS,
)
from pipewatch.inspector_config import build_inspector_from_config


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def inspector(log_file):
    return RunInspector(log_file=str(log_file))


_FULL_RECORD = {
    "run_id": "abc-1",
    "pipeline": "etl",
    "status": "success",
    "start_time": "2024-01-01T00:00:00",
    "end_time": "2024-01-01T00:01:00",
    "duration_seconds": 60.0,
}


def test_inspect_returns_none_for_missing_id(log_file, inspector):
    _write_records(log_file, [_FULL_RECORD])
    assert inspector.inspect("no-such-id") is None


def test_inspect_returns_inspection_result(log_file, inspector):
    _write_records(log_file, [_FULL_RECORD])
    result = inspector.inspect("abc-1")
    assert isinstance(result, InspectionResult)
    assert result.run_id == "abc-1"
    assert result.pipeline == "etl"


def test_inspect_valid_record_has_no_missing_fields(log_file, inspector):
    _write_records(log_file, [_FULL_RECORD])
    result = inspector.inspect("abc-1")
    assert result.missing_fields == []
    assert result.is_valid is True


def test_inspect_detects_missing_required_field(log_file, inspector):
    bad = {k: v for k, v in _FULL_RECORD.items() if k != "status"}
    _write_records(log_file, [bad])
    result = inspector.inspect("abc-1")
    assert "status" in result.missing_fields
    assert result.is_valid is False


def test_inspect_detects_extra_fields(log_file, inspector):
    extra = {**_FULL_RECORD, "custom_metric": 42}
    _write_records(log_file, [extra])
    result = inspector.inspect("abc-1")
    assert "custom_metric" in result.extra_fields


def test_inspect_all_returns_all_results(log_file, inspector):
    r2 = {**_FULL_RECORD, "run_id": "abc-2"}
    _write_records(log_file, [_FULL_RECORD, r2])
    results = inspector.inspect_all()
    assert len(results) == 2


def test_inspect_all_empty_file(log_file, inspector):
    log_file.write_text("")
    assert inspector.inspect_all() == []


def test_inspect_all_missing_file(inspector):
    assert inspector.inspect_all() == []


def test_inspect_pipeline_filters_correctly(log_file, inspector):
    r2 = {**_FULL_RECORD, "run_id": "abc-2", "pipeline": "loader"}
    _write_records(log_file, [_FULL_RECORD, r2])
    results = inspector.inspect_pipeline("etl")
    assert len(results) == 1
    assert results[0].pipeline == "etl"


def test_to_dict_contains_expected_keys(log_file, inspector):
    _write_records(log_file, [_FULL_RECORD])
    result = inspector.inspect("abc-1")
    d = result.to_dict()
    assert set(d.keys()) == {"run_id", "pipeline", "fields", "missing_fields", "extra_fields", "is_valid"}


def test_build_inspector_from_config_returns_instance(tmp_path):
    lf = tmp_path / "runs.log"
    inst = build_inspector_from_config({"log_file": str(lf)})
    assert isinstance(inst, RunInspector)


def test_build_inspector_missing_log_file_raises():
    with pytest.raises(InspectorError, match="log_file"):
        build_inspector_from_config({})


def test_build_inspector_non_string_log_file_raises():
    with pytest.raises(InspectorError, match="string"):
        build_inspector_from_config({"log_file": 123})


def test_build_inspector_empty_log_file_raises():
    with pytest.raises(InspectorError, match="empty"):
        build_inspector_from_config({"log_file": "   "})
