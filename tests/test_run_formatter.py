import json
import pytest
from pipewatch.run_formatter import FormatError, FormattedRun, RunFormatter


def _write_records(path, records):
    with open(path, "w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    p = tmp_path / "runs.log"
    _write_records(p, [
        {"run_id": "abc", "pipeline": "etl", "status": "success", "duration_seconds": 1.2},
        {"run_id": "def", "pipeline": "etl", "status": "failure", "duration_seconds": 0.5},
        {"run_id": "ghi", "pipeline": "ingest", "status": "success", "duration_seconds": 3.0},
    ])
    return str(p)


@pytest.fixture
def formatter(log_file):
    return RunFormatter(log_file)


def test_format_all_returns_formatted_run_instances(formatter):
    results = formatter.format_all()
    assert all(isinstance(r, FormattedRun) for r in results)


def test_format_all_returns_all_records(formatter):
    results = formatter.format_all()
    assert len(results) == 3


def test_format_all_filters_by_pipeline(formatter):
    results = formatter.format_all(pipeline="ingest")
    assert len(results) == 1
    assert results[0].pipeline == "ingest"


def test_format_all_empty_for_missing_pipeline(formatter):
    results = formatter.format_all(pipeline="nonexistent")
    assert results == []


def test_format_record_populates_fields(formatter, log_file):
    record = {"run_id": "x1", "pipeline": "p", "status": "success", "extra": 42}
    result = formatter.format_record(record)
    assert result.run_id == "x1"
    assert result.fields["extra"] == 42


def test_format_record_raises_for_missing_field(formatter):
    with pytest.raises(FormatError, match="missing required fields"):
        formatter.format_record({"run_id": "x", "pipeline": "p"})


def test_render_uses_default_template(formatter):
    record = {"run_id": "abc", "pipeline": "etl", "status": "success", "duration_seconds": 1.2}
    result = formatter.render(record)
    assert "abc" in result
    assert "etl" in result
    assert "success" in result


def test_render_uses_custom_template(log_file):
    fmt = RunFormatter(log_file, template="[{status}] {pipeline}/{run_id}")
    record = {"run_id": "abc", "pipeline": "etl", "status": "success"}
    assert fmt.render(record) == "[success] etl/abc"


def test_render_raises_for_unknown_template_field(log_file):
    fmt = RunFormatter(log_file, template="{nonexistent_field}")
    record = {"run_id": "abc", "pipeline": "etl", "status": "success"}
    with pytest.raises(FormatError, match="unknown field"):
        fmt.render(record)


def test_render_all_returns_strings(formatter):
    results = formatter.render_all()
    assert all(isinstance(r, str) for r in results)
    assert len(results) == 3


def test_render_all_filters_by_pipeline(formatter):
    results = formatter.render_all(pipeline="etl")
    assert len(results) == 2


def test_format_all_empty_for_missing_file(tmp_path):
    fmt = RunFormatter(str(tmp_path / "missing.log"))
    assert fmt.format_all() == []


def test_constructor_rejects_empty_log_file():
    with pytest.raises(ValueError):
        RunFormatter("")


def test_constructor_rejects_non_string_log_file():
    with pytest.raises(ValueError):
        RunFormatter(None)


def test_formatted_run_to_dict_includes_all_fields():
    fr = FormattedRun(run_id="r1", pipeline="p", status="success", fields={"k": "v"})
    d = fr.to_dict()
    assert d["run_id"] == "r1"
    assert d["k"] == "v"
