"""Tests for RunExporter."""

import json
import csv
import io
import pytest
from pathlib import Path
from pipewatch.run_exporter import RunExporter


SAMPLE_RECORDS = [
    {"run_id": "abc1", "pipeline": "etl", "status": "success", "duration": 1.2},
    {"run_id": "abc2", "pipeline": "etl", "status": "failure", "duration": 0.5},
    {"run_id": "abc3", "pipeline": "ingest", "status": "success", "duration": 3.1},
]


@pytest.fixture
def log_file(tmp_path):
    path = tmp_path / "runs.log"
    with open(path, "w") as f:
        for record in SAMPLE_RECORDS:
            f.write(json.dumps(record) + "\n")
    return str(path)


@pytest.fixture
def exporter(log_file):
    return RunExporter(log_file)


def test_to_json_returns_all_records(exporter):
    result = json.loads(exporter.to_json())
    assert len(result) == 3


def test_to_json_filters_by_pipeline(exporter):
    result = json.loads(exporter.to_json(pipeline="ingest"))
    assert len(result) == 1
    assert result[0]["run_id"] == "abc3"


def test_to_json_empty_for_missing_pipeline(exporter):
    result = json.loads(exporter.to_json(pipeline="nonexistent"))
    assert result == []


def test_to_csv_contains_header(exporter):
    csv_text = exporter.to_csv()
    reader = csv.DictReader(io.StringIO(csv_text))
    assert "run_id" in reader.fieldnames
    assert "pipeline" in reader.fieldnames
    assert "status" in reader.fieldnames


def test_to_csv_row_count(exporter):
    csv_text = exporter.to_csv()
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    assert len(rows) == 3


def test_to_csv_filters_by_pipeline(exporter):
    csv_text = exporter.to_csv(pipeline="etl")
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    assert len(rows) == 2
    assert all(r["pipeline"] == "etl" for r in rows)


def test_to_csv_empty_for_no_records(tmp_path):
    empty_log = str(tmp_path / "empty.log")
    exporter = RunExporter(empty_log)
    assert exporter.to_csv() == ""


def test_write_json_creates_file(exporter, tmp_path):
    dest = str(tmp_path / "out.json")
    count = exporter.write_json(dest)
    assert count == 3
    assert Path(dest).exists()
    with open(dest) as f:
        data = json.load(f)
    assert len(data) == 3


def test_write_csv_creates_file(exporter, tmp_path):
    dest = str(tmp_path / "out.csv")
    count = exporter.write_csv(dest)
    assert count == 3
    assert Path(dest).exists()


def test_missing_log_file_returns_empty_json(tmp_path):
    exporter = RunExporter(str(tmp_path / "missing.log"))
    result = json.loads(exporter.to_json())
    assert result == []
