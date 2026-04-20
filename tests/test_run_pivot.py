import json
import pytest
from pathlib import Path
from pipewatch.run_pivot import PivotError, PivotTable, RunPivot


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.log"


@pytest.fixture()
def pivot(log_file: Path) -> RunPivot:
    return RunPivot(str(log_file))


_RECORDS = [
    {"pipeline": "etl", "status": "success", "duration_seconds": 10.0},
    {"pipeline": "etl", "status": "success", "duration_seconds": 20.0},
    {"pipeline": "etl", "status": "failure", "duration_seconds": 5.0},
    {"pipeline": "ingest", "status": "success", "duration_seconds": 8.0},
    {"pipeline": "ingest", "status": "failure", "duration_seconds": 3.0},
]


def test_build_returns_pivot_table_instance(log_file, pivot):
    _write_records(log_file, _RECORDS)
    result = pivot.build("pipeline", "status", "duration_seconds")
    assert isinstance(result, PivotTable)


def test_build_returns_empty_for_missing_file(pivot):
    result = pivot.build("pipeline", "status", "duration_seconds")
    assert result.rows == []
    assert result.columns == []
    assert result.cells == {}


def test_build_count_aggregation(log_file, pivot):
    _write_records(log_file, _RECORDS)
    result = pivot.build("pipeline", "status", "duration_seconds", agg="count")
    assert result.cells["etl"]["success"] == 2
    assert result.cells["etl"]["failure"] == 1
    assert result.cells["ingest"]["success"] == 1
    assert result.cells["ingest"]["failure"] == 1


def test_build_sum_aggregation(log_file, pivot):
    _write_records(log_file, _RECORDS)
    result = pivot.build("pipeline", "status", "duration_seconds", agg="sum")
    assert result.cells["etl"]["success"] == pytest.approx(30.0)
    assert result.cells["etl"]["failure"] == pytest.approx(5.0)


def test_build_avg_aggregation(log_file, pivot):
    _write_records(log_file, _RECORDS)
    result = pivot.build("pipeline", "status", "duration_seconds", agg="avg")
    assert result.cells["etl"]["success"] == pytest.approx(15.0)
    assert result.cells["ingest"]["failure"] == pytest.approx(3.0)


def test_build_avg_missing_cell_is_none(log_file, pivot):
    _write_records(log_file, _RECORDS)
    result = pivot.build("pipeline", "status", "duration_seconds", agg="avg")
    # Both pipelines have both statuses in our fixture, so check zero-count path
    # by using a field that only some records carry
    result2 = pivot.build("pipeline", "status", "nonexistent_field", agg="avg")
    for row in result2.rows:
        for col in result2.columns:
            assert result2.cells[row][col] is None


def test_build_invalid_agg_raises(log_file, pivot):
    _write_records(log_file, _RECORDS)
    with pytest.raises(PivotError, match="Unsupported aggregation"):
        pivot.build("pipeline", "status", "duration_seconds", agg="median")


def test_pivot_table_rows_and_columns_sorted(log_file, pivot):
    _write_records(log_file, _RECORDS)
    result = pivot.build("pipeline", "status", "duration_seconds")
    assert result.rows == sorted(result.rows)
    assert result.columns == sorted(result.columns)


def test_pivot_table_to_dict_contains_expected_keys(log_file, pivot):
    _write_records(log_file, _RECORDS)
    result = pivot.build("pipeline", "status", "duration_seconds")
    d = result.to_dict()
    assert set(d.keys()) == {"row_field", "col_field", "value_field", "rows", "columns", "cells"}


def test_build_skips_malformed_lines(tmp_path, log_file):
    log_file.write_text(
        json.dumps({"pipeline": "etl", "status": "success", "duration_seconds": 5.0}) + "\n"
        + "not-json\n"
        + json.dumps({"pipeline": "etl", "status": "failure", "duration_seconds": 2.0}) + "\n"
    )
    rp = RunPivot(str(log_file))
    result = rp.build("pipeline", "status", "duration_seconds", agg="count")
    assert result.cells["etl"]["success"] == 1
    assert result.cells["etl"]["failure"] == 1
