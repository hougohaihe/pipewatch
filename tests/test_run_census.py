import json
import pytest
from pathlib import Path
from pipewatch.run_census import RunCensus, PipelineCensus, CensusError


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def census(log_file):
    return RunCensus(str(log_file))


SAMPLE_RECORDS = [
    {"pipeline": "etl", "status": "success", "started_at": "2024-01-01T10:00:00"},
    {"pipeline": "etl", "status": "success", "started_at": "2024-01-02T10:00:00"},
    {"pipeline": "etl", "status": "failure", "started_at": "2024-01-02T12:00:00"},
    {"pipeline": "ingest", "status": "success", "started_at": "2024-01-01T08:00:00"},
    {"pipeline": "ingest", "status": "failure", "started_at": "2024-01-03T08:00:00"},
]


def test_compute_returns_empty_for_missing_file(census):
    result = census.compute()
    assert result == {}


def test_compute_returns_pipeline_census_instances(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute()
    assert all(isinstance(v, PipelineCensus) for v in result.values())


def test_compute_includes_all_pipelines(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute()
    assert set(result.keys()) == {"etl", "ingest"}


def test_compute_total_runs_correct(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute()
    assert result["etl"].total_runs == 3
    assert result["ingest"].total_runs == 2


def test_compute_status_counts_correct(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute()
    assert result["etl"].status_counts == {"success": 2, "failure": 1}
    assert result["ingest"].status_counts == {"success": 1, "failure": 1}


def test_compute_unique_statuses_sorted(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute()
    assert result["etl"].unique_statuses == ["failure", "success"]


def test_compute_first_and_last_seen(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute()
    assert result["etl"].first_seen == "2024-01-01T10:00:00"
    assert result["etl"].last_seen == "2024-01-02T12:00:00"


def test_compute_active_days(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute()
    assert result["etl"].active_days == 2
    assert result["ingest"].active_days == 2


def test_compute_for_returns_none_for_unknown(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    assert census.compute_for("nonexistent") is None


def test_compute_for_returns_correct_pipeline(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute_for("ingest")
    assert result is not None
    assert result.pipeline == "ingest"
    assert result.total_runs == 2


def test_to_dict_contains_expected_keys(log_file, census):
    _write_records(log_file, SAMPLE_RECORDS)
    result = census.compute_for("etl")
    d = result.to_dict()
    expected_keys = {"pipeline", "total_runs", "unique_statuses", "status_counts",
                     "first_seen", "last_seen", "active_days"}
    assert set(d.keys()) == expected_keys


def test_compute_handles_missing_timestamps(log_file, census):
    records = [{"pipeline": "bare", "status": "success"}]
    _write_records(log_file, records)
    result = census.compute_for("bare")
    assert result.first_seen is None
    assert result.last_seen is None
    assert result.active_days == 0
