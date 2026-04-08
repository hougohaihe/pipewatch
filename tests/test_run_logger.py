"""Tests for the RunLogger module."""

import json
import pytest
from pathlib import Path
from pipewatch.run_logger import RunLogger, RunStatus


@pytest.fixture
def tmp_logger(tmp_path):
    log_file = tmp_path / "test_pipewatch.log"
    return RunLogger(log_path=str(log_file))


def test_log_start_creates_file(tmp_logger):
    tmp_logger.log_start("etl_pipeline")
    assert Path(tmp_logger.log_path).exists()


def test_log_start_returns_run_id(tmp_logger):
    run_id = tmp_logger.log_start("etl_pipeline")
    assert isinstance(run_id, str)
    assert len(run_id) == 36  # UUID format


def test_log_start_record_structure(tmp_logger):
    run_id = tmp_logger.log_start("etl_pipeline", metadata={"env": "prod"})
    runs = tmp_logger.read_runs()
    assert len(runs) == 1
    record = runs[0]
    assert record["run_id"] == run_id
    assert record["pipeline"] == "etl_pipeline"
    assert record["status"] == RunStatus.STARTED.value
    assert record["metadata"] == {"env": "prod"}
    assert "timestamp" in record


def test_log_success_record(tmp_logger):
    run_id = tmp_logger.log_start("etl_pipeline")
    tmp_logger.log_success("etl_pipeline", run_id, duration_seconds=12.5, message="All rows processed")
    runs = tmp_logger.read_runs()
    success_record = runs[1]
    assert success_record["status"] == RunStatus.SUCCESS.value
    assert success_record["duration_seconds"] == 12.5
    assert success_record["message"] == "All rows processed"


def test_log_failure_record(tmp_logger):
    run_id = tmp_logger.log_start("etl_pipeline")
    tmp_logger.log_failure("etl_pipeline", run_id, duration_seconds=3.2, message="DB connection timeout")
    runs = tmp_logger.read_runs()
    failure_record = runs[1]
    assert failure_record["status"] == RunStatus.FAILED.value
    assert failure_record["message"] == "DB connection timeout"


def test_read_runs_empty_when_no_log(tmp_path):
    logger = RunLogger(log_path=str(tmp_path / "nonexistent.log"))
    assert logger.read_runs() == []


def test_multiple_pipelines_logged(tmp_logger):
    tmp_logger.log_start("pipeline_a")
    tmp_logger.log_start("pipeline_b")
    runs = tmp_logger.read_runs()
    assert len(runs) == 2
    pipelines = {r["pipeline"] for r in runs}
    assert pipelines == {"pipeline_a", "pipeline_b"}


def test_custom_run_id_preserved(tmp_logger):
    custom_id = "custom-run-001"
    returned_id = tmp_logger.log_start("etl_pipeline", run_id=custom_id)
    assert returned_id == custom_id
    runs = tmp_logger.read_runs()
    assert runs[0]["run_id"] == custom_id
