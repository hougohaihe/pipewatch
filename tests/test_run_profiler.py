from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_profiler import PipelineProfile, RunProfiler


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.jsonl"


@pytest.fixture
def profiler(log_file):
    return RunProfiler(log_file=str(log_file))


_RECORDS = [
    {"run_id": "a1", "pipeline": "etl", "status": "success", "duration_seconds": 10.0, "tags": ["prod"]},
    {"run_id": "a2", "pipeline": "etl", "status": "success", "duration_seconds": 20.0, "tags": ["prod", "nightly"]},
    {"run_id": "a3", "pipeline": "etl", "status": "failure", "duration_seconds": 5.0, "tags": []},
    {"run_id": "b1", "pipeline": "ingest", "status": "success", "duration_seconds": 3.0, "tags": ["dev"]},
]


def test_profile_returns_pipeline_profile(log_file, profiler):
    _write_records(log_file, _RECORDS)
    result = profiler.profile("etl")
    assert isinstance(result, PipelineProfile)
    assert result.pipeline == "etl"


def test_profile_run_count(log_file, profiler):
    _write_records(log_file, _RECORDS)
    result = profiler.profile("etl")
    assert result.run_count == 3


def test_profile_avg_duration(log_file, profiler):
    _write_records(log_file, _RECORDS)
    result = profiler.profile("etl")
    assert result.avg_duration_seconds == pytest.approx(11.6667, rel=1e-3)


def test_profile_min_max_duration(log_file, profiler):
    _write_records(log_file, _RECORDS)
    result = profiler.profile("etl")
    assert result.min_duration_seconds == 5.0
    assert result.max_duration_seconds == 20.0


def test_profile_p50_duration(log_file, profiler):
    _write_records(log_file, _RECORDS)
    result = profiler.profile("etl")
    assert result.p50_duration_seconds is not None


def test_profile_p95_duration(log_file, profiler):
    _write_records(log_file, _RECORDS)
    result = profiler.profile("etl")
    assert result.p95_duration_seconds is not None


def test_profile_collects_unique_tags(log_file, profiler):
    _write_records(log_file, _RECORDS)
    result = profiler.profile("etl")
    assert set(result.tags) == {"nightly", "prod"}


def test_profile_returns_none_for_unknown_pipeline(log_file, profiler):
    _write_records(log_file, _RECORDS)
    assert profiler.profile("nonexistent") is None


def test_profile_returns_none_for_missing_file(profiler):
    assert profiler.profile("etl") is None


def test_profile_all_returns_all_pipelines(log_file, profiler):
    _write_records(log_file, _RECORDS)
    result = profiler.profile_all()
    assert set(result.keys()) == {"etl", "ingest"}


def test_profile_all_empty_file(log_file, profiler):
    log_file.write_text("")
    assert profiler.profile_all() == {}


def test_to_dict_contains_expected_keys(log_file, profiler):
    _write_records(log_file, _RECORDS)
    d = profiler.profile("etl").to_dict()
    for key in ("pipeline", "run_count", "avg_duration_seconds", "min_duration_seconds",
                "max_duration_seconds", "p50_duration_seconds", "p95_duration_seconds", "tags"):
        assert key in d
