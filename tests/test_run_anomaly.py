from __future__ import annotations

import json
import os

import pytest

from pipewatch.run_anomaly import AnomalyError, PipelineAnomaly, RunAnomaly


def _write_records(path, records):
    with open(path, "w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return str(tmp_path / "runs.log")


@pytest.fixture
def anomaly(log_file):
    return RunAnomaly(log_file=log_file, z_threshold=2.0)


def test_detect_returns_empty_for_missing_file(anomaly):
    assert anomaly.detect("pipe_a") == []


def test_detect_returns_empty_when_fewer_than_three_runs(log_file, anomaly):
    _write_records(log_file, [
        {"pipeline": "pipe_a", "run_id": "r1", "duration_seconds": 10.0},
        {"pipeline": "pipe_a", "run_id": "r2", "duration_seconds": 12.0},
    ])
    assert anomaly.detect("pipe_a") == []


def test_detect_returns_pipeline_anomaly_instances(log_file, anomaly):
    _write_records(log_file, [
        {"pipeline": "pipe_a", "run_id": f"r{i}", "duration_seconds": float(i)}
        for i in range(1, 6)
    ])
    results = anomaly.detect("pipe_a")
    assert all(isinstance(r, PipelineAnomaly) for r in results)


def test_detect_flags_outlier_as_anomaly(log_file):
    an = RunAnomaly(log_file=log_file, z_threshold=2.0)
    records = [
        {"pipeline": "pipe_a", "run_id": f"r{i}", "duration_seconds": 10.0}
        for i in range(9)
    ]
    records.append({"pipeline": "pipe_a", "run_id": "outlier", "duration_seconds": 1000.0})
    _write_records(log_file, records)
    results = an.detect("pipe_a")
    outlier = next(r for r in results if r.run_id == "outlier")
    assert outlier.is_anomaly is True


def test_detect_normal_run_not_flagged(log_file):
    an = RunAnomaly(log_file=log_file, z_threshold=2.0)
    records = [
        {"pipeline": "pipe_a", "run_id": f"r{i}", "duration_seconds": 10.0}
        for i in range(10)
    ]
    _write_records(log_file, records)
    results = an.detect("pipe_a")
    assert all(not r.is_anomaly for r in results)


def test_detect_all_returns_all_pipelines(log_file, anomaly):
    records = [
        {"pipeline": "pipe_a", "run_id": f"a{i}", "duration_seconds": float(i)}
        for i in range(1, 6)
    ] + [
        {"pipeline": "pipe_b", "run_id": f"b{i}", "duration_seconds": float(i)}
        for i in range(1, 6)
    ]
    _write_records(log_file, records)
    result = anomaly.detect_all()
    assert set(result.keys()) == {"pipe_a", "pipe_b"}


def test_to_dict_contains_expected_keys(log_file, anomaly):
    _write_records(log_file, [
        {"pipeline": "pipe_a", "run_id": f"r{i}", "duration_seconds": float(i)}
        for i in range(1, 6)
    ])
    result = anomaly.detect("pipe_a")[0]
    d = result.to_dict()
    for key in ("pipeline", "run_id", "duration_seconds", "mean_duration", "stddev", "z_score", "is_anomaly"):
        assert key in d


def test_invalid_log_file_raises():
    with pytest.raises(AnomalyError):
        RunAnomaly(log_file="", z_threshold=2.0)


def test_invalid_z_threshold_raises(log_file):
    with pytest.raises(AnomalyError):
        RunAnomaly(log_file=log_file, z_threshold=-1.0)
