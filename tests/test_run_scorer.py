"""Tests for RunScorer and PipelineScore."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_scorer import PipelineScore, RunScorer


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.jsonl"


@pytest.fixture
def scorer(log_file):
    return RunScorer(log_file=str(log_file))


def test_score_all_returns_empty_for_missing_file(scorer):
    assert scorer.score_all() == []


def test_score_pipeline_returns_none_for_unknown_pipeline(log_file, scorer):
    _write_records(log_file, [{"pipeline": "alpha", "status": "success", "duration_seconds": 1.0}])
    assert scorer.score_pipeline("ghost") is None


def test_score_pipeline_perfect_score(log_file, scorer):
    records = [
        {"pipeline": "alpha", "status": "success", "duration_seconds": 2.0},
        {"pipeline": "alpha", "status": "success", "duration_seconds": 3.0},
    ]
    _write_records(log_file, records)
    ps = scorer.score_pipeline("alpha")
    assert ps is not None
    assert ps.score == pytest.approx(100.0)
    assert ps.grade == "A"
    assert ps.failure_streak == 0


def test_score_pipeline_failure_streak_reduces_score(log_file, scorer):
    records = [
        {"pipeline": "beta", "status": "success", "duration_seconds": 1.0},
        {"pipeline": "beta", "status": "failure", "duration_seconds": 0.5},
        {"pipeline": "beta", "status": "failure", "duration_seconds": 0.5},
    ]
    _write_records(log_file, records)
    ps = scorer.score_pipeline("beta")
    assert ps is not None
    assert ps.failure_streak == 2
    assert ps.score < 100.0


def test_score_pipeline_all_failures_clamps_to_zero(log_file, scorer):
    records = [{"pipeline": "gamma", "status": "failure"} for _ in range(10)]
    _write_records(log_file, records)
    ps = scorer.score_pipeline("gamma")
    assert ps is not None
    assert ps.score == pytest.approx(0.0)
    assert ps.grade == "F"


def test_score_pipeline_avg_duration_computed(log_file, scorer):
    records = [
        {"pipeline": "delta", "status": "success", "duration_seconds": 4.0},
        {"pipeline": "delta", "status": "success", "duration_seconds": 6.0},
    ]
    _write_records(log_file, records)
    ps = scorer.score_pipeline("delta")
    assert ps.avg_duration == pytest.approx(5.0)


def test_score_pipeline_avg_duration_none_when_missing(log_file, scorer):
    records = [{"pipeline": "epsilon", "status": "success"}]
    _write_records(log_file, records)
    ps = scorer.score_pipeline("epsilon")
    assert ps.avg_duration is None


def test_score_all_returns_all_pipelines(log_file, scorer):
    records = [
        {"pipeline": "a", "status": "success", "duration_seconds": 1.0},
        {"pipeline": "b", "status": "failure", "duration_seconds": 2.0},
    ]
    _write_records(log_file, records)
    results = scorer.score_all()
    names = [r.pipeline for r in results]
    assert "a" in names and "b" in names


def test_pipeline_score_to_dict_keys(log_file, scorer):
    _write_records(log_file, [{"pipeline": "zeta", "status": "success", "duration_seconds": 1.5}])
    ps = scorer.score_pipeline("zeta")
    d = ps.to_dict()
    assert set(d.keys()) == {"pipeline", "score", "success_rate", "avg_duration", "failure_streak", "grade"}


def test_pipeline_score_grade_boundaries():
    def make(score):
        ps = PipelineScore(pipeline="x", score=score, success_rate=1.0, avg_duration=None, failure_streak=0)
        return ps.grade

    assert make(95.0) == "A"
    assert make(80.0) == "B"
    assert make(60.0) == "C"
    assert make(40.0) == "D"
    assert make(20.0) == "F"
