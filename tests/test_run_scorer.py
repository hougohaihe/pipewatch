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
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.log"


@pytest.fixture
def scorer(log_file: Path) -> RunScorer:
    return RunScorer(log_file=str(log_file))


def test_score_all_returns_empty_for_missing_file(scorer: RunScorer) -> None:
    assert scorer.score_all() == {}


def test_score_pipeline_returns_none_for_unknown_pipeline(
    scorer: RunScorer, log_file: Path
) -> None:
    _write_records(log_file, [{"pipeline": "etl", "status": "success", "duration_seconds": 10}])
    assert scorer.score_pipeline("nonexistent") is None


def test_score_all_returns_pipeline_score_instances(
    scorer: RunScorer, log_file: Path
) -> None:
    _write_records(
        log_file,
        [
            {"pipeline": "etl", "status": "success", "duration_seconds": 20},
            {"pipeline": "etl", "status": "failure", "duration_seconds": 30},
        ],
    )
    result = scorer.score_all()
    assert "etl" in result
    assert isinstance(result["etl"], PipelineScore)


def test_score_run_count(scorer: RunScorer, log_file: Path) -> None:
    _write_records(
        log_file,
        [
            {"pipeline": "etl", "status": "success", "duration_seconds": 5},
            {"pipeline": "etl", "status": "success", "duration_seconds": 5},
            {"pipeline": "etl", "status": "failure", "duration_seconds": 5},
        ],
    )
    ps = scorer.score_all()["etl"]
    assert ps.run_count == 3
    assert ps.success_count == 2
    assert ps.failure_count == 1


def test_score_pipeline_full_success_gives_high_score(
    log_file: Path,
) -> None:
    scorer = RunScorer(str(log_file), max_expected_duration=300.0)
    _write_records(
        log_file,
        [{"pipeline": "etl", "status": "success", "duration_seconds": 1} for _ in range(5)],
    )
    ps = scorer.score_pipeline("etl")
    assert ps is not None
    assert ps.score > 90.0


def test_score_pipeline_all_failures_gives_low_score(
    log_file: Path,
) -> None:
    scorer = RunScorer(str(log_file))
    _write_records(
        log_file,
        [{"pipeline": "etl", "status": "failure", "duration_seconds": 400} for _ in range(4)],
    )
    ps = scorer.score_pipeline("etl")
    assert ps is not None
    assert ps.score < 20.0


def test_score_clamped_to_100(log_file: Path) -> None:
    scorer = RunScorer(str(log_file), success_weight=1.0, duration_weight=0.0)
    _write_records(
        log_file,
        [{"pipeline": "p", "status": "success", "duration_seconds": 1}],
    )
    ps = scorer.score_pipeline("p")
    assert ps is not None
    assert ps.score <= 100.0


def test_avg_duration_is_none_when_no_duration_field(
    scorer: RunScorer, log_file: Path
) -> None:
    _write_records(log_file, [{"pipeline": "p", "status": "success"}])
    ps = scorer.score_pipeline("p")
    assert ps is not None
    assert ps.avg_duration is None


def test_to_dict_contains_expected_keys(scorer: RunScorer, log_file: Path) -> None:
    _write_records(
        log_file,
        [{"pipeline": "p", "status": "success", "duration_seconds": 10}],
    )
    ps = scorer.score_pipeline("p")
    assert ps is not None
    d = ps.to_dict()
    for key in ("pipeline", "run_count", "success_count", "failure_count", "avg_duration", "score"):
        assert key in d


def test_multiple_pipelines_scored_independently(
    scorer: RunScorer, log_file: Path
) -> None:
    _write_records(
        log_file,
        [
            {"pipeline": "a", "status": "success", "duration_seconds": 5},
            {"pipeline": "b", "status": "failure", "duration_seconds": 5},
        ],
    )
    result = scorer.score_all()
    assert "a" in result and "b" in result
    assert result["a"].score > result["b"].score


def test_pipeline_score_rejects_invalid_score() -> None:
    with pytest.raises(ValueError):
        PipelineScore(
            pipeline="p",
            run_count=1,
            success_count=1,
            failure_count=0,
            avg_duration=5.0,
            score=150.0,
        )
