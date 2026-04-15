from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_ranker import RankedPipeline, RunRanker


def _write_records(path: Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.log"


@pytest.fixture
def ranker(log_file: Path) -> RunRanker:
    return RunRanker(log_file=str(log_file))


def test_rank_returns_empty_for_missing_file(ranker: RunRanker) -> None:
    assert ranker.rank() == []


def test_rank_returns_ranked_pipeline_instances(log_file: Path, ranker: RunRanker) -> None:
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10.0},
    ])
    result = ranker.rank()
    assert len(result) == 1
    assert isinstance(result[0], RankedPipeline)


def test_rank_assigns_rank_1_to_best(log_file: Path, ranker: RunRanker) -> None:
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 5.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 5.0},
        {"pipeline": "slow", "status": "failure", "duration_seconds": 3600.0},
    ])
    result = ranker.rank()
    assert result[0].rank == 1
    assert result[0].pipeline == "etl"


def test_rank_filters_by_pipeline(log_file: Path, ranker: RunRanker) -> None:
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 5.0},
        {"pipeline": "ingest", "status": "failure", "duration_seconds": 2.0},
    ])
    result = ranker.rank(pipeline="etl")
    assert len(result) == 1
    assert result[0].pipeline == "etl"


def test_rank_success_rate_computed_correctly(log_file: Path, ranker: RunRanker) -> None:
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 1.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 1.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 1.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 1.0},
    ])
    result = ranker.rank()
    assert result[0].success_rate == pytest.approx(0.75)


def test_rank_run_count_correct(log_file: Path, ranker: RunRanker) -> None:
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 2.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 4.0},
        {"pipeline": "etl", "status": "failure", "duration_seconds": 6.0},
    ])
    result = ranker.rank()
    assert result[0].run_count == 3


def test_rank_avg_duration_computed(log_file: Path, ranker: RunRanker) -> None:
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10.0},
        {"pipeline": "etl", "status": "success", "duration_seconds": 20.0},
    ])
    result = ranker.rank()
    assert result[0].avg_duration == pytest.approx(15.0)


def test_rank_to_dict_has_required_keys(log_file: Path, ranker: RunRanker) -> None:
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 3.0},
    ])
    d = ranker.rank()[0].to_dict()
    for key in ("pipeline", "rank", "score", "run_count", "success_rate", "avg_duration"):
        assert key in d


def test_rank_skips_malformed_lines(log_file: Path, ranker: RunRanker) -> None:
    with log_file.open("w") as fh:
        fh.write("not-json\n")
        fh.write(json.dumps({"pipeline": "etl", "status": "success", "duration_seconds": 1.0}) + "\n")
    result = ranker.rank()
    assert len(result) == 1


def test_rank_no_duration_yields_none_avg(log_file: Path, ranker: RunRanker) -> None:
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success"},
    ])
    result = ranker.rank()
    assert result[0].avg_duration is None
