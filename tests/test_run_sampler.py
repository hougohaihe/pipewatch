from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.run_sampler import RunSampler, SampleResult, SamplerError


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    p = tmp_path / "runs.jsonl"
    records = [
        {"run_id": f"r{i}", "pipeline": "etl" if i % 2 == 0 else "report", "status": "success"}
        for i in range(10)
    ]
    _write_records(p, records)
    return p


@pytest.fixture
def sampler(log_file):
    return RunSampler(log_file=str(log_file))


def test_sample_returns_sample_result(sampler):
    result = sampler.sample(3)
    assert isinstance(result, SampleResult)


def test_sample_correct_size(sampler):
    result = sampler.sample(3)
    assert result.sample_size == 3
    assert len(result.records) == 3


def test_sample_total_records(sampler):
    result = sampler.sample(3)
    assert result.total_records == 10


def test_sample_does_not_exceed_total(sampler):
    result = sampler.sample(100)
    assert result.sample_size == 10
    assert len(result.records) == 10


def test_sample_filters_by_pipeline(sampler):
    result = sampler.sample(3, pipeline="etl")
    assert all(r["pipeline"] == "etl" for r in result.records)
    assert result.pipeline == "etl"


def test_sample_pipeline_total_records(sampler):
    result = sampler.sample(10, pipeline="etl")
    assert result.total_records == 5


def test_sample_seed_is_deterministic(sampler):
    r1 = sampler.sample(4, seed=42)
    r2 = sampler.sample(4, seed=42)
    assert r1.records == r2.records


def test_sample_different_seeds_differ(sampler):
    r1 = sampler.sample(4, seed=1)
    r2 = sampler.sample(4, seed=99)
    assert r1.records != r2.records


def test_sample_missing_file_returns_empty(tmp_path):
    s = RunSampler(log_file=str(tmp_path / "missing.jsonl"))
    result = s.sample(5)
    assert result.total_records == 0
    assert result.sample_size == 0
    assert result.records == []


def test_sample_raises_for_invalid_n(sampler):
    with pytest.raises(SamplerError):
        sampler.sample(0)


def test_to_dict_contains_expected_keys(sampler):
    result = sampler.sample(2, seed=7)
    d = result.to_dict()
    assert "pipeline" in d
    assert "total_records" in d
    assert "sample_size" in d
    assert "seed" in d
    assert "records" in d
