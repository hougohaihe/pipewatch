from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_summarizer import PipelineSummary, RunSummarizer


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def summarizer(log_file):
    return RunSummarizer(log_file=str(log_file))


_RECORDS = [
    {"pipeline": "etl", "status": "success", "duration_seconds": 10.0, "started_at": "2024-01-01T10:00:00"},
    {"pipeline": "etl", "status": "success", "duration_seconds": 20.0, "started_at": "2024-01-02T10:00:00"},
    {"pipeline": "etl", "status": "failure", "duration_seconds": 5.0,  "started_at": "2024-01-03T10:00:00"},
    {"pipeline": "ingest", "status": "success", "duration_seconds": 8.0, "started_at": "2024-01-01T09:00:00"},
]


def test_summarize_all_returns_all_pipelines(log_file, summarizer):
    _write_records(log_file, _RECORDS)
    summaries = summarizer.summarize_all()
    names = {s.pipeline for s in summaries}
    assert names == {"etl", "ingest"}


def test_summarize_all_empty_file(summarizer):
    assert summarizer.summarize_all() == []


def test_summarize_all_missing_file(tmp_path):
    s = RunSummarizer(log_file=str(tmp_path / "missing.log"))
    assert s.summarize_all() == []


def test_summarize_pipeline_counts(log_file, summarizer):
    _write_records(log_file, _RECORDS)
    summary = summarizer.summarize_pipeline("etl")
    assert summary is not None
    assert summary.total_runs == 3
    assert summary.successful_runs == 2
    assert summary.failed_runs == 1


def test_summarize_pipeline_avg_duration(log_file, summarizer):
    _write_records(log_file, _RECORDS)
    summary = summarizer.summarize_pipeline("etl")
    assert summary.avg_duration_seconds == pytest.approx(11.6667, rel=1e-3)


def test_summarize_pipeline_success_rate(log_file, summarizer):
    _write_records(log_file, _RECORDS)
    summary = summarizer.summarize_pipeline("etl")
    assert summary.success_rate() == pytest.approx(66.67, rel=1e-2)


def test_summarize_pipeline_last_run(log_file, summarizer):
    _write_records(log_file, _RECORDS)
    summary = summarizer.summarize_pipeline("etl")
    assert summary.last_run_status == "failure"
    assert summary.last_run_time == "2024-01-03T10:00:00"


def test_summarize_pipeline_unknown_returns_none(log_file, summarizer):
    _write_records(log_file, _RECORDS)
    assert summarizer.summarize_pipeline("nonexistent") is None


def test_pipeline_summary_to_dict_keys(log_file, summarizer):
    _write_records(log_file, _RECORDS)
    summary = summarizer.summarize_pipeline("ingest")
    d = summary.to_dict()
    assert "pipeline" in d
    assert "success_rate_pct" in d
    assert d["total_runs"] == 1


def test_success_rate_zero_runs():
    s = PipelineSummary(pipeline="empty")
    assert s.success_rate() is None


def test_skips_malformed_lines(log_file, summarizer):
    with log_file.open("w") as fh:
        fh.write("not-json\n")
        fh.write(json.dumps({"pipeline": "etl", "status": "success", "duration_seconds": 1.0, "started_at": "2024-01-01"}) + "\n")
    summaries = summarizer.summarize_all()
    assert len(summaries) == 1
