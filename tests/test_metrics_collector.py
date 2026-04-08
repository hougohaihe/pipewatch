"""Tests for MetricsCollector and RunMetrics."""

import time
import pytest
from pipewatch.metrics_collector import MetricsCollector, RunMetrics


@pytest.fixture
def collector():
    return MetricsCollector()


def test_start_creates_metrics(collector):
    metrics = collector.start("run-1", "my_pipeline")
    assert isinstance(metrics, RunMetrics)
    assert metrics.run_id == "run-1"
    assert metrics.pipeline_name == "my_pipeline"
    assert metrics.end_time is None


def test_start_stores_metrics(collector):
    collector.start("run-1", "pipe")
    assert collector.get("run-1") is not None


def test_finish_sets_end_time(collector):
    collector.start("run-2", "pipe")
    metrics = collector.finish("run-2", records_processed=50)
    assert metrics.end_time is not None
    assert metrics.records_processed == 50


def test_finish_calculates_duration(collector):
    collector.start("run-3", "pipe")
    time.sleep(0.05)
    metrics = collector.finish("run-3")
    assert metrics.duration_seconds is not None
    assert metrics.duration_seconds >= 0.04


def test_finish_stores_errors(collector):
    collector.start("run-4", "pipe")
    metrics = collector.finish("run-4", errors_encountered=3)
    assert metrics.errors_encountered == 3


def test_finish_stores_extra(collector):
    collector.start("run-5", "pipe")
    metrics = collector.finish("run-5", extra={"source": "s3"})
    assert metrics.extra["source"] == "s3"


def test_finish_unknown_run_raises(collector):
    with pytest.raises(KeyError, match="no-such-run"):
        collector.finish("no-such-run")


def test_get_returns_none_for_missing(collector):
    assert collector.get("ghost") is None


def test_to_dict_contains_expected_keys(collector):
    collector.start("run-6", "pipe")
    metrics = collector.finish("run-6", records_processed=10)
    d = metrics.to_dict()
    for key in ("run_id", "pipeline_name", "start_time", "end_time",
                "duration_seconds", "records_processed", "errors_encountered", "extra"):
        assert key in d


def test_all_metrics_returns_list(collector):
    collector.start("r1", "p1")
    collector.start("r2", "p2")
    collector.finish("r1")
    result = collector.all_metrics()
    assert len(result) == 2


def test_summary_aggregates_correctly(collector):
    collector.start("s1", "pipe")
    collector.finish("s1", records_processed=100, errors_encountered=2)
    collector.start("s2", "pipe")
    collector.finish("s2", records_processed=200, errors_encountered=1)
    summary = collector.summary()
    assert summary["total_runs"] == 2
    assert summary["completed_runs"] == 2
    assert summary["total_records_processed"] == 300
    assert summary["total_errors"] == 3
    assert summary["avg_duration_seconds"] is not None


def test_summary_no_completed_runs(collector):
    collector.start("x1", "pipe")
    summary = collector.summary()
    assert summary["completed_runs"] == 0
    assert summary["avg_duration_seconds"] is None
