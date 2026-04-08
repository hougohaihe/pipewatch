"""Tests for MetricsReporter output formatting."""

import io
import json
import pytest
from pipewatch.metrics_collector import MetricsCollector
from pipewatch.metrics_reporter import MetricsReporter


@pytest.fixture
def populated_collector():
    c = MetricsCollector()
    c.start("run-a", "etl_pipeline")
    c.finish("run-a", records_processed=120, errors_encountered=1, extra={"env": "prod"})
    c.start("run-b", "etl_pipeline")
    c.finish("run-b", records_processed=80, errors_encountered=0)
    return c


@pytest.fixture
def reporter(populated_collector):
    buf = io.StringIO()
    return MetricsReporter(populated_collector, output=buf), buf


def test_print_summary_contains_totals(reporter):
    r, buf = reporter
    r.print_summary()
    output = buf.getvalue()
    assert "Total runs" in output
    assert "2" in output
    assert "200" in output  # total records


def test_print_summary_contains_errors(reporter):
    r, buf = reporter
    r.print_summary()
    output = buf.getvalue()
    assert "Total errors" in output
    assert "1" in output


def test_print_run_shows_pipeline_name(reporter):
    r, buf = reporter
    r.print_run("run-a")
    output = buf.getvalue()
    assert "etl_pipeline" in output


def test_print_run_shows_records(reporter):
    r, buf = reporter
    r.print_run("run-a")
    output = buf.getvalue()
    assert "120" in output


def test_print_run_shows_extra(reporter):
    r, buf = reporter
    r.print_run("run-a")
    output = buf.getvalue()
    assert "prod" in output


def test_print_run_missing_id(populated_collector):
    buf = io.StringIO()
    r = MetricsReporter(populated_collector, output=buf)
    r.print_run("ghost-run")
    assert "No metrics found" in buf.getvalue()


def test_print_all_json_valid(reporter):
    r, buf = reporter
    r.print_all_json()
    data = json.loads(buf.getvalue())
    assert isinstance(data, list)
    assert len(data) == 2


def test_print_all_json_contains_run_ids(reporter):
    r, buf = reporter
    r.print_all_json()
    data = json.loads(buf.getvalue())
    ids = {d["run_id"] for d in data}
    assert "run-a" in ids
    assert "run-b" in ids


def test_print_summary_json_valid(reporter):
    r, buf = reporter
    r.print_summary_json()
    summary = json.loads(buf.getvalue())
    assert summary["total_runs"] == 2
    assert summary["total_records_processed"] == 200
