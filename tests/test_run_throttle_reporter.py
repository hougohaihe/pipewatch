import json
import time
from pathlib import Path

import pytest

from pipewatch.notification_throttle import NotificationThrottle, ThrottlePolicy
from pipewatch.run_throttle_reporter import ThrottleReporter


@pytest.fixture()
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "throttle_state.json"


@pytest.fixture()
def throttle(state_file: Path) -> NotificationThrottle:
    policy = ThrottlePolicy(min_interval_seconds=300)
    return NotificationThrottle(policy=policy, state_file=str(state_file))


@pytest.fixture()
def reporter(throttle: NotificationThrottle) -> ThrottleReporter:
    return ThrottleReporter(throttle)


def test_print_summary_no_state(reporter: ThrottleReporter, capsys):
    reporter.print_summary()
    captured = capsys.readouterr()
    assert "No throttle state recorded" in captured.out


def test_print_summary_shows_entries(throttle: NotificationThrottle, reporter: ThrottleReporter, capsys):
    throttle.allow("pipeline_a")
    throttle.allow("pipeline_b")
    reporter.print_summary()
    captured = capsys.readouterr()
    assert "pipeline_a" in captured.out
    assert "pipeline_b" in captured.out
    assert "300" in captured.out


def test_print_summary_shows_total(throttle: NotificationThrottle, reporter: ThrottleReporter, capsys):
    throttle.allow("pipeline_x")
    reporter.print_summary()
    captured = capsys.readouterr()
    assert "Total entries: 1" in captured.out


def test_print_json_returns_valid_json(throttle: NotificationThrottle, reporter: ThrottleReporter, capsys):
    throttle.allow("pipeline_json")
    reporter.print_json()
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["key"] == "pipeline_json"
    assert data[0]["min_interval_seconds"] == 300


def test_print_json_empty_state(reporter: ThrottleReporter, capsys):
    reporter.print_json()
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data == []


def test_print_entry_known_key(throttle: NotificationThrottle, reporter: ThrottleReporter, capsys):
    throttle.allow("my_pipeline")
    reporter.print_entry("my_pipeline")
    captured = capsys.readouterr()
    entry = json.loads(captured.out)
    assert entry["key"] == "my_pipeline"
    assert "last_sent" in entry
    assert entry["min_interval_seconds"] == 300


def test_print_entry_unknown_key(reporter: ThrottleReporter, capsys):
    reporter.print_entry("nonexistent")
    captured = capsys.readouterr()
    assert "nonexistent" in captured.out
    assert "No throttle entry found" in captured.out
