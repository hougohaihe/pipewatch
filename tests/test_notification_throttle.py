"""Tests for NotificationThrottle and throttle_config helpers."""

import json
import time
from pathlib import Path

import pytest

from pipewatch.notification_throttle import NotificationThrottle, ThrottlePolicy
from pipewatch.throttle_config import build_throttle_from_config


@pytest.fixture()
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "throttle_state.json"


@pytest.fixture()
def throttle(state_file: Path) -> NotificationThrottle:
    policy = ThrottlePolicy(min_interval_seconds=60)
    return NotificationThrottle(state_path=state_file, policy=policy)


# --- ThrottlePolicy ---

def test_policy_default_interval():
    p = ThrottlePolicy()
    assert p.min_interval_seconds == 300


def test_policy_rejects_negative_interval():
    with pytest.raises(ValueError):
        ThrottlePolicy(min_interval_seconds=-1)


def test_policy_to_dict():
    p = ThrottlePolicy(min_interval_seconds=120)
    assert p.to_dict() == {"min_interval_seconds": 120}


# --- NotificationThrottle ---

def test_should_alert_when_no_prior_record(throttle: NotificationThrottle):
    assert throttle.should_alert("my_pipeline") is True


def test_should_not_alert_immediately_after_record(throttle: NotificationThrottle):
    throttle.record_alert("my_pipeline")
    assert throttle.should_alert("my_pipeline") is False


def test_should_alert_after_interval_passes(state_file: Path):
    policy = ThrottlePolicy(min_interval_seconds=1)
    t = NotificationThrottle(state_path=state_file, policy=policy)
    t.record_alert("pipe")
    time.sleep(1.05)
    assert t.should_alert("pipe") is True


def test_record_alert_persists_state(throttle: NotificationThrottle, state_file: Path):
    throttle.record_alert("pipe_a")
    data = json.loads(state_file.read_text())
    assert "pipe_a" in data
    assert isinstance(data["pipe_a"], float)


def test_reset_clears_pipeline(throttle: NotificationThrottle):
    throttle.record_alert("pipe_b")
    throttle.reset("pipe_b")
    assert throttle.should_alert("pipe_b") is True


def test_reset_nonexistent_pipeline_is_safe(throttle: NotificationThrottle):
    throttle.reset("nonexistent")  # should not raise


def test_last_alert_time_none_before_record(throttle: NotificationThrottle):
    assert throttle.last_alert_time("pipe_c") is None


def test_last_alert_time_set_after_record(throttle: NotificationThrottle):
    before = time.time()
    throttle.record_alert("pipe_c")
    after = time.time()
    ts = throttle.last_alert_time("pipe_c")
    assert ts is not None
    assert before <= ts <= after


# --- build_throttle_from_config ---

def test_build_throttle_from_config_defaults(state_file: Path):
    t = build_throttle_from_config({}, state_path=state_file)
    assert t.policy.min_interval_seconds == 300


def test_build_throttle_from_config_custom_interval(state_file: Path):
    t = build_throttle_from_config({"min_interval_seconds": 600}, state_path=state_file)
    assert t.policy.min_interval_seconds == 600


def test_build_throttle_invalid_type_raises(state_file: Path):
    with pytest.raises(TypeError):
        build_throttle_from_config({"min_interval_seconds": "fast"}, state_path=state_file)


def test_build_throttle_non_dict_raises(state_file: Path):
    with pytest.raises(TypeError):
        build_throttle_from_config("bad", state_path=state_file)  # type: ignore
