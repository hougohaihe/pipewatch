"""Tests for pipewatch.retention_config."""

import json
from pathlib import Path

import pytest

from pipewatch.retention_config import build_policy_from_config, load_policy_from_file
from pipewatch.retention_policy import RetentionPolicy


def test_build_policy_empty_config():
    policy = build_policy_from_config({})
    assert isinstance(policy, RetentionPolicy)
    assert policy.max_age_days is None
    assert policy.max_runs is None


def test_build_policy_max_age_days():
    policy = build_policy_from_config({"max_age_days": 30})
    assert policy.max_age_days == 30
    assert policy.max_runs is None


def test_build_policy_max_runs():
    policy = build_policy_from_config({"max_runs": 500})
    assert policy.max_runs == 500
    assert policy.max_age_days is None


def test_build_policy_both_fields():
    policy = build_policy_from_config({"max_age_days": 7, "max_runs": 100})
    assert policy.max_age_days == 7
    assert policy.max_runs == 100


def test_build_policy_invalid_age_type():
    with pytest.raises(ValueError, match="max_age_days must be an integer"):
        build_policy_from_config({"max_age_days": "30"})


def test_build_policy_invalid_runs_type():
    with pytest.raises(ValueError, match="max_runs must be an integer"):
        build_policy_from_config({"max_runs": 50.5})


def test_build_policy_zero_age_raises():
    with pytest.raises(ValueError, match="positive integer"):
        build_policy_from_config({"max_age_days": 0})


def test_build_policy_negative_runs_raises():
    with pytest.raises(ValueError, match="positive integer"):
        build_policy_from_config({"max_runs": -1})


def test_load_policy_from_json_file(tmp_path):
    config_file = tmp_path / "retention.json"
    config_file.write_text(json.dumps({"max_age_days": 14, "max_runs": 200}))
    policy = load_policy_from_file(config_file)
    assert policy.max_age_days == 14
    assert policy.max_runs == 200


def test_load_policy_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_policy_from_file(tmp_path / "missing.json")


def test_load_policy_unsupported_format(tmp_path):
    bad_file = tmp_path / "retention.yaml"
    bad_file.write_text("max_age_days: 7\n")
    with pytest.raises(ValueError, match="Unsupported config file format"):
        load_policy_from_file(bad_file)


def test_load_policy_non_object_json(tmp_path):
    config_file = tmp_path / "retention.json"
    config_file.write_text(json.dumps([1, 2, 3]))
    with pytest.raises(ValueError, match="JSON object"):
        load_policy_from_file(config_file)
