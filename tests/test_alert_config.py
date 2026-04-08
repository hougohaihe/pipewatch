"""Tests for alert dispatcher config loading."""

import json
from pathlib import Path

import pytest

from pipewatch.alert_config import build_dispatcher_from_config, load_dispatcher_from_file
from pipewatch.alert_hooks import AlertDispatcher, LogAlertHook, WebhookAlertHook


def test_build_dispatcher_empty_config():
    dispatcher = build_dispatcher_from_config({})
    assert isinstance(dispatcher, AlertDispatcher)
    assert dispatcher._hooks == []


def test_build_dispatcher_log_hook():
    config = {"alerts": [{"type": "log", "level": "ERROR"}]}
    dispatcher = build_dispatcher_from_config(config)
    assert len(dispatcher._hooks) == 1
    assert isinstance(dispatcher._hooks[0], LogAlertHook)
    assert dispatcher._hooks[0].level == 40  # logging.ERROR


def test_build_dispatcher_webhook_hook():
    config = {"alerts": [{"type": "webhook", "url": "https://example.com/hook"}]}
    dispatcher = build_dispatcher_from_config(config)
    assert len(dispatcher._hooks) == 1
    assert isinstance(dispatcher._hooks[0], WebhookAlertHook)
    assert dispatcher._hooks[0].url == "https://example.com/hook"


def test_build_dispatcher_multiple_hooks():
    config = {
        "alerts": [
            {"type": "log"},
            {"type": "webhook", "url": "https://hooks.example.com/pw"},
        ]
    }
    dispatcher = build_dispatcher_from_config(config)
    assert len(dispatcher._hooks) == 2


def test_build_dispatcher_unknown_type_raises():
    config = {"alerts": [{"type": "slack"}]}
    with pytest.raises(ValueError, match="Unknown alert hook type"):
        build_dispatcher_from_config(config)


def test_load_dispatcher_from_file(tmp_path: Path):
    cfg = {"alerts": [{"type": "log", "level": "WARNING"}]}
    cfg_file = tmp_path / "alert_config.json"
    cfg_file.write_text(json.dumps(cfg))

    dispatcher = load_dispatcher_from_file(cfg_file)
    assert len(dispatcher._hooks) == 1
    assert isinstance(dispatcher._hooks[0], LogAlertHook)


def test_load_dispatcher_missing_file_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_dispatcher_from_file(tmp_path / "nonexistent.json")
