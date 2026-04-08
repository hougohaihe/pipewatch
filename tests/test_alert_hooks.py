"""Tests for pipewatch alert hooks."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alert_hooks import AlertDispatcher, LogAlertHook, WebhookAlertHook

SAMPLE_RECORD = {
    "run_id": "abc-123",
    "pipeline_name": "etl_daily",
    "status": "failed",
    "error_message": "Connection timeout",
}


def test_log_alert_hook_returns_true():
    hook = LogAlertHook(level="ERROR")
    result = hook.send(SAMPLE_RECORD)
    assert result is True


def test_log_alert_hook_logs_message(caplog):
    import logging

    hook = LogAlertHook(level="WARNING")
    with caplog.at_level(logging.WARNING, logger="pipewatch.alert_hooks"):
        hook.send(SAMPLE_RECORD)
    assert "etl_daily" in caplog.text
    assert "abc-123" in caplog.text


def test_dispatcher_collects_results():
    hook1 = MagicMock(spec=LogAlertHook)
    hook1.send.return_value = True
    hook2 = MagicMock(spec=WebhookAlertHook)
    hook2.send.return_value = False

    dispatcher = AlertDispatcher()
    dispatcher.register(hook1)
    dispatcher.register(hook2)

    results = dispatcher.dispatch(SAMPLE_RECORD)
    assert results["LogAlertHook"] is True
    assert results["WebhookAlertHook"] is False
    hook1.send.assert_called_once_with(SAMPLE_RECORD)
    hook2.send.assert_called_once_with(SAMPLE_RECORD)


def test_dispatcher_handles_hook_exception():
    hook = MagicMock(spec=LogAlertHook)
    hook.send.side_effect = RuntimeError("boom")

    dispatcher = AlertDispatcher()
    dispatcher.register(hook)
    results = dispatcher.dispatch(SAMPLE_RECORD)
    assert results["LogAlertHook"] is False


def test_webhook_hook_success():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers["Content-Length"])
            received.append(json.loads(self.rfile.read(length)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):  # silence server logs
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = Thread(target=server.handle_request, daemon=True)
    thread.start()

    hook = WebhookAlertHook(url=f"http://127.0.0.1:{port}/hook", timeout=3)
    result = hook.send(SAMPLE_RECORD)
    thread.join(timeout=2)
    server.server_close()

    assert result is True
    assert len(received) == 1
    assert received[0]["run_id"] == "abc-123"


def test_webhook_hook_failure_returns_false():
    hook = WebhookAlertHook(url="http://127.0.0.1:19999/nonexistent", timeout=1)
    result = hook.send(SAMPLE_RECORD)
    assert result is False
