"""Tests for PipelineRunner."""

import pytest
from unittest.mock import MagicMock, patch

from pipewatch.pipeline_runner import PipelineRunner
from pipewatch.run_logger import RunLogger, RunStatus


@pytest.fixture()
def mock_logger():
    """Create a mock RunLogger with default return values."""
    logger = MagicMock(spec=RunLogger)
    logger.log_start.return_value = "run-abc"
    return logger


@pytest.fixture()
def mock_dispatcher():
    """Create a mock AlertDispatcher."""
    from pipewatch.alert_hooks import AlertDispatcher
    return MagicMock(spec=AlertDispatcher)


def test_run_calls_log_start(mock_logger):
    runner = PipelineRunner("my_pipe", mock_logger)
    runner.run(lambda: None)
    mock_logger.log_start.assert_called_once_with("my_pipe")


def test_run_success_calls_log_success(mock_logger):
    runner = PipelineRunner("my_pipe", mock_logger)
    runner.run(lambda: 42)
    mock_logger.log_success.assert_called_once()
    args, kwargs = mock_logger.log_success.call_args
    assert args[0] == "run-abc"
    assert kwargs["duration_seconds"] >= 0


def test_run_returns_fn_result(mock_logger):
    runner = PipelineRunner("my_pipe", mock_logger)
    result = runner.run(lambda: "output")
    assert result == "output"


def test_run_failure_calls_log_failure(mock_logger):
    def bad_fn():
        raise ValueError("boom")

    runner = PipelineRunner("my_pipe", mock_logger)
    result = runner.run(bad_fn)
    assert result is None
    mock_logger.log_failure.assert_called_once()
    args, kwargs = mock_logger.log_failure.call_args
    assert args[0] == "run-abc"
    assert "boom" in kwargs["error"]
    assert "traceback" in kwargs


def test_run_success_dispatches_info_alert(mock_logger, mock_dispatcher):
    runner = PipelineRunner("my_pipe", mock_logger, dispatcher=mock_dispatcher)
    runner.run(lambda: None)
    mock_dispatcher.dispatch.assert_called_once()
    _, kwargs = mock_dispatcher.dispatch.call_args
    assert kwargs["level"] == "info"
    assert "my_pipe" in kwargs["message"]


def test_run_failure_dispatches_error_alert(mock_logger, mock_dispatcher):
    runner = PipelineRunner("my_pipe", mock_logger, dispatcher=mock_dispatcher)
    runner.run(lambda: (_ for _ in ()).throw(RuntimeError("fail")))
    mock_dispatcher.dispatch.assert_called_once()
    _, kwargs = mock_dispatcher.dispatch.call_args
    assert kwargs["level"] == "error"
    assert "FAILED" in kwargs["message"]


def test_run_without_dispatcher_does_not_raise(mock_logger):
    runner = PipelineRunner("my_pipe", mock_logger, dispatcher=None)
    runner.run(lambda: None)
    mock_logger.log_success.assert_called_once()
