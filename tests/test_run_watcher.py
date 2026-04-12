"""Tests for pipewatch.run_watcher."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_watcher import RunWatcher, WatcherError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _append(log_file: Path, record: dict) -> None:
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


@pytest.fixture()
def watcher(log_file: Path) -> RunWatcher:
    return RunWatcher(log_file=str(log_file), poll_interval=0.0)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_register_accepts_callable(watcher: RunWatcher) -> None:
    watcher.register(lambda r: None)
    assert len(watcher._handlers) == 1


def test_register_rejects_non_callable(watcher: RunWatcher) -> None:
    with pytest.raises(WatcherError, match="callable"):
        watcher.register("not_a_function")  # type: ignore[arg-type]


def test_poll_dispatches_new_records(watcher: RunWatcher, log_file: Path) -> None:
    seen: list[dict] = []
    watcher.register(seen.append)

    _append(log_file, {"run_id": "a", "status": "success"})
    _append(log_file, {"run_id": "b", "status": "failure"})

    watcher._poll()

    assert len(seen) == 2
    assert seen[0]["run_id"] == "a"
    assert seen[1]["run_id"] == "b"


def test_poll_does_not_replay_old_records(watcher: RunWatcher, log_file: Path) -> None:
    seen: list[dict] = []
    watcher.register(seen.append)

    _append(log_file, {"run_id": "a"})
    watcher._poll()
    assert len(seen) == 1

    # Second poll — no new lines
    watcher._poll()
    assert len(seen) == 1  # still 1


def test_poll_picks_up_incremental_writes(watcher: RunWatcher, log_file: Path) -> None:
    seen: list[dict] = []
    watcher.register(seen.append)

    _append(log_file, {"run_id": "first"})
    watcher._poll()
    assert len(seen) == 1

    _append(log_file, {"run_id": "second"})
    watcher._poll()
    assert len(seen) == 2
    assert seen[1]["run_id"] == "second"


def test_poll_skips_missing_file(watcher: RunWatcher) -> None:
    """Should not raise when the log file does not exist yet."""
    watcher._poll()  # no exception


def test_poll_skips_invalid_json(watcher: RunWatcher, log_file: Path) -> None:
    seen: list[dict] = []
    watcher.register(seen.append)

    log_file.write_text("not json\n{\"run_id\": \"ok\"}\n", encoding="utf-8")
    watcher._poll()

    assert len(seen) == 1
    assert seen[0]["run_id"] == "ok"


def test_start_stops_after_max_iterations(watcher: RunWatcher, log_file: Path) -> None:
    counts: list[int] = []
    watcher.register(lambda r: counts.append(1))

    _append(log_file, {"run_id": "x"})
    watcher.start(max_iterations=1)

    assert len(counts) == 1


def test_handler_exception_does_not_abort_dispatch(
    watcher: RunWatcher, log_file: Path
) -> None:
    results: list[dict] = []

    def bad_handler(r: dict) -> None:
        raise RuntimeError("boom")

    watcher.register(bad_handler)
    watcher.register(results.append)

    _append(log_file, {"run_id": "z"})
    watcher._poll()

    assert len(results) == 1
