import json
import pytest
from pathlib import Path
from pipewatch.run_timeline import RunTimeline, TimelineEvent, TimelineError


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def timeline(log_file):
    return RunTimeline(str(log_file))


_RECORDS = [
    {"run_id": "a1", "pipeline": "etl", "status": "success", "started_at": "2024-01-01T08:00:00", "ended_at": "2024-01-01T08:05:00", "duration_seconds": 300.0, "tags": ["prod"]},
    {"run_id": "b2", "pipeline": "etl", "status": "failure", "started_at": "2024-01-02T08:00:00", "ended_at": "2024-01-02T08:03:00", "duration_seconds": 180.0, "tags": []},
    {"run_id": "c3", "pipeline": "ingest", "status": "success", "started_at": "2024-01-01T09:00:00", "ended_at": None, "duration_seconds": None, "tags": ["staging"]},
]


def test_all_events_returns_empty_for_missing_file(timeline):
    result = timeline.all_events()
    assert result == []


def test_all_events_returns_timeline_event_instances(log_file, timeline):
    _write_records(log_file, _RECORDS)
    result = timeline.all_events()
    assert all(isinstance(e, TimelineEvent) for e in result)


def test_all_events_sorted_by_started_at(log_file, timeline):
    _write_records(log_file, _RECORDS)
    result = timeline.all_events()
    starts = [e.started_at for e in result]
    assert starts == sorted(starts)


def test_all_events_count(log_file, timeline):
    _write_records(log_file, _RECORDS)
    result = timeline.all_events()
    assert len(result) == 3


def test_for_pipeline_filters_correctly(log_file, timeline):
    _write_records(log_file, _RECORDS)
    result = timeline.for_pipeline("etl")
    assert len(result) == 2
    assert all(e.pipeline == "etl" for e in result)


def test_for_pipeline_returns_empty_for_unknown(log_file, timeline):
    _write_records(log_file, _RECORDS)
    result = timeline.for_pipeline("nonexistent")
    assert result == []


def test_for_pipeline_sorted_by_started_at(log_file, timeline):
    _write_records(log_file, _RECORDS)
    result = timeline.for_pipeline("etl")
    starts = [e.started_at for e in result]
    assert starts == sorted(starts)


def test_between_returns_events_in_range(log_file, timeline):
    _write_records(log_file, _RECORDS)
    result = timeline.between("2024-01-01T00:00:00", "2024-01-01T23:59:59")
    assert len(result) == 2
    assert all(e.started_at.startswith("2024-01-01") for e in result)


def test_between_raises_when_start_after_end(timeline):
    with pytest.raises(TimelineError):
        timeline.between("2024-01-10T00:00:00", "2024-01-01T00:00:00")


def test_event_to_dict_contains_expected_keys(log_file, timeline):
    _write_records(log_file, _RECORDS)
    event = timeline.all_events()[0]
    d = event.to_dict()
    assert set(d.keys()) == {"run_id", "pipeline", "status", "started_at", "ended_at", "duration_seconds", "tags"}


def test_event_tags_populated(log_file, timeline):
    _write_records(log_file, _RECORDS)
    events = {e.run_id: e for e in timeline.all_events()}
    assert events["a1"].tags == ["prod"]
    assert events["b2"].tags == []


def test_event_optional_fields_none_when_absent(log_file, timeline):
    _write_records(log_file, [{"run_id": "x9", "pipeline": "p", "status": "running", "started_at": "2024-03-01T10:00:00"}])
    result = timeline.all_events()
    assert result[0].ended_at is None
    assert result[0].duration_seconds is None
