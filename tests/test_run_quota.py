from __future__ import annotations

import json
import pytest
from datetime import datetime, timezone, timedelta
from pathlib import Path

from pipewatch.run_quota import QuotaError, PipelineQuota, RunQuota


def _ts(offset_days: int = 0) -> str:
    dt = datetime.now(timezone.utc) + timedelta(days=offset_days)
    return dt.isoformat()


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    return tmp_path / "runs.log"


@pytest.fixture
def quota(log_file: Path) -> RunQuota:
    return RunQuota(str(log_file), max_runs_per_day=3)


def test_check_returns_pipeline_quota_instance(quota, log_file):
    _write_records(log_file, [])
    result = quota.check("pipe_a")
    assert isinstance(result, PipelineQuota)


def test_check_runs_today_zero_for_missing_file(quota):
    result = quota.check("pipe_a")
    assert result.runs_today == 0
    assert result.exceeded is False


def test_check_counts_todays_runs(quota, log_file):
    records = [
        {"pipeline": "pipe_a", "started_at": _ts(0)},
        {"pipeline": "pipe_a", "started_at": _ts(0)},
        {"pipeline": "pipe_b", "started_at": _ts(0)},
    ]
    _write_records(log_file, records)
    result = quota.check("pipe_a")
    assert result.runs_today == 2


def test_check_excludes_old_runs(quota, log_file):
    records = [
        {"pipeline": "pipe_a", "started_at": _ts(-1)},
        {"pipeline": "pipe_a", "started_at": _ts(0)},
    ]
    _write_records(log_file, records)
    result = quota.check("pipe_a")
    assert result.runs_today == 1


def test_check_exceeded_when_at_limit(quota, log_file):
    records = [{"pipeline": "pipe_a", "started_at": _ts(0)} for _ in range(3)]
    _write_records(log_file, records)
    result = quota.check("pipe_a")
    assert result.exceeded is True


def test_check_not_exceeded_below_limit(quota, log_file):
    records = [{"pipeline": "pipe_a", "started_at": _ts(0)} for _ in range(2)]
    _write_records(log_file, records)
    result = quota.check("pipe_a")
    assert result.exceeded is False


def test_check_all_returns_all_pipelines(quota, log_file):
    records = [
        {"pipeline": "pipe_a", "started_at": _ts(0)},
        {"pipeline": "pipe_b", "started_at": _ts(0)},
    ]
    _write_records(log_file, records)
    results = quota.check_all()
    names = [r.pipeline for r in results]
    assert "pipe_a" in names
    assert "pipe_b" in names


def test_check_all_empty_file_returns_empty_list(quota, log_file):
    _write_records(log_file, [])
    assert quota.check_all() == []


def test_to_dict_has_expected_keys(quota, log_file):
    _write_records(log_file, [])
    d = quota.check("pipe_a").to_dict()
    assert set(d.keys()) == {"pipeline", "max_runs_per_day", "runs_today", "exceeded"}


def test_invalid_max_runs_raises(log_file):
    with pytest.raises(QuotaError):
        RunQuota(str(log_file), max_runs_per_day=0)


def test_empty_pipeline_name_raises(quota):
    with pytest.raises(QuotaError):
        quota.check("")
