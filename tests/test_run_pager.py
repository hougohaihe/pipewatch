import json
import pytest
from pathlib import Path
from pipewatch.run_pager import PagerError, PageResult, RunPager


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.jsonl"


@pytest.fixture
def pager(log_file):
    records = [
        {"run_id": f"r{i}", "pipeline": "etl" if i % 2 == 0 else "ingest", "status": "success"}
        for i in range(45)
    ]
    _write_records(log_file, records)
    return RunPager(str(log_file), page_size=10)


def test_get_page_returns_page_result(pager):
    result = pager.get_page(1)
    assert isinstance(result, PageResult)


def test_get_page_first_page_length(pager):
    result = pager.get_page(1)
    assert len(result.records) == 10


def test_get_page_last_page_partial(pager):
    result = pager.get_page(5)
    assert len(result.records) == 5


def test_get_page_total_records(pager):
    result = pager.get_page(1)
    assert result.total_records == 45


def test_get_page_total_pages(pager):
    result = pager.get_page(1)
    assert result.total_pages == 5


def test_get_page_pipeline_filter(pager):
    result = pager.get_page(1, pipeline="etl")
    assert all(r["pipeline"] == "etl" for r in result.records)


def test_get_page_pipeline_filter_total(pager):
    result = pager.get_page(1, pipeline="etl")
    assert result.total_records == 23  # indices 0,2,4,...,44 => 23 even numbers


def test_get_page_missing_file_returns_empty(tmp_path):
    pager = RunPager(str(tmp_path / "missing.jsonl"), page_size=10)
    result = pager.get_page(1)
    assert result.records == []
    assert result.total_records == 0


def test_get_page_invalid_page_raises(pager):
    with pytest.raises(PagerError, match="page must be at least 1"):
        pager.get_page(0)


def test_invalid_page_size_raises(log_file):
    with pytest.raises(PagerError, match="page_size must be at least 1"):
        RunPager(str(log_file), page_size=0)


def test_to_dict_contains_expected_keys(pager):
    result = pager.get_page(1)
    d = result.to_dict()
    assert set(d.keys()) == {"page", "page_size", "total_records", "total_pages", "records"}


def test_single_page_when_records_fit(tmp_path):
    lf = tmp_path / "small.jsonl"
    _write_records(lf, [{"run_id": "x", "pipeline": "p", "status": "success"}])
    pager = RunPager(str(lf), page_size=20)
    result = pager.get_page(1)
    assert result.total_pages == 1
