import json
import pytest
from pathlib import Path
from pipewatch.run_cluster import ClusterError, PipelineCluster, RunCluster


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def clusterer(log_file):
    _write_records(
        log_file,
        [
            {"pipeline": "etl", "status": "success", "env": "prod", "duration_seconds": 10.0},
            {"pipeline": "etl", "status": "failure", "env": "prod", "duration_seconds": 5.0},
            {"pipeline": "ingest", "status": "success", "env": "prod", "duration_seconds": 8.0},
            {"pipeline": "report", "status": "success", "env": "dev", "duration_seconds": 3.0},
            {"pipeline": "report", "status": "failure", "env": "dev", "duration_seconds": 2.0},
        ],
    )
    return RunCluster(str(log_file))


def test_cluster_by_returns_empty_for_missing_file(tmp_path):
    rc = RunCluster(str(tmp_path / "missing.log"))
    assert rc.cluster_by("env") == {}


def test_cluster_by_returns_pipeline_cluster_instances(clusterer):
    result = clusterer.cluster_by("env")
    assert all(isinstance(v, PipelineCluster) for v in result.values())


def test_cluster_by_correct_keys(clusterer):
    result = clusterer.cluster_by("env")
    assert set(result.keys()) == {"prod", "dev"}


def test_cluster_by_pipeline_list(clusterer):
    result = clusterer.cluster_by("env")
    assert sorted(result["prod"].pipelines) == ["etl", "ingest"]
    assert result["dev"].pipelines == ["report"]


def test_cluster_by_run_count(clusterer):
    result = clusterer.cluster_by("env")
    assert result["prod"].run_count == 3
    assert result["dev"].run_count == 2


def test_cluster_by_avg_duration(clusterer):
    result = clusterer.cluster_by("env")
    assert result["prod"].avg_duration == pytest.approx(7.6667, abs=1e-3)
    assert result["dev"].avg_duration == pytest.approx(2.5, abs=1e-3)


def test_cluster_by_success_rate(clusterer):
    result = clusterer.cluster_by("env")
    assert result["prod"].success_rate == pytest.approx(2 / 3, abs=1e-4)
    assert result["dev"].success_rate == pytest.approx(0.5, abs=1e-4)


def test_cluster_skips_records_without_field(tmp_path):
    lf = tmp_path / "runs.log"
    _write_records(
        lf,
        [
            {"pipeline": "etl", "status": "success", "env": "prod"},
            {"pipeline": "etl", "status": "success"},
        ],
    )
    rc = RunCluster(str(lf))
    result = rc.cluster_by("env")
    assert list(result.keys()) == ["prod"]


def test_cluster_to_dict_has_expected_keys(clusterer):
    result = clusterer.cluster_by("env")
    d = result["prod"].to_dict()
    assert set(d.keys()) == {"key", "pipelines", "run_count", "avg_duration", "success_rate"}


def test_cluster_raises_for_empty_field(clusterer):
    with pytest.raises(ClusterError):
        clusterer.cluster_by("")


def test_cluster_raises_for_invalid_log_file():
    with pytest.raises(ClusterError):
        RunCluster("")
