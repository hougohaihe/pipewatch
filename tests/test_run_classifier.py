import json
import pytest
from pathlib import Path
from pipewatch.run_classifier import RunClassifier, PipelineClass


def _write_records(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "runs.log"


@pytest.fixture
def classifier(log_file):
    return RunClassifier(str(log_file))


def test_classify_all_empty_file(classifier):
    assert classifier.classify_all() == []


def test_classify_all_missing_file(tmp_path):
    c = RunClassifier(str(tmp_path / "nonexistent.log"))
    assert c.classify_all() == []


def test_classify_all_returns_pipeline_class_instances(log_file, classifier):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10},
        {"pipeline": "etl", "status": "success", "duration_seconds": 20},
    ])
    results = classifier.classify_all()
    assert len(results) == 1
    assert isinstance(results[0], PipelineClass)


def test_classify_healthy_pipeline(log_file, classifier):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 5},
        {"pipeline": "etl", "status": "success", "duration_seconds": 6},
        {"pipeline": "etl", "status": "success", "duration_seconds": 7},
    ])
    pc = classifier.classify_pipeline("etl")
    assert pc is not None
    assert pc.label == RunClassifier.LABEL_HEALTHY


def test_classify_flaky_pipeline(log_file, classifier):
    _write_records(log_file, [
        {"pipeline": "ingest", "status": "success", "duration_seconds": 3},
        {"pipeline": "ingest", "status": "failure", "duration_seconds": 1},
        {"pipeline": "ingest", "status": "success", "duration_seconds": 4},
        {"pipeline": "ingest", "status": "success", "duration_seconds": 4},
    ])
    pc = classifier.classify_pipeline("ingest")
    assert pc is not None
    assert pc.label == RunClassifier.LABEL_FLAKY


def test_classify_failing_pipeline(log_file, classifier):
    _write_records(log_file, [
        {"pipeline": "sync", "status": "failure", "duration_seconds": 2},
        {"pipeline": "sync", "status": "failure", "duration_seconds": 2},
        {"pipeline": "sync", "status": "failure", "duration_seconds": 2},
        {"pipeline": "sync", "status": "success", "duration_seconds": 5},
    ])
    pc = classifier.classify_pipeline("sync")
    assert pc is not None
    assert pc.label == RunClassifier.LABEL_FAILING


def test_classify_inactive_pipeline(log_file):
    c = RunClassifier(str(log_file), min_runs=3)
    _write_records(log_file, [
        {"pipeline": "rare", "status": "success", "duration_seconds": 10},
    ])
    pc = c.classify_pipeline("rare")
    assert pc is not None
    assert pc.label == RunClassifier.LABEL_INACTIVE


def test_classify_pipeline_returns_none_for_unknown(log_file, classifier):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 5},
    ])
    assert classifier.classify_pipeline("unknown_pipe") is None


def test_avg_duration_calculated(log_file, classifier):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 10},
        {"pipeline": "etl", "status": "success", "duration_seconds": 20},
    ])
    pc = classifier.classify_pipeline("etl")
    assert pc is not None
    assert pc.avg_duration == 15.0


def test_to_dict_structure(log_file, classifier):
    _write_records(log_file, [
        {"pipeline": "etl", "status": "success", "duration_seconds": 8},
        {"pipeline": "etl", "status": "success", "duration_seconds": 12},
    ])
    pc = classifier.classify_pipeline("etl")
    d = pc.to_dict()
    assert set(d.keys()) == {"pipeline", "label", "run_count", "failure_rate", "avg_duration"}
    assert d["pipeline"] == "etl"
    assert d["run_count"] == 2
