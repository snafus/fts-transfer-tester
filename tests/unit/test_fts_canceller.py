"""Unit tests for fts_framework.fts.canceller."""

import json
import os
import pytest

from fts_framework.fts.canceller import cancel_jobs, collect_job_ids_from_sequence


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class _FakeResponse(object):
    def __init__(self, status_code):
        self.status_code = status_code


class _FakeClient(object):
    def __init__(self, responses):
        self._responses = list(responses)
        self.deleted = []

    def delete(self, path, **kwargs):
        self.deleted.append(path)
        if not self._responses:
            raise AssertionError("Unexpected delete call")
        r = self._responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


# ---------------------------------------------------------------------------
# cancel_jobs
# ---------------------------------------------------------------------------

class TestCancelJobs:
    def test_200_marks_cancelled(self):
        client = _FakeClient([_FakeResponse(200)])
        results = cancel_jobs(["job-1"], client)
        assert results[0]["cancelled"] is True
        assert results[0]["status_code"] == 200
        assert results[0]["error"] is None

    def test_204_marks_cancelled(self):
        client = _FakeClient([_FakeResponse(204)])
        results = cancel_jobs(["job-1"], client)
        assert results[0]["cancelled"] is True

    def test_404_treated_as_cancelled(self):
        client = _FakeClient([_FakeResponse(404)])
        results = cancel_jobs(["job-1"], client)
        assert results[0]["cancelled"] is True
        assert results[0]["status_code"] == 404

    def test_500_not_cancelled_records_error(self):
        client = _FakeClient([_FakeResponse(500)])
        results = cancel_jobs(["job-1"], client)
        assert results[0]["cancelled"] is False
        assert "500" in results[0]["error"]

    def test_connection_error_not_cancelled_records_error(self):
        import requests
        client = _FakeClient([requests.exceptions.ConnectionError("refused")])
        results = cancel_jobs(["job-1"], client)
        assert results[0]["cancelled"] is False
        assert results[0]["error"] is not None

    def test_empty_list_returns_empty(self):
        client = _FakeClient([])
        assert cancel_jobs([], client) == []

    def test_multiple_jobs_all_attempted(self):
        client = _FakeClient([_FakeResponse(200), _FakeResponse(200), _FakeResponse(200)])
        results = cancel_jobs(["j1", "j2", "j3"], client)
        assert len(results) == 3
        assert client.deleted == ["/jobs/j1", "/jobs/j2", "/jobs/j3"]

    def test_one_failure_does_not_abort_remaining(self):
        client = _FakeClient([_FakeResponse(500), _FakeResponse(200)])
        results = cancel_jobs(["j1", "j2"], client)
        assert results[0]["cancelled"] is False
        assert results[1]["cancelled"] is True

    def test_result_contains_job_id(self):
        client = _FakeClient([_FakeResponse(200)])
        results = cancel_jobs(["job-abc"], client)
        assert results[0]["job_id"] == "job-abc"

    def test_delete_path_correct(self):
        client = _FakeClient([_FakeResponse(200)])
        cancel_jobs(["job-xyz-123"], client)
        assert client.deleted == ["/jobs/job-xyz-123"]


# ---------------------------------------------------------------------------
# collect_job_ids_from_sequence
# ---------------------------------------------------------------------------

def _write_json(path, data):
    with open(path, "w") as fh:
        json.dump(data, fh)


def _make_sequence(tmp_path, trials, runs_dir=None):
    """Build a minimal sequence directory with state.json and run manifests."""
    seq_dir = str(tmp_path / "seq")
    os.makedirs(seq_dir)
    rdir = runs_dir or str(tmp_path / "runs")
    os.makedirs(rdir, exist_ok=True)

    state = {
        "sequence_id": "test_seq",
        "baseline_config": "config/test.yaml",
        "runs_dir": rdir,
        "cases": [{"case_index": 0, "params": {}, "trials": trials}],
    }
    _write_json(os.path.join(seq_dir, "state.json"), state)
    return seq_dir, rdir


def _make_manifest(runs_dir, run_id, subjobs):
    run_path = os.path.join(runs_dir, run_id)
    os.makedirs(run_path, exist_ok=True)
    _write_json(os.path.join(run_path, "manifest.json"), {"subjobs": subjobs})


class TestCollectJobIds:
    def test_non_terminal_jobs_collected(self, tmp_path):
        seq_dir, rdir = _make_sequence(tmp_path, [
            {"trial_index": 0, "run_id": "run-1", "status": "running"},
        ])
        _make_manifest(rdir, "run-1", [
            {"job_id": "job-a", "terminal": False},
        ])
        ids = collect_job_ids_from_sequence(seq_dir)
        assert ids == ["job-a"]

    def test_terminal_jobs_excluded(self, tmp_path):
        seq_dir, rdir = _make_sequence(tmp_path, [
            {"trial_index": 0, "run_id": "run-1", "status": "completed"},
        ])
        _make_manifest(rdir, "run-1", [
            {"job_id": "job-a", "terminal": True},
            {"job_id": "job-b", "terminal": False},
        ])
        ids = collect_job_ids_from_sequence(seq_dir)
        assert ids == ["job-b"]

    def test_trial_without_run_id_skipped(self, tmp_path):
        seq_dir, rdir = _make_sequence(tmp_path, [
            {"trial_index": 0, "run_id": None, "status": "pending"},
        ])
        ids = collect_job_ids_from_sequence(seq_dir)
        assert ids == []

    def test_missing_manifest_skipped(self, tmp_path):
        seq_dir, rdir = _make_sequence(tmp_path, [
            {"trial_index": 0, "run_id": "run-missing", "status": "running"},
        ])
        ids = collect_job_ids_from_sequence(seq_dir)
        assert ids == []

    def test_duplicate_job_ids_deduplicated(self, tmp_path):
        seq_dir, rdir = _make_sequence(tmp_path, [
            {"trial_index": 0, "run_id": "run-1", "status": "running"},
            {"trial_index": 1, "run_id": "run-2", "status": "running"},
        ])
        _make_manifest(rdir, "run-1", [{"job_id": "job-shared", "terminal": False}])
        _make_manifest(rdir, "run-2", [{"job_id": "job-shared", "terminal": False}])
        ids = collect_job_ids_from_sequence(seq_dir)
        assert ids.count("job-shared") == 1

    def test_multiple_trials_all_collected(self, tmp_path):
        seq_dir, rdir = _make_sequence(tmp_path, [
            {"trial_index": 0, "run_id": "run-1", "status": "running"},
            {"trial_index": 1, "run_id": "run-2", "status": "running"},
        ])
        _make_manifest(rdir, "run-1", [{"job_id": "job-a", "terminal": False}])
        _make_manifest(rdir, "run-2", [{"job_id": "job-b", "terminal": False}])
        ids = collect_job_ids_from_sequence(seq_dir)
        assert set(ids) == {"job-a", "job-b"}

    def test_subjob_without_job_id_skipped(self, tmp_path):
        seq_dir, rdir = _make_sequence(tmp_path, [
            {"trial_index": 0, "run_id": "run-1", "status": "running"},
        ])
        _make_manifest(rdir, "run-1", [
            {"job_id": None, "terminal": False},
            {"job_id": "job-real", "terminal": False},
        ])
        ids = collect_job_ids_from_sequence(seq_dir)
        assert ids == ["job-real"]

    def test_empty_sequence_returns_empty(self, tmp_path):
        seq_dir, rdir = _make_sequence(tmp_path, [])
        ids = collect_job_ids_from_sequence(seq_dir)
        assert ids == []
