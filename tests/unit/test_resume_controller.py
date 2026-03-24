"""
Unit tests for fts_framework.resume.controller.

All file I/O uses tmp_path.  FTS3 calls use _FakeClient (no real HTTP).
"""

import json
import os

import pytest

from fts_framework.resume.controller import load, run_exists, _scan_for_job
from fts_framework.exceptions import SubmissionError, TokenExpiredError
from fts_framework.persistence import store


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(scan_window_s=300):
    return {
        "run": {"test_label": "test"},
        "fts": {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
        "tokens": {
            "fts_submit": "tok_s", "source_read": "tok_r", "dest_write": "tok_w",
        },
        "submission": {"scan_window_s": scan_window_s},
    }


def _subjob(job_id="job-1", chunk_index=0, retry_round=0,
            status="FINISHED", terminal=True):
    return {
        "job_id": job_id,
        "chunk_index": chunk_index,
        "retry_round": retry_round,
        "run_id": "run-001",
        "submitted_at": "2026-01-01T00:00:00Z",
        "file_count": 2,
        "status": status,
        "terminal": terminal,
        "payload_path": "submitted_payloads/chunk_0000_r0.json",
    }


def _setup_run(tmp_path, run_id="run-001", subjobs=None):
    """Create a minimal run directory with manifest."""
    cfg = _config()
    store.init_run_directory(run_id, cfg, runs_dir=str(tmp_path))
    store.write_manifest(run_id, {}, cfg, runs_dir=str(tmp_path))
    if subjobs:
        store.update_manifest(run_id, subjobs, runs_dir=str(tmp_path))
    return str(tmp_path)


def _write_payload(tmp_path, run_id, chunk_index, retry_round, file_count=2):
    """Write a synthetic payload file to submitted_payloads/."""
    filename = "chunk_{:04d}_r{}.json".format(chunk_index, retry_round)
    payload = {
        "files": [{"source_surl": "https://src/f{}".format(i),
                   "dest_surl": "https://dst/f{}".format(i)}
                  for i in range(file_count)],
        "params": {},
        "job_metadata": {
            "run_id": run_id,
            "chunk_index": chunk_index,
            "retry_round": retry_round,
        },
    }
    path = os.path.join(str(tmp_path), run_id, "submitted_payloads", filename)
    with open(path, "w") as fh:
        json.dump(payload, fh)
    return path


class _FakeClient:
    """Configurable fake FTS3 client."""

    def __init__(self, responses=None):
        # responses: list of (value_or_exception) consumed in order
        self._responses = list(responses or [])
        self.get_calls = []
        self.post_calls = []

    def get(self, path, **kwargs):
        self.get_calls.append(path)
        if not self._responses:
            raise AssertionError("Unexpected get() for {}".format(path))
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def post(self, path, payload=None, **kwargs):
        self.post_calls.append(path)
        if not self._responses:
            raise AssertionError("Unexpected post() for {}".format(path))
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _FakeResponse:
    """Minimal requests.Response stand-in."""

    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body or {}

    def json(self):
        return self._body


# ---------------------------------------------------------------------------
# run_exists
# ---------------------------------------------------------------------------

class TestRunExists:
    def test_returns_false_when_no_directory(self, tmp_path):
        assert run_exists("no-such-run", runs_dir=str(tmp_path)) is False

    def test_returns_false_when_directory_but_no_manifest(self, tmp_path):
        os.makedirs(os.path.join(str(tmp_path), "run-partial"))
        assert run_exists("run-partial", runs_dir=str(tmp_path)) is False

    def test_returns_true_when_manifest_exists(self, tmp_path):
        _setup_run(tmp_path, "run-001")
        assert run_exists("run-001", runs_dir=str(tmp_path)) is True


# ---------------------------------------------------------------------------
# load — all-terminal run (nothing to poll or submit)
# ---------------------------------------------------------------------------

class TestLoadAllTerminal:
    def test_returns_all_terminal_subjobs(self, tmp_path):
        _setup_run(tmp_path, "r1", subjobs=[
            _subjob("job-1", chunk_index=0, terminal=True),
            _subjob("job-2", chunk_index=1, terminal=True),
        ])
        client = _FakeClient()
        result = load("r1", client, _config(), runs_dir=str(tmp_path))
        assert len(result) == 2

    def test_no_fts_calls_when_no_missing_payloads(self, tmp_path):
        _setup_run(tmp_path, "r1", subjobs=[_subjob("job-1", terminal=True)])
        client = _FakeClient()
        load("r1", client, _config(), runs_dir=str(tmp_path))
        assert len(client.get_calls) == 0
        assert len(client.post_calls) == 0

    def test_non_terminal_subjob_included(self, tmp_path):
        _setup_run(tmp_path, "r1", subjobs=[
            _subjob("job-1", terminal=False, status="ACTIVE"),
        ])
        client = _FakeClient()
        result = load("r1", client, _config(), runs_dir=str(tmp_path))
        assert result[0]["terminal"] is False

    def test_empty_subjobs_no_payloads_returns_empty(self, tmp_path):
        _setup_run(tmp_path, "r1")
        client = _FakeClient()
        result = load("r1", client, _config(), runs_dir=str(tmp_path))
        assert result == []


# ---------------------------------------------------------------------------
# load — crash-recovery (missing chunk resubmission)
# ---------------------------------------------------------------------------

class TestLoadMissingChunkRecovery:
    def test_missing_payload_triggers_prescan_then_post(self, tmp_path):
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        # Pre-scan returns empty list → POST → 200
        client = _FakeClient([
            [],  # GET /jobs (pre-scan) — no match
            _FakeResponse(200, {"job_id": "new-job-1"}),  # POST /jobs
        ])
        result = load("r1", client, _config(), runs_dir=str(tmp_path))
        assert len(result) == 1
        assert result[0]["job_id"] == "new-job-1"

    def test_prescan_finds_existing_job_no_post(self, tmp_path):
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        # Pre-scan returns a match → no POST
        scan_hit = [{
            "job_id": "existing-job",
            "submit_time": "2026-01-01T00:00:00",
            "job_metadata": {"run_id": "r1", "chunk_index": 0, "retry_round": 0},
        }]
        client = _FakeClient([scan_hit])
        result = load("r1", client, _config(), runs_dir=str(tmp_path))
        assert result[0]["job_id"] == "existing-job"
        assert len(client.post_calls) == 0

    def test_recovered_job_added_to_manifest(self, tmp_path):
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        client = _FakeClient([
            [],
            _FakeResponse(200, {"job_id": "job-recovered"}),
        ])
        load("r1", client, _config(), runs_dir=str(tmp_path))
        m = store.load_manifest("r1", runs_dir=str(tmp_path))
        job_ids = [s["job_id"] for s in m["subjobs"]]
        assert "job-recovered" in job_ids

    def test_multiple_missing_payloads_all_resubmitted(self, tmp_path):
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        _write_payload(tmp_path, "r1", chunk_index=1, retry_round=0)
        client = _FakeClient([
            [],  # pre-scan for chunk 0
            _FakeResponse(200, {"job_id": "job-0"}),
            [],  # pre-scan for chunk 1
            _FakeResponse(200, {"job_id": "job-1"}),
        ])
        result = load("r1", client, _config(), runs_dir=str(tmp_path))
        assert len(result) == 2
        job_ids = {r["job_id"] for r in result}
        assert job_ids == {"job-0", "job-1"}

    def test_existing_terminal_subjob_with_same_chunk_not_resubmitted(self, tmp_path):
        _setup_run(tmp_path, "r1", subjobs=[
            _subjob("job-1", chunk_index=0, retry_round=0, terminal=True),
        ])
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        # No FTS3 calls should happen — subjob already in manifest
        client = _FakeClient()
        load("r1", client, _config(), runs_dir=str(tmp_path))
        assert len(client.get_calls) == 0
        assert len(client.post_calls) == 0

    def test_non_terminal_subjob_with_same_chunk_not_resubmitted(self, tmp_path):
        _setup_run(tmp_path, "r1", subjobs=[
            _subjob("job-1", chunk_index=0, terminal=False, status="ACTIVE"),
        ])
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        client = _FakeClient()
        load("r1", client, _config(), runs_dir=str(tmp_path))
        assert len(client.post_calls) == 0

    def test_resubmitted_subjob_not_terminal(self, tmp_path):
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        client = _FakeClient([
            [],
            _FakeResponse(200, {"job_id": "job-new"}),
        ])
        result = load("r1", client, _config(), runs_dir=str(tmp_path))
        assert result[0]["terminal"] is False


# ---------------------------------------------------------------------------
# load — 500 recovery on resubmission
# ---------------------------------------------------------------------------

class TestLoadResubmission500:
    def test_500_followed_by_scan_match_recovers(self, tmp_path, monkeypatch):
        import fts_framework.resume.controller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        scan_hit = [{
            "job_id": "job-after-500",
            "submit_time": "2026-01-01T00:00:00",
            "job_metadata": {"run_id": "r1", "chunk_index": 0, "retry_round": 0},
        }]
        client = _FakeClient([
            [],                          # pre-scan: no match
            _FakeResponse(500),          # POST: 500
            scan_hit,                    # post-500 scan: found
        ])
        result = load("r1", client, _config(), runs_dir=str(tmp_path))
        assert result[0]["job_id"] == "job-after-500"

    def test_500_with_no_scan_match_raises_submission_error(self, tmp_path, monkeypatch):
        import fts_framework.resume.controller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        client = _FakeClient([
            [],                 # pre-scan: no match
            _FakeResponse(500), # POST: 500
            [],                 # post-500 scan: still no match
        ])
        with pytest.raises(SubmissionError):
            load("r1", client, _config(), runs_dir=str(tmp_path))

    def test_unexpected_4xx_raises_submission_error(self, tmp_path):
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        client = _FakeClient([
            [],                  # pre-scan: no match
            _FakeResponse(403),  # POST: 403 Forbidden
        ])
        with pytest.raises(SubmissionError):
            load("r1", client, _config(), runs_dir=str(tmp_path))

    def test_token_expired_on_post_500_scan_propagates(self, tmp_path, monkeypatch):
        """TokenExpiredError from the post-500 scan must not be swallowed."""
        import fts_framework.resume.controller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        client = _FakeClient([
            [],                        # pre-scan: no match
            _FakeResponse(500),        # POST: 500
            TokenExpiredError(),       # post-500 scan: token expired
        ])
        with pytest.raises(TokenExpiredError):
            load("r1", client, _config(), runs_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# load — TokenExpiredError propagation
# ---------------------------------------------------------------------------

class TestLoadTokenExpiry:
    def test_token_expired_on_prescan_propagates(self, tmp_path):
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        client = _FakeClient([TokenExpiredError()])
        with pytest.raises(TokenExpiredError):
            load("r1", client, _config(), runs_dir=str(tmp_path))

    def test_token_expired_on_post_propagates(self, tmp_path):
        _setup_run(tmp_path, "r1")
        _write_payload(tmp_path, "r1", chunk_index=0, retry_round=0)
        client = _FakeClient([
            [],              # pre-scan: no match
            TokenExpiredError(),  # POST: token expired
        ])
        with pytest.raises(TokenExpiredError):
            load("r1", client, _config(), runs_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# _scan_for_job
# ---------------------------------------------------------------------------

class TestScanForJob:
    def test_returns_none_when_no_match(self):
        client = _FakeClient([
            [{
                "job_id": "other-job",
                "job_metadata": {"run_id": "other-run", "chunk_index": 0, "retry_round": 0},
            }]
        ])
        result = _scan_for_job(client, "my-run", 0, 0, 300)
        assert result is None

    def test_returns_job_id_on_match(self):
        client = _FakeClient([
            [{
                "job_id": "found-job",
                "submit_time": "2026-01-01T00:00:00",
                "job_metadata": {"run_id": "r1", "chunk_index": 0, "retry_round": 0},
            }]
        ])
        result = _scan_for_job(client, "r1", 0, 0, 300)
        assert result == "found-job"

    def test_multiple_matches_returns_most_recent(self):
        jobs = [
            {
                "job_id": "old-job",
                "submit_time": "2026-01-01T00:00:01",
                "job_metadata": {"run_id": "r1", "chunk_index": 0, "retry_round": 0},
            },
            {
                "job_id": "new-job",
                "submit_time": "2026-01-01T00:00:10",
                "job_metadata": {"run_id": "r1", "chunk_index": 0, "retry_round": 0},
            },
        ]
        client = _FakeClient([jobs])
        result = _scan_for_job(client, "r1", 0, 0, 300)
        assert result == "new-job"

    def test_non_list_response_returns_none(self):
        client = _FakeClient([{"error": "unexpected"}])
        result = _scan_for_job(client, "r1", 0, 0, 300)
        assert result is None

    def test_network_error_returns_none(self):
        import requests
        client = _FakeClient([requests.ConnectionError("timeout")])
        result = _scan_for_job(client, "r1", 0, 0, 300)
        assert result is None

    def test_token_expired_propagates(self):
        client = _FakeClient([TokenExpiredError()])
        with pytest.raises(TokenExpiredError):
            _scan_for_job(client, "r1", 0, 0, 300)

    def test_scan_window_h_at_least_one(self):
        """scan_window_s=0 must still result in a positive time_window."""
        client = _FakeClient([[]])
        _scan_for_job(client, "r1", 0, 0, scan_window_s=0)
        assert "time_window=1" in client.get_calls[0]

    def test_chunk_index_mismatch_not_returned(self):
        client = _FakeClient([
            [{
                "job_id": "wrong-chunk",
                "job_metadata": {"run_id": "r1", "chunk_index": 5, "retry_round": 0},
            }]
        ])
        result = _scan_for_job(client, "r1", 0, 0, 300)
        assert result is None

    def test_retry_round_mismatch_not_returned(self):
        client = _FakeClient([
            [{
                "job_id": "wrong-round",
                "job_metadata": {"run_id": "r1", "chunk_index": 0, "retry_round": 2},
            }]
        ])
        result = _scan_for_job(client, "r1", 0, 1, 300)
        assert result is None

    def test_non_numeric_chunk_index_in_metadata_skipped(self):
        """Jobs with non-numeric chunk_index must be skipped, not crash."""
        jobs = [
            {
                "job_id": "bad-meta",
                "job_metadata": {"run_id": "r1", "chunk_index": "abc", "retry_round": 0},
            },
            {
                "job_id": "good-job",
                "submit_time": "2026-01-01T00:00:00",
                "job_metadata": {"run_id": "r1", "chunk_index": 0, "retry_round": 0},
            },
        ]
        client = _FakeClient([jobs])
        result = _scan_for_job(client, "r1", 0, 0, 300)
        assert result == "good-job"
