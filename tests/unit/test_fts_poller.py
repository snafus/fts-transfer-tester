"""
Unit tests for fts_framework.fts.poller.

FTSClient is replaced with a lightweight fake that returns pre-configured
responses in order, so no real HTTP calls are made.
"""

import pytest
import requests

from fts_framework.fts.poller import poll_to_completion, TERMINAL_STATES
from fts_framework.exceptions import PollingTimeoutError, TokenExpiredError


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class _FakeClient(object):
    """Returns pre-configured job_state responses in order."""

    def __init__(self, responses):
        # responses: list of dicts (job data) or exceptions to raise
        self._responses = list(responses)
        self.get_calls = []  # list of paths called

    def get(self, path, **kwargs):
        self.get_calls.append(path)
        if not self._responses:
            raise AssertionError("Unexpected get() call for {}".format(path))
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(initial_interval_s=0, backoff_multiplier=1.0,
            max_interval_s=0, campaign_timeout_s=3600):
    return {
        "polling": {
            "initial_interval_s": initial_interval_s,
            "backoff_multiplier": backoff_multiplier,
            "max_interval_s": max_interval_s,
            "campaign_timeout_s": campaign_timeout_s,
        }
    }


def _subjob(job_id, chunk_index=0, retry_round=0, terminal=False, status=""):
    return {
        "job_id": job_id,
        "chunk_index": chunk_index,
        "retry_round": retry_round,
        "terminal": terminal,
        "status": status,
    }


# ---------------------------------------------------------------------------
# TERMINAL_STATES constant
# ---------------------------------------------------------------------------

class TestTerminalStates:
    def test_finished_is_terminal(self):
        assert "FINISHED" in TERMINAL_STATES

    def test_failed_is_terminal(self):
        assert "FAILED" in TERMINAL_STATES

    def test_finisheddirty_is_terminal(self):
        assert "FINISHEDDIRTY" in TERMINAL_STATES

    def test_canceled_is_terminal(self):
        assert "CANCELED" in TERMINAL_STATES

    def test_submitted_not_terminal(self):
        assert "SUBMITTED" not in TERMINAL_STATES

    def test_active_not_terminal(self):
        assert "ACTIVE" not in TERMINAL_STATES

    def test_staging_not_terminal(self):
        assert "STAGING" not in TERMINAL_STATES


# ---------------------------------------------------------------------------
# poll_to_completion — single job paths
# ---------------------------------------------------------------------------

class TestPollToCompletion:
    def test_single_job_finishes_immediately(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([{"job_state": "FINISHED"}])
        subjobs = [_subjob("job-1")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "FINISHED"
        assert result[0]["terminal"] is True

    def test_single_job_fails(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([{"job_state": "FAILED"}])
        subjobs = [_subjob("job-1")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "FAILED"
        assert result[0]["terminal"] is True

    def test_finisheddirty_is_terminal(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([{"job_state": "FINISHEDDIRTY"}])
        subjobs = [_subjob("job-1")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "FINISHEDDIRTY"
        assert result[0]["terminal"] is True

    def test_canceled_is_terminal(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([{"job_state": "CANCELED"}])
        subjobs = [_subjob("job-1")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "CANCELED"
        assert result[0]["terminal"] is True

    def test_job_transitions_through_active_to_finished(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        # First poll: ACTIVE, second poll: FINISHED
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "FINISHED"},
        ])
        subjobs = [_subjob("job-1")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "FINISHED"
        assert len(client.get_calls) == 2

    def test_staging_marked_unsupported(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([{"job_state": "STAGING"}])
        subjobs = [_subjob("job-1")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "STAGING_UNSUPPORTED"
        assert result[0]["terminal"] is True

    def test_already_terminal_job_skipped(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([])
        subjobs = [_subjob("job-1", terminal=True, status="FINISHED")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "FINISHED"
        assert len(client.get_calls) == 0

    def test_all_already_terminal_returns_immediately(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([])
        subjobs = [
            _subjob("job-1", terminal=True, status="FINISHED"),
            _subjob("job-2", terminal=True, status="FAILED"),
        ]
        result = poll_to_completion(subjobs, client, _config())
        assert len(client.get_calls) == 0
        assert len(result) == 2

    def test_empty_subjob_list_returns_empty(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([])
        result = poll_to_completion([], client, _config())
        assert result == []
        assert len(client.get_calls) == 0


# ---------------------------------------------------------------------------
# poll_to_completion — multiple jobs
# ---------------------------------------------------------------------------

class TestPollMultipleJobs:
    def test_two_jobs_both_finish_first_round(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        # Order: job-1 polled, then job-2 (dict iteration order in Python 3.6
        # is insertion order since we use a dict built from the subjob list)
        client = _FakeClient([
            {"job_state": "FINISHED"},   # job-1
            {"job_state": "FINISHED"},   # job-2
        ])
        subjobs = [_subjob("job-1"), _subjob("job-2")]
        result = poll_to_completion(subjobs, client, _config())
        assert all(s["status"] == "FINISHED" for s in result)
        assert all(s["terminal"] for s in result)

    def test_one_job_finishes_before_other(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        # Round 1: job-1 FINISHED, job-2 ACTIVE
        # Round 2: job-2 FAILED
        client = _FakeClient([
            {"job_state": "FINISHED"},  # job-1 round 1
            {"job_state": "ACTIVE"},    # job-2 round 1
            {"job_state": "FAILED"},    # job-2 round 2
        ])
        subjobs = [_subjob("job-1"), _subjob("job-2")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "FINISHED"
        assert result[1]["status"] == "FAILED"
        assert len(client.get_calls) == 3

    def test_mix_of_terminal_and_active_on_input(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([{"job_state": "FINISHED"}])  # only job-2 polled
        subjobs = [
            _subjob("job-1", terminal=True, status="FINISHED"),
            _subjob("job-2"),
        ]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "FINISHED"
        assert result[1]["status"] == "FINISHED"
        assert len(client.get_calls) == 1


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------

class TestPollingTimeout:
    def test_timeout_raises_with_active_job_ids(self, monkeypatch):
        import fts_framework.fts.poller as mod
        import time as time_mod

        # Make time.time() advance past the deadline after the first sleep
        call_count = [0]
        real_time = time_mod.time()

        def fake_time():
            call_count[0] += 1
            # First call (deadline calc): real time; subsequent: deadline + 1
            if call_count[0] <= 1:
                return real_time
            return real_time + 10  # past deadline=real_time+5

        monkeypatch.setattr(mod.time, "time", fake_time)
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)

        client = _FakeClient([])  # never gets called
        subjobs = [_subjob("job-stuck")]
        with pytest.raises(PollingTimeoutError) as exc_info:
            poll_to_completion(subjobs, client, _config(campaign_timeout_s=5))
        assert "job-stuck" in exc_info.value.active_job_ids

    def test_timeout_error_contains_all_active_jobs(self, monkeypatch):
        import fts_framework.fts.poller as mod
        import time as time_mod

        call_count = [0]
        real_time = time_mod.time()

        def fake_time():
            call_count[0] += 1
            return real_time if call_count[0] <= 1 else real_time + 100

        monkeypatch.setattr(mod.time, "time", fake_time)
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)

        client = _FakeClient([])
        subjobs = [_subjob("job-a"), _subjob("job-b"), _subjob("job-c")]
        with pytest.raises(PollingTimeoutError) as exc_info:
            poll_to_completion(subjobs, client, _config(campaign_timeout_s=5))
        assert set(exc_info.value.active_job_ids) == {"job-a", "job-b", "job-c"}


# ---------------------------------------------------------------------------
# TokenExpiredError propagation
# ---------------------------------------------------------------------------

class TestTokenExpiry:
    def test_token_expired_propagates(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([TokenExpiredError("job-1")])
        subjobs = [_subjob("job-1")]
        with pytest.raises(TokenExpiredError):
            poll_to_completion(subjobs, client, _config())


# ---------------------------------------------------------------------------
# Backoff behaviour
# ---------------------------------------------------------------------------

class TestBackoff:
    def test_interval_increases_each_round(self, monkeypatch):
        import fts_framework.fts.poller as mod
        sleep_calls = []
        monkeypatch.setattr(mod.time, "sleep", lambda s: sleep_calls.append(s))
        # Three rounds: ACTIVE, ACTIVE, FINISHED
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            {"job_state": "FINISHED"},
        ])
        subjobs = [_subjob("job-1")]
        poll_to_completion(subjobs, client, _config(
            initial_interval_s=10,
            backoff_multiplier=2.0,
            max_interval_s=60,
        ))
        assert len(sleep_calls) == 3
        assert sleep_calls[0] == 10.0
        assert sleep_calls[1] == 20.0
        assert sleep_calls[2] == 40.0

    def test_interval_capped_at_max(self, monkeypatch):
        import fts_framework.fts.poller as mod
        sleep_calls = []
        monkeypatch.setattr(mod.time, "sleep", lambda s: sleep_calls.append(s))
        # Many rounds — interval must not exceed max_interval_s=15
        responses = [{"job_state": "ACTIVE"}] * 5 + [{"job_state": "FINISHED"}]
        client = _FakeClient(responses)
        subjobs = [_subjob("job-1")]
        poll_to_completion(subjobs, client, _config(
            initial_interval_s=10,
            backoff_multiplier=2.0,
            max_interval_s=15,
        ))
        assert all(s <= 15.0 for s in sleep_calls)

    def test_poll_url_correct(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([{"job_state": "FINISHED"}])
        poll_to_completion([_subjob("job-xyz-123")], client, _config())
        assert client.get_calls[0] == "/jobs/job-xyz-123"

    def test_transient_connection_error_retried(self, monkeypatch):
        """A ConnectionError on one round must not terminate the loop."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        # Round 1: ConnectionError (swallowed), Round 2: FINISHED
        client = _FakeClient([
            requests.ConnectionError("timeout"),
            {"job_state": "FINISHED"},
        ])
        subjobs = [_subjob("job-1")]
        result = poll_to_completion(subjobs, client, _config())
        assert result[0]["status"] == "FINISHED"
        assert len(client.get_calls) == 2

    def test_http_error_propagates(self, monkeypatch):
        """Permanent HTTP errors (non-transient, no response) must propagate."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([requests.HTTPError("403 Forbidden")])
        with pytest.raises(requests.HTTPError):
            poll_to_completion([_subjob("job-1")], client, _config())

    def test_transient_http_error_retried(self, monkeypatch):
        """HTTPError with a transient status (502/503/504) must be swallowed and retried."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)

        # Build an HTTPError that carries a response with status 503
        fake_response = requests.models.Response()
        fake_response.status_code = 503
        transient_exc = requests.HTTPError("503 Service Unavailable",
                                           response=fake_response)

        client = _FakeClient([transient_exc, {"job_state": "FINISHED"}])
        result = poll_to_completion([_subjob("job-1")], client, _config())
        assert result[0]["status"] == "FINISHED"
        assert len(client.get_calls) == 2

    def test_transient_timeout_retried(self, monkeypatch):
        """requests.Timeout (W1/W2) must be swallowed and retried next round."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([
            requests.Timeout("connect timeout"),
            {"job_state": "FINISHED"},
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config())
        assert result[0]["status"] == "FINISHED"
        assert len(client.get_calls) == 2

    def test_zero_timeout_raises_before_first_poll(self, monkeypatch):
        """W5: campaign_timeout_s=0 means deadline is already past; raises immediately."""
        import fts_framework.fts.poller as mod
        import time as time_mod
        # Make time.time() always return a value past the deadline (deadline = t + 0)
        real_time = time_mod.time()
        call_count = [0]

        def fake_time():
            call_count[0] += 1
            # First call: real time (for deadline = real_time + 0)
            # All subsequent: real_time + 1 (past the deadline)
            if call_count[0] == 1:
                return real_time
            return real_time + 1

        monkeypatch.setattr(mod.time, "time", fake_time)
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([])
        with pytest.raises(PollingTimeoutError):
            poll_to_completion([_subjob("job-1")], client, _config(campaign_timeout_s=0))
        # No poll should have been issued
        assert len(client.get_calls) == 0

    def test_missing_job_state_key_retried(self, monkeypatch):
        """W6: response missing job_state is treated as non-terminal (warning logged)."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        # First response has no job_state; second is FINISHED
        client = _FakeClient([
            {"status": "200 OK"},
            {"job_state": "FINISHED"},
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config())
        assert result[0]["status"] == "FINISHED"
        assert len(client.get_calls) == 2

    def test_non_dict_response_skipped(self, monkeypatch):
        """W7: non-dict GET /jobs response is skipped with a warning."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([
            ["unexpected", "list"],
            {"job_state": "FINISHED"},
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config())
        assert result[0]["status"] == "FINISHED"
        assert len(client.get_calls) == 2


# ---------------------------------------------------------------------------
# Stuck-ACTIVE detection
# ---------------------------------------------------------------------------

def _config_with_stuck(stuck_active_check_rounds=2, **kwargs):
    """Config with stuck_active_check_rounds set (default 2 for fast tests)."""
    cfg = _config(**kwargs)
    cfg["polling"]["stuck_active_check_rounds"] = stuck_active_check_rounds
    return cfg


def _all_finished_files():
    return [
        {"file_state": "FINISHED"},
        {"file_state": "FINISHED"},
    ]


class TestStuckActive:
    def test_stuck_active_all_files_finished_derives_finished(self, monkeypatch):
        """After stuck_active_check_rounds non-terminal rounds, all-FINISHED files → FINISHED."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        # 2 ACTIVE rounds, then file check triggered, then derived FINISHED
        client = _FakeClient([
            {"job_state": "ACTIVE"},       # round 1
            {"job_state": "ACTIVE"},       # round 2 → triggers check
            _all_finished_files(),         # GET /jobs/job-1/files
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config_with_stuck(2))
        assert result[0]["status"] == "FINISHED"
        assert result[0]["terminal"] is True

    def test_stuck_active_all_files_failed_derives_failed(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            [{"file_state": "FAILED"}, {"file_state": "FAILED"}],
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config_with_stuck(2))
        assert result[0]["status"] == "FAILED"
        assert result[0]["terminal"] is True

    def test_stuck_active_mixed_files_derives_finisheddirty(self, monkeypatch):
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            [{"file_state": "FINISHED"}, {"file_state": "FAILED"}],
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config_with_stuck(2))
        assert result[0]["status"] == "FINISHEDDIRTY"
        assert result[0]["terminal"] is True

    def test_stuck_active_not_yet_terminal_files_does_not_resolve(self, monkeypatch):
        """If file check shows non-terminal files, polling continues normally."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            # file check: one file still ACTIVE
            [{"file_state": "ACTIVE"}, {"file_state": "FINISHED"}],
            {"job_state": "FINISHED"},   # job eventually finishes normally
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config_with_stuck(2))
        assert result[0]["status"] == "FINISHED"

    def test_stuck_active_disabled_when_zero(self, monkeypatch):
        """stuck_active_check_rounds=0 disables the feature entirely."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            {"job_state": "FINISHED"},
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config_with_stuck(0))
        assert result[0]["status"] == "FINISHED"
        # No /files call should have been made
        assert all("/files" not in p for p in client.get_calls)

    def test_stuck_active_not_triggered_before_threshold(self, monkeypatch):
        """File check is only done at multiples of stuck_active_check_rounds."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        # 3 rounds, threshold=5 — file check never triggered
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            {"job_state": "FINISHED"},
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config_with_stuck(5))
        assert result[0]["status"] == "FINISHED"
        assert all("/files" not in p for p in client.get_calls)

    def test_stuck_active_file_fetch_failure_retried(self, monkeypatch):
        """If GET /jobs/{id}/files raises, the check is skipped and polling continues."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            # file check fails
            requests.ConnectionError("timeout"),
            {"job_state": "FINISHED"},
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config_with_stuck(2))
        assert result[0]["status"] == "FINISHED"

    def test_stuck_active_not_used_files_only_derives_finished(self, monkeypatch):
        """Files all NOT_USED (no meaningful transfers) → FINISHED."""
        import fts_framework.fts.poller as mod
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        client = _FakeClient([
            {"job_state": "ACTIVE"},
            {"job_state": "ACTIVE"},
            [{"file_state": "NOT_USED"}, {"file_state": "NOT_USED"}],
        ])
        result = poll_to_completion([_subjob("job-1")], client, _config_with_stuck(2))
        assert result[0]["status"] == "FINISHED"
        assert result[0]["terminal"] is True
