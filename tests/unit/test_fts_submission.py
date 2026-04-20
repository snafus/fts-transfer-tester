"""
Unit tests for fts_framework.fts.submission.

FTSClient is replaced with a lightweight test double that records calls and
returns pre-configured responses, avoiding the need for ``responses`` mocking
at the HTTP layer (submission logic is what's under test, not HTTP mechanics).
"""

import pytest
from collections import OrderedDict

from fts_framework.fts.submission import (
    chunk,
    build_payload,
    _build_job_metadata,
    submit_with_500_recovery,
)
from fts_framework.exceptions import SubmissionError


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class _FakeResponse(object):
    """Minimal requests.Response stand-in."""
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body or {}
        self.text = str(body)

    def json(self):
        return self._body


class _FakeClient(object):
    """Captures post/get calls; returns pre-configured responses."""

    def __init__(self, post_responses=None, get_responses=None):
        # post_responses: list of _FakeResponse, consumed in order
        self._post_responses = list(post_responses or [])
        # get_responses: list of values (parsed JSON), consumed in order
        self._get_responses = list(get_responses or [])
        self.post_calls = []   # type: list — recorded (path, payload) pairs
        self.get_calls = []    # type: list — recorded (path, kwargs) pairs

    def post(self, path, payload, **kwargs):
        self.post_calls.append((path, payload))
        if not self._post_responses:
            raise AssertionError("Unexpected post() call")
        return self._post_responses.pop(0)

    def get(self, path, **kwargs):
        self.get_calls.append((path, kwargs))
        if not self._get_responses:
            raise AssertionError("Unexpected get() call")
        return self._get_responses.pop(0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mapping(*keys):
    """Build an OrderedDict with integer values for simple tests."""
    return OrderedDict((k, "dst://{}".format(k)) for k in keys)


def _config(chunk_size=200, scan_window_s=300, fts_retry_max=2,
            priority=3, activity=None, job_metadata=None,
            verify_checksum="both", overwrite=False, unmanaged_tokens=False):
    return {
        "run": {"test_label": "campaign_test"},
        "fts": {
            "endpoint": "https://fts3.example.org:8446",
        },
        "tokens": {
            "fts_submit": "tok_submit",
            "source_read": "tok_source",
            "dest_write": "tok_dest",
        },
        "transfer": {
            "chunk_size": chunk_size,
            "checksum_algorithm": "adler32",
            "verify_checksum": verify_checksum,
            "overwrite": overwrite,
            "priority": priority,
            "activity": activity,
            "job_metadata": job_metadata or {},
            "unmanaged_tokens": unmanaged_tokens,
        },
        "submission": {
            "scan_window_s": scan_window_s,
        },
        "retry": {
            "fts_retry_max": fts_retry_max,
        },
    }


RUN_ID = "20260101_abcd1234"


# ---------------------------------------------------------------------------
# chunk()
# ---------------------------------------------------------------------------

class TestChunk:
    def test_single_chunk_when_items_le_size(self):
        m = _mapping("a", "b", "c")
        result = chunk(m, size=10)
        assert len(result) == 1
        assert list(result[0].keys()) == ["a", "b", "c"]

    def test_exact_multiple(self):
        m = _mapping(*[str(i) for i in range(6)])
        result = chunk(m, size=3)
        assert len(result) == 2
        assert len(result[0]) == 3
        assert len(result[1]) == 3

    def test_last_chunk_smaller(self):
        m = _mapping(*[str(i) for i in range(5)])
        result = chunk(m, size=3)
        assert len(result) == 2
        assert len(result[1]) == 2

    def test_order_preserved(self):
        keys = ["c", "a", "b"]
        m = _mapping(*keys)
        result = chunk(m, size=10)
        assert list(result[0].keys()) == keys

    def test_size_one(self):
        m = _mapping("x", "y", "z")
        result = chunk(m, size=1)
        assert len(result) == 3
        for c in result:
            assert len(c) == 1

    def test_size_zero_raises(self):
        with pytest.raises(ValueError, match="chunk size"):
            chunk(_mapping("a"), size=0)

    def test_empty_items_raises(self):
        with pytest.raises(ValueError, match="empty"):
            chunk(OrderedDict(), size=10)

    def test_returns_list_of_ordered_dicts(self):
        result = chunk(_mapping("a", "b"), size=10)
        assert isinstance(result, list)
        assert isinstance(result[0], OrderedDict)


# ---------------------------------------------------------------------------
# _build_job_metadata()
# ---------------------------------------------------------------------------

class TestBuildJobMetadata:
    def test_framework_keys_present(self):
        meta = _build_job_metadata(_config(), RUN_ID, 0, 0)
        assert meta["run_id"] == RUN_ID
        assert meta["chunk_index"] == 0
        assert meta["retry_round"] == 0
        assert meta["test_label"] == "campaign_test"

    def test_user_metadata_merged(self):
        cfg = _config(job_metadata={"operator": "alice", "campaign": "perf"})
        meta = _build_job_metadata(cfg, RUN_ID, 0, 0)
        assert meta["operator"] == "alice"
        assert meta["campaign"] == "perf"

    def test_framework_keys_override_user_keys(self):
        cfg = _config(job_metadata={"run_id": "user-override", "chunk_index": 999})
        meta = _build_job_metadata(cfg, RUN_ID, 2, 1)
        assert meta["run_id"] == RUN_ID
        assert meta["chunk_index"] == 2
        assert meta["retry_round"] == 1

    def test_no_user_metadata(self):
        meta = _build_job_metadata(_config(job_metadata={}), RUN_ID, 0, 0)
        assert set(meta.keys()) == {"run_id", "chunk_index", "retry_round", "test_label"}

    def test_chunk_index_and_retry_round_correct(self):
        meta = _build_job_metadata(_config(), RUN_ID, 7, 3)
        assert meta["chunk_index"] == 7
        assert meta["retry_round"] == 3


# ---------------------------------------------------------------------------
# build_payload()
# ---------------------------------------------------------------------------

class TestBuildPayload:
    def _make_chunk(self, srcs):
        return OrderedDict(
            (s, "https://dst.example.org/data/testfile_{:06d}".format(i))
            for i, s in enumerate(srcs)
        )

    def test_files_list_length(self):
        srcs = ["https://src.example.org/f{}.dat".format(i) for i in range(3)]
        mapping = self._make_chunk(srcs)
        checksums = {s: "adler32:a1b2c3d4" for s in srcs}
        payload = build_payload(mapping, checksums, _config(), RUN_ID, 0, 0)
        assert len(payload["files"]) == 3

    def test_sources_and_destinations(self):
        src = "https://src.example.org/file.dat"
        dst = "https://dst.example.org/data/testfile_000000"
        mapping = OrderedDict([(src, dst)])
        checksums = {src: "adler32:a1b2c3d4"}
        payload = build_payload(mapping, checksums, _config(), RUN_ID, 0, 0)
        entry = payload["files"][0]
        assert entry["sources"] == [src]
        assert entry["destinations"] == [dst]

    def test_checksum_included(self):
        src = "https://src.example.org/file.dat"
        mapping = OrderedDict([(src, "https://dst.example.org/testfile_000000")])
        checksums = {src: "adler32:a1b2c3d4"}
        payload = build_payload(mapping, checksums, _config(), RUN_ID, 0, 0)
        assert payload["files"][0]["checksum"] == "adler32:a1b2c3d4"

    def test_checksum_missing_pfn_omitted(self):
        src = "https://src.example.org/file.dat"
        mapping = OrderedDict([(src, "https://dst.example.org/testfile_000000")])
        payload = build_payload(mapping, {}, _config(), RUN_ID, 0, 0)
        assert "checksum" not in payload["files"][0]

    def test_priority_in_params(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(priority=5), RUN_ID, 0, 0)
        assert payload["params"]["priority"] == 5

    def test_activity_included_when_set(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(activity="benchmark"), RUN_ID, 0, 0)
        assert payload["params"]["activity"] == "benchmark"

    def test_activity_absent_when_not_set(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(activity=None), RUN_ID, 0, 0)
        assert "activity" not in payload["params"]

    def test_overwrite_included_when_true(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(overwrite=True), RUN_ID, 0, 0)
        assert payload["params"]["overwrite"] is True

    def test_overwrite_absent_when_false(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(overwrite=False), RUN_ID, 0, 0)
        assert "overwrite" not in payload["params"]

    def test_retry_count_from_config(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(fts_retry_max=4), RUN_ID, 0, 0)
        assert payload["params"]["retry"] == 4

    def test_verify_checksum_from_config(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(verify_checksum="source"), RUN_ID, 0, 0)
        assert payload["params"]["verify_checksum"] == "source"

    def test_job_metadata_embedded(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(), RUN_ID, 3, 1)
        meta = payload["params"]["job_metadata"]
        assert meta["run_id"] == RUN_ID
        assert meta["chunk_index"] == 3
        assert meta["retry_round"] == 1

    def test_unmanaged_tokens_absent_by_default(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(), RUN_ID, 0, 0)
        assert "source_token" not in payload["params"]
        assert "destination_token" not in payload["params"]

    def test_unmanaged_tokens_included_when_enabled(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(unmanaged_tokens=True), RUN_ID, 0, 0)
        assert payload["params"]["source_token"] == "tok_source"
        assert payload["params"]["destination_token"] == "tok_dest"
        assert payload["params"]["unmanaged_tokens"] is True

    def test_unmanaged_tokens_absent_when_disabled(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        payload = build_payload(mapping, {}, _config(), RUN_ID, 0, 0)
        assert "unmanaged_tokens" not in payload["params"]

    def test_unmanaged_tokens_uses_correct_roles(self):
        mapping = self._make_chunk(["https://src.example.org/f.dat"])
        cfg = _config(unmanaged_tokens=True)
        cfg["tokens"]["source_read"] = "source_role_tok"
        cfg["tokens"]["dest_write"] = "dest_role_tok"
        payload = build_payload(mapping, {}, cfg, RUN_ID, 0, 0)
        assert payload["params"]["source_token"] == "source_role_tok"
        assert payload["params"]["destination_token"] == "dest_role_tok"


# ---------------------------------------------------------------------------
# submit_with_500_recovery()
# ---------------------------------------------------------------------------

class TestSubmitWith500Recovery:
    def test_200_returns_job_id(self):
        client = _FakeClient(
            post_responses=[_FakeResponse(200, {"job_id": "job-abc"})]
        )
        job_id = submit_with_500_recovery(client, {}, _config(), RUN_ID, 0, 0)
        assert job_id == "job-abc"

    def test_non_200_non_500_raises_submission_error(self):
        client = _FakeClient(
            post_responses=[_FakeResponse(400, "bad request")]
        )
        with pytest.raises(SubmissionError) as exc_info:
            submit_with_500_recovery(client, {}, _config(), RUN_ID, 0, 0)
        assert exc_info.value.chunk_index == 0
        assert exc_info.value.status_code == 400

    def test_500_with_matching_job_recovers(self, monkeypatch):
        import fts_framework.fts.submission as sub_mod
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: None)
        recovered_job = {
            "job_id": "job-recovered",
            "submit_time": "2026-01-01T00:00:00",
            "job_metadata": {
                "run_id": RUN_ID,
                "chunk_index": 0,
                "retry_round": 0,
            },
        }
        client = _FakeClient(
            post_responses=[_FakeResponse(500, "internal error")],
            get_responses=[[recovered_job]],
        )
        job_id = submit_with_500_recovery(client, {}, _config(), RUN_ID, 0, 0)
        assert job_id == "job-recovered"

    def test_500_no_match_raises_submission_error(self, monkeypatch):
        import fts_framework.fts.submission as sub_mod
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: None)
        client = _FakeClient(
            post_responses=[_FakeResponse(500, "internal error")],
            get_responses=[[]],  # empty scan
        )
        with pytest.raises(SubmissionError) as exc_info:
            submit_with_500_recovery(client, {}, _config(), RUN_ID, 2, 0)
        assert exc_info.value.chunk_index == 2
        assert exc_info.value.status_code == 500
        assert "scan window" in str(exc_info.value)

    def test_500_multiple_matches_uses_most_recent(self, monkeypatch):
        import fts_framework.fts.submission as sub_mod
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: None)
        jobs = [
            {
                "job_id": "job-old",
                "submit_time": "2026-01-01T00:00:00",
                "job_metadata": {"run_id": RUN_ID, "chunk_index": 0, "retry_round": 0},
            },
            {
                "job_id": "job-new",
                "submit_time": "2026-01-01T01:00:00",
                "job_metadata": {"run_id": RUN_ID, "chunk_index": 0, "retry_round": 0},
            },
        ]
        client = _FakeClient(
            post_responses=[_FakeResponse(500, "internal error")],
            get_responses=[jobs],
        )
        job_id = submit_with_500_recovery(client, {}, _config(), RUN_ID, 0, 0)
        assert job_id == "job-new"

    def test_500_scan_uses_correct_time_window(self, monkeypatch):
        """scan_window_s=600 must produce time_window=1 (max(1, 600//3600+1)=1)."""
        import fts_framework.fts.submission as sub_mod
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: None)
        client = _FakeClient(
            post_responses=[_FakeResponse(500)],
            get_responses=[[]],
        )
        with pytest.raises(SubmissionError):
            submit_with_500_recovery(
                client, {}, _config(scan_window_s=600), RUN_ID, 0, 0
            )
        path, _ = client.get_calls[0]
        assert "time_window=1" in path

    def test_500_scan_state_in_parameter(self, monkeypatch):
        """state_in must use literal commas (not %2C) and include CANCELED."""
        import fts_framework.fts.submission as sub_mod
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: None)
        client = _FakeClient(
            post_responses=[_FakeResponse(500)],
            get_responses=[[]],
        )
        with pytest.raises(SubmissionError):
            submit_with_500_recovery(client, {}, _config(), RUN_ID, 0, 0)
        path, _ = client.get_calls[0]
        assert "state_in=" in path
        assert "%2C" not in path  # must not be URL-encoded
        expected_states = {"SUBMITTED", "READY", "ACTIVE", "FINISHED",
                           "FAILED", "FINISHEDDIRTY", "CANCELED"}
        state_in_val = path.split("state_in=")[1].split("&")[0]
        assert set(state_in_val.split(",")) == expected_states

    def test_500_scan_filters_by_run_id(self, monkeypatch):
        """Jobs from a different run_id must not match."""
        import fts_framework.fts.submission as sub_mod
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: None)
        other_run_job = {
            "job_id": "job-other",
            "submit_time": "2026-01-01T00:00:00",
            "job_metadata": {"run_id": "other-run", "chunk_index": 0, "retry_round": 0},
        }
        client = _FakeClient(
            post_responses=[_FakeResponse(500)],
            get_responses=[[other_run_job]],
        )
        with pytest.raises(SubmissionError):
            submit_with_500_recovery(client, {}, _config(), RUN_ID, 0, 0)

    def test_500_scan_filters_by_chunk_index(self, monkeypatch):
        """Jobs from a different chunk must not match."""
        import fts_framework.fts.submission as sub_mod
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: None)
        wrong_chunk_job = {
            "job_id": "job-wrong-chunk",
            "submit_time": "2026-01-01T00:00:00",
            "job_metadata": {"run_id": RUN_ID, "chunk_index": 99, "retry_round": 0},
        }
        client = _FakeClient(
            post_responses=[_FakeResponse(500)],
            get_responses=[[wrong_chunk_job]],
        )
        with pytest.raises(SubmissionError):
            submit_with_500_recovery(client, {}, _config(), RUN_ID, 0, 0)

    def test_500_scan_filters_by_retry_round(self, monkeypatch):
        """Jobs from a different retry_round must not match."""
        import fts_framework.fts.submission as sub_mod
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: None)
        wrong_round_job = {
            "job_id": "job-wrong-round",
            "submit_time": "2026-01-01T00:00:00",
            "job_metadata": {"run_id": RUN_ID, "chunk_index": 0, "retry_round": 1},
        }
        client = _FakeClient(
            post_responses=[_FakeResponse(500)],
            get_responses=[[wrong_round_job]],
        )
        with pytest.raises(SubmissionError):
            submit_with_500_recovery(client, {}, _config(), RUN_ID, 0, 0)


