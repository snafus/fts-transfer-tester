"""
Unit tests for fts_framework.fts.collector.

FTSClient is replaced with a lightweight fake.
"""

import pytest

from fts_framework.fts.collector import (
    harvest_all,
    _harvest_files,
    _harvest_retries,
    _harvest_dm,
    _normalise_file_record,
)
from fts_framework.exceptions import TokenExpiredError


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class _FakeClient(object):
    """Returns pre-configured responses by path pattern in order."""

    def __init__(self, responses):
        # responses: list of (path_suffix, value) pairs consumed in order,
        # or a dict keyed by path for deterministic lookup.
        # Accept both; for simplicity use an ordered list.
        self._responses = list(responses)
        self.get_calls = []

    def get(self, path, **kwargs):
        self.get_calls.append(path)
        if not self._responses:
            raise AssertionError("Unexpected get() for {}".format(path))
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _subjob(job_id, chunk_index=0, retry_round=0, terminal=True, status="FINISHED"):
    return {
        "job_id": job_id,
        "chunk_index": chunk_index,
        "retry_round": retry_round,
        "terminal": terminal,
        "status": status,
    }


def _fts_file(file_id=1, file_state="FINISHED", filesize=1024, throughput=500.0,
              tx_duration=2.0, source_surl="https://src/f", dest_surl="https://dst/f",
              reason="", start_time="2026-01-01T00:00:00", finish_time="2026-01-01T00:00:02",
              checksum="adler32:a1b2c3d4"):
    return {
        "file_id": file_id,
        "file_state": file_state,
        "filesize": filesize,
        "throughput": throughput,
        "tx_duration": tx_duration,
        "source_surl": source_surl,
        "dest_surl": dest_surl,
        "reason": reason,
        "start_time": start_time,
        "finish_time": finish_time,
        "checksum": checksum,
        "job_metadata": {},
        "file_metadata": {},
    }


# ---------------------------------------------------------------------------
# _normalise_file_record
# ---------------------------------------------------------------------------

class TestNormaliseFileRecord:
    def test_identity_fields_set(self):
        item = _fts_file(file_id=42)
        rec = _normalise_file_record(item, "job-abc", 2, 1)
        assert rec["job_id"] == "job-abc"
        assert rec["file_id"] == 42
        assert rec["chunk_index"] == 2
        assert rec["retry_round"] == 1

    def test_transfer_addresses_preserved(self):
        item = _fts_file(source_surl="https://src/x", dest_surl="https://dst/y")
        rec = _normalise_file_record(item, "j", 0, 0)
        assert rec["source_surl"] == "https://src/x"
        assert rec["dest_surl"] == "https://dst/y"

    def test_file_state_preserved(self):
        item = _fts_file(file_state="FAILED")
        rec = _normalise_file_record(item, "j", 0, 0)
        assert rec["file_state"] == "FAILED"

    def test_reason_preserved(self):
        item = _fts_file(reason="checksum mismatch")
        rec = _normalise_file_record(item, "j", 0, 0)
        assert rec["reason"] == "checksum mismatch"

    def test_reason_none_becomes_empty_string(self):
        item = _fts_file()
        item["reason"] = None
        rec = _normalise_file_record(item, "j", 0, 0)
        assert rec["reason"] == ""

    def test_filesize_cast_to_int(self):
        item = _fts_file(filesize=4096)
        rec = _normalise_file_record(item, "j", 0, 0)
        assert isinstance(rec["filesize"], int)
        assert rec["filesize"] == 4096

    def test_throughput_cast_to_float(self):
        item = _fts_file(throughput=1234)
        rec = _normalise_file_record(item, "j", 0, 0)
        assert isinstance(rec["throughput"], float)
        assert rec["throughput"] == 1234.0

    def test_tx_duration_cast_to_float(self):
        item = _fts_file(tx_duration=5)
        rec = _normalise_file_record(item, "j", 0, 0)
        assert isinstance(rec["tx_duration"], float)

    def test_missing_filesize_defaults_to_zero(self):
        item = _fts_file()
        del item["filesize"]
        rec = _normalise_file_record(item, "j", 0, 0)
        assert rec["filesize"] == 0

    def test_none_throughput_defaults_to_zero(self):
        item = _fts_file()
        item["throughput"] = None
        rec = _normalise_file_record(item, "j", 0, 0)
        assert rec["throughput"] == 0.0

    def test_computed_metrics_zeroed(self):
        rec = _normalise_file_record(_fts_file(), "j", 0, 0)
        assert rec["throughput_wire"] == 0.0
        assert rec["throughput_wall"] == 0.0
        assert rec["wall_duration_s"] == 0.0

    def test_staging_fields_reserved_none(self):
        rec = _normalise_file_record(_fts_file(), "j", 0, 0)
        assert rec["staging_start"] is None
        assert rec["staging_finished"] is None

    def test_checksum_preserved(self):
        item = _fts_file(checksum="adler32:deadbeef")
        rec = _normalise_file_record(item, "j", 0, 0)
        assert rec["checksum"] == "adler32:deadbeef"

    def test_missing_fields_get_safe_defaults(self):
        rec = _normalise_file_record({}, "j", 0, 0)
        assert rec["file_state"] == ""
        assert rec["source_surl"] == ""
        assert rec["dest_surl"] == ""
        assert rec["filesize"] == 0
        assert rec["throughput"] == 0.0


# ---------------------------------------------------------------------------
# _harvest_files
# ---------------------------------------------------------------------------

class TestHarvestFiles:
    def test_returns_normalised_list(self):
        items = [_fts_file(file_id=1), _fts_file(file_id=2)]
        client = _FakeClient([items])
        result = _harvest_files(client, "job-1", 0, 0)
        assert len(result) == 2
        assert result[0]["file_id"] == 1
        assert result[1]["file_id"] == 2

    def test_calls_correct_endpoint(self):
        client = _FakeClient([[]])
        _harvest_files(client, "job-abc-123", 0, 0)
        assert client.get_calls[0] == "/jobs/job-abc-123/files"

    def test_non_list_response_returns_empty(self):
        client = _FakeClient([{"unexpected": "dict"}])
        result = _harvest_files(client, "job-1", 0, 0)
        assert result == []

    def test_empty_response_returns_empty(self):
        client = _FakeClient([[]])
        result = _harvest_files(client, "job-1", 0, 0)
        assert result == []

    def test_chunk_index_and_retry_round_propagated(self):
        client = _FakeClient([[_fts_file(file_id=7)]])
        result = _harvest_files(client, "job-1", chunk_index=3, retry_round=2)
        assert result[0]["chunk_index"] == 3
        assert result[0]["retry_round"] == 2


# ---------------------------------------------------------------------------
# _harvest_retries
# ---------------------------------------------------------------------------

class TestHarvestRetries:
    def test_returns_retry_records(self):
        raw = [
            {"attempt": 0, "datetime": "2026-01-01T00:00:01",
             "reason": "timeout", "transfer_host": "worker1"},
        ]
        client = _FakeClient([raw])
        result = _harvest_retries(client, "job-1", 42)
        assert len(result) == 1
        assert result[0]["job_id"] == "job-1"
        assert result[0]["file_id"] == 42
        assert result[0]["attempt"] == 0
        assert result[0]["reason"] == "timeout"
        assert result[0]["transfer_host"] == "worker1"

    def test_calls_correct_endpoint(self):
        client = _FakeClient([[]])
        _harvest_retries(client, "job-abc", 7)
        assert client.get_calls[0] == "/jobs/job-abc/files/7/retries"

    def test_empty_list_returns_empty(self):
        client = _FakeClient([[]])
        result = _harvest_retries(client, "job-1", 1)
        assert result == []

    def test_non_list_response_returns_empty(self):
        client = _FakeClient([None])
        result = _harvest_retries(client, "job-1", 1)
        assert result == []

    def test_none_fields_become_empty_strings(self):
        raw = [{"attempt": 1, "datetime": None, "reason": None, "transfer_host": None}]
        client = _FakeClient([raw])
        result = _harvest_retries(client, "job-1", 1)
        assert result[0]["datetime"] == ""
        assert result[0]["reason"] == ""
        assert result[0]["transfer_host"] == ""


# ---------------------------------------------------------------------------
# _harvest_dm
# ---------------------------------------------------------------------------

class TestHarvestDm:
    def test_returns_dm_records(self):
        raw = [{"operation": "delete", "url": "https://dst/f"}]
        client = _FakeClient([raw])
        result = _harvest_dm(client, "job-1")
        assert len(result) == 1
        assert result[0]["operation"] == "delete"

    def test_calls_correct_endpoint(self):
        client = _FakeClient([[]])
        _harvest_dm(client, "job-xyz")
        assert client.get_calls[0] == "/jobs/job-xyz/dm"

    def test_exception_returns_empty(self):
        """DM endpoint may 404 for non-DM jobs — treat as empty."""
        import requests
        client = _FakeClient([requests.HTTPError("404")])
        result = _harvest_dm(client, "job-1")
        assert result == []

    def test_non_requests_exception_propagates(self):
        """W4: bare Exception was too broad — non-requests errors must not be swallowed."""
        client = _FakeClient([AttributeError("unexpected")])
        with pytest.raises(AttributeError):
            _harvest_dm(client, "job-1")

    def test_non_list_response_returns_empty(self):
        client = _FakeClient([{}])
        result = _harvest_dm(client, "job-1")
        assert result == []


# ---------------------------------------------------------------------------
# harvest_all
# ---------------------------------------------------------------------------

class TestHarvestAll:
    def _client_for_job(self, file_items, retry_items_per_file, dm_items):
        """Build a _FakeClient whose responses follow the harvest_all call order:
        GET /jobs/{id}/files → for each file: GET /jobs/{id}/files/{fid}/retries → GET /jobs/{id}/dm
        """
        responses = [file_items]
        for retries in retry_items_per_file:
            responses.append(retries)
        responses.append(dm_items)
        return _FakeClient(responses)

    def test_returns_three_lists(self):
        client = self._client_for_job([_fts_file(file_id=1)], [[]], [])
        file_recs, retry_recs, dm_recs = harvest_all([_subjob("job-1")], client)
        assert isinstance(file_recs, list)
        assert isinstance(retry_recs, list)
        assert isinstance(dm_recs, list)

    def test_single_job_single_file_no_retries(self):
        client = self._client_for_job([_fts_file(file_id=1)], [[]], [])
        file_recs, retry_recs, dm_recs = harvest_all([_subjob("job-1")], client)
        assert len(file_recs) == 1
        assert len(retry_recs) == 0

    def test_single_job_with_retries(self):
        retry_data = [
            {"attempt": 0, "datetime": "2026-01-01T00:00:01",
             "reason": "timeout", "transfer_host": "h1"},
        ]
        client = self._client_for_job([_fts_file(file_id=1)], [retry_data], [])
        file_recs, retry_recs, dm_recs = harvest_all([_subjob("job-1")], client)
        assert len(retry_recs) == 1
        assert retry_recs[0]["file_id"] == 1

    def test_multiple_files_retries_fetched_per_file(self):
        files = [_fts_file(file_id=1), _fts_file(file_id=2)]
        # file_id=1 has 1 retry, file_id=2 has 0
        retry_1 = [{"attempt": 0, "datetime": "", "reason": "err", "transfer_host": "h"}]
        retry_2 = []
        client = self._client_for_job(files, [retry_1, retry_2], [])
        file_recs, retry_recs, _ = harvest_all([_subjob("job-1")], client)
        assert len(file_recs) == 2
        assert len(retry_recs) == 1

    def test_non_terminal_job_skipped(self):
        client = _FakeClient([])
        file_recs, retry_recs, dm_recs = harvest_all(
            [_subjob("job-1", terminal=False)], client
        )
        assert file_recs == []
        assert retry_recs == []
        assert dm_recs == []
        assert len(client.get_calls) == 0

    def test_multiple_jobs_aggregated(self):
        # Two jobs, one file each, no retries
        responses = [
            [_fts_file(file_id=1)],  # job-1 files
            [],                        # job-1 file-1 retries
            [],                        # job-1 dm
            [_fts_file(file_id=2)],  # job-2 files
            [],                        # job-2 file-2 retries
            [],                        # job-2 dm
        ]
        client = _FakeClient(responses)
        subjobs = [_subjob("job-1"), _subjob("job-2")]
        file_recs, _, _ = harvest_all(subjobs, client)
        assert len(file_recs) == 2

    def test_file_records_carry_job_id(self):
        client = self._client_for_job([_fts_file(file_id=1)], [[]], [])
        file_recs, _, _ = harvest_all([_subjob("job-unique-99")], client)
        assert file_recs[0]["job_id"] == "job-unique-99"

    def test_token_expired_on_files_propagates(self):
        """TokenExpiredError from the files call must not be swallowed."""
        client = _FakeClient([TokenExpiredError()])
        with pytest.raises(TokenExpiredError):
            harvest_all([_subjob("job-1")], client)

    def test_token_expired_on_retries_propagates(self):
        """W3: TokenExpiredError from the retries call must propagate."""
        responses = [
            [_fts_file(file_id=1)],   # files — OK
            TokenExpiredError(),        # retries for file_id=1 — must propagate
        ]
        client = _FakeClient(responses)
        with pytest.raises(TokenExpiredError):
            harvest_all([_subjob("job-1")], client)

    def test_token_expired_on_dm_propagates(self):
        """TokenExpiredError from the DM call must propagate, not be swallowed."""
        # files: 1 file; retries: empty; dm: raises TokenExpiredError
        responses = [
            [_fts_file(file_id=1)],   # files
            [],                         # retries for file_id=1
            TokenExpiredError(),        # dm — must propagate
        ]
        client = _FakeClient(responses)
        with pytest.raises(TokenExpiredError):
            harvest_all([_subjob("job-1")], client)

    def test_dm_records_included(self):
        dm_raw = [{"op": "delete", "url": "https://dst/f"}]
        client = self._client_for_job([_fts_file(file_id=1)], [[]], dm_raw)
        _, _, dm_recs = harvest_all([_subjob("job-1")], client)
        assert len(dm_recs) == 1

    def test_empty_subjobs_returns_empty_lists(self):
        client = _FakeClient([])
        file_recs, retry_recs, dm_recs = harvest_all([], client)
        assert file_recs == []
        assert retry_recs == []
        assert dm_recs == []

    def test_staging_unsupported_job_harvested(self):
        """STAGING_UNSUPPORTED jobs are terminal and should be harvested."""
        client = self._client_for_job([_fts_file(file_id=1, file_state="STAGING")], [[]], [])
        file_recs, _, _ = harvest_all(
            [_subjob("job-1", terminal=True, status="STAGING_UNSUPPORTED")], client
        )
        assert len(file_recs) == 1
        assert file_recs[0]["file_state"] == "STAGING"
