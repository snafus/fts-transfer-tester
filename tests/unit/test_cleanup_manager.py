"""
Unit tests for fts_framework.cleanup.manager.

All HTTP calls are mocked via the ``responses`` library.
"""

import pytest
import responses as resp_lib
import requests
from collections import OrderedDict

from fts_framework.cleanup.manager import (
    cleanup_pre,
    cleanup_post,
    _delete_one,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE = "https://storage.example.org"

def _session(token="dest-write-token", ssl_verify=True):
    s = requests.Session()
    s.headers["Authorization"] = "Bearer {}".format(token)
    s.verify = ssl_verify
    return s


def _mapping(*dest_urls):
    """Build a minimal OrderedDict with integer source keys."""
    return OrderedDict(
        ("https://src.example.org/f{:03d}.dat".format(i), url)
        for i, url in enumerate(dest_urls)
    )


def _file_record(dest_surl, file_state="FINISHED"):
    return {
        "file_id": 1,
        "job_id": "job-1",
        "dest_surl": dest_surl,
        "file_state": file_state,
        "source_surl": "https://src.example.org/f.dat",
    }


# ---------------------------------------------------------------------------
# _delete_one
# ---------------------------------------------------------------------------

class TestDeleteOne:
    @resp_lib.activate
    def test_200_success(self):
        url = BASE + "/data/testfile_000000"
        resp_lib.add(resp_lib.DELETE, url, status=200)
        record = _delete_one(url, _session())
        assert record["success"] is True
        assert record["status_code"] == 200
        assert record["error"] is None

    @resp_lib.activate
    def test_204_success(self):
        url = BASE + "/data/testfile_000001"
        resp_lib.add(resp_lib.DELETE, url, status=204)
        record = _delete_one(url, _session())
        assert record["success"] is True
        assert record["status_code"] == 204

    @resp_lib.activate
    def test_404_treated_as_success(self):
        url = BASE + "/data/testfile_missing"
        resp_lib.add(resp_lib.DELETE, url, status=404)
        record = _delete_one(url, _session())
        assert record["success"] is True
        assert record["status_code"] == 404

    @resp_lib.activate
    def test_403_non_fatal(self):
        url = BASE + "/data/testfile_noperm"
        resp_lib.add(resp_lib.DELETE, url, status=403)
        record = _delete_one(url, _session())
        assert record["success"] is False
        assert record["status_code"] == 403
        assert "403" in record["error"]

    @resp_lib.activate
    def test_500_non_fatal(self):
        url = BASE + "/data/testfile_err"
        resp_lib.add(resp_lib.DELETE, url, status=500)
        record = _delete_one(url, _session())
        assert record["success"] is False
        assert record["status_code"] == 500

    def test_connection_error_non_fatal(self):
        with resp_lib.RequestsMock() as rsps:
            url = BASE + "/data/testfile_conn"
            rsps.add(resp_lib.DELETE, url, body=requests.ConnectionError("no route"))
            record = _delete_one(url, _session())
        assert record["success"] is False
        assert record["status_code"] is None
        assert "no route" in record["error"]

    @resp_lib.activate
    def test_audit_record_contains_url(self):
        url = BASE + "/data/testfile_000000"
        resp_lib.add(resp_lib.DELETE, url, status=204)
        record = _delete_one(url, _session())
        assert record["url"] == url


# ---------------------------------------------------------------------------
# cleanup_pre
# ---------------------------------------------------------------------------

class TestCleanupPre:
    @resp_lib.activate
    def test_deletes_all_dest_urls(self):
        urls = [BASE + "/data/testfile_{:06d}".format(i) for i in range(3)]
        for url in urls:
            resp_lib.add(resp_lib.DELETE, url, status=204)
        mapping = _mapping(*urls)
        audit = cleanup_pre(mapping, _session(), {})
        assert len(audit) == 3
        assert len(resp_lib.calls) == 3

    @resp_lib.activate
    def test_returns_audit_records(self):
        url = BASE + "/data/testfile_000000"
        resp_lib.add(resp_lib.DELETE, url, status=204)
        audit = cleanup_pre(_mapping(url), _session(), {})
        assert isinstance(audit, list)
        assert audit[0]["url"] == url
        assert audit[0]["success"] is True

    @resp_lib.activate
    def test_failure_does_not_abort(self):
        """A 500 on one URL must not prevent the others from being attempted."""
        url0 = BASE + "/data/testfile_000000"
        url1 = BASE + "/data/testfile_000001"
        url2 = BASE + "/data/testfile_000002"
        resp_lib.add(resp_lib.DELETE, url0, status=204)
        resp_lib.add(resp_lib.DELETE, url1, status=500)  # failure
        resp_lib.add(resp_lib.DELETE, url2, status=204)
        audit = cleanup_pre(_mapping(url0, url1, url2), _session(), {})
        assert len(audit) == 3
        assert audit[0]["success"] is True
        assert audit[1]["success"] is False
        assert audit[2]["success"] is True

    @resp_lib.activate
    def test_404_not_counted_as_failure(self):
        url = BASE + "/data/testfile_absent"
        resp_lib.add(resp_lib.DELETE, url, status=404)
        audit = cleanup_pre(_mapping(url), _session(), {})
        assert audit[0]["success"] is True

    def test_empty_mapping_returns_empty_audit(self):
        audit = cleanup_pre(OrderedDict(), _session(), {})
        assert audit == []

    @resp_lib.activate
    def test_connection_error_does_not_abort(self):
        url0 = BASE + "/data/testfile_000000"
        url1 = BASE + "/data/testfile_000001"
        with resp_lib.RequestsMock() as rsps:
            rsps.add(resp_lib.DELETE, url0, body=requests.ConnectionError("down"))
            rsps.add(resp_lib.DELETE, url1, status=204)
            audit = cleanup_pre(_mapping(url0, url1), _session(), {})
        assert audit[0]["success"] is False
        assert audit[1]["success"] is True


# ---------------------------------------------------------------------------
# cleanup_post
# ---------------------------------------------------------------------------

class TestCleanupPost:
    @resp_lib.activate
    def test_only_finished_files_deleted(self):
        finished_url = BASE + "/data/testfile_000000"
        failed_url = BASE + "/data/testfile_000001"
        resp_lib.add(resp_lib.DELETE, finished_url, status=204)
        file_records = [
            _file_record(finished_url, file_state="FINISHED"),
            _file_record(failed_url, file_state="FAILED"),
        ]
        audit = cleanup_post(file_records, _session(), {})
        assert len(audit) == 1
        assert audit[0]["url"] == finished_url
        # Only one DELETE should have been sent
        assert len(resp_lib.calls) == 1

    @resp_lib.activate
    def test_not_used_files_skipped(self):
        url = BASE + "/data/testfile_000000"
        file_records = [_file_record(url, file_state="NOT_USED")]
        audit = cleanup_post(file_records, _session(), {})
        assert audit == []
        assert len(resp_lib.calls) == 0

    @resp_lib.activate
    def test_canceled_files_skipped(self):
        url = BASE + "/data/testfile_000000"
        file_records = [_file_record(url, file_state="CANCELED")]
        audit = cleanup_post(file_records, _session(), {})
        assert audit == []

    def test_empty_file_records_returns_empty(self):
        audit = cleanup_post([], _session(), {})
        assert audit == []

    @resp_lib.activate
    def test_all_failed_returns_empty(self):
        records = [
            _file_record(BASE + "/data/f{}.dat".format(i), file_state="FAILED")
            for i in range(3)
        ]
        audit = cleanup_post(records, _session(), {})
        assert audit == []

    @resp_lib.activate
    def test_returns_audit_for_finished(self):
        url = BASE + "/data/testfile_000000"
        resp_lib.add(resp_lib.DELETE, url, status=204)
        audit = cleanup_post([_file_record(url, "FINISHED")], _session(), {})
        assert len(audit) == 1
        assert audit[0]["success"] is True

    @resp_lib.activate
    def test_failure_does_not_abort_post_cleanup(self):
        url0 = BASE + "/data/testfile_000000"
        url1 = BASE + "/data/testfile_000001"
        resp_lib.add(resp_lib.DELETE, url0, status=403)
        resp_lib.add(resp_lib.DELETE, url1, status=204)
        records = [
            _file_record(url0, "FINISHED"),
            _file_record(url1, "FINISHED"),
        ]
        audit = cleanup_post(records, _session(), {})
        assert len(audit) == 2
        assert audit[0]["success"] is False
        assert audit[1]["success"] is True

    @resp_lib.activate
    def test_mixed_states_only_finished_in_audit(self):
        urls = [BASE + "/data/testfile_{:06d}".format(i) for i in range(4)]
        states = ["FINISHED", "FAILED", "FINISHED", "CANCELED"]
        resp_lib.add(resp_lib.DELETE, urls[0], status=204)
        resp_lib.add(resp_lib.DELETE, urls[2], status=204)
        records = [_file_record(u, s) for u, s in zip(urls, states)]
        audit = cleanup_post(records, _session(), {})
        assert len(audit) == 2
        assert all(r["success"] for r in audit)
