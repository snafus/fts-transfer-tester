"""
Unit tests for fts_framework.fts.client.

All HTTP calls are mocked via the ``responses`` library.
"""

import unittest.mock

import pytest
import responses as resp_lib
import requests

from fts_framework.fts.client import (
    build_session,
    fts_request_with_retry,
    FTSClient,
)
from fts_framework.exceptions import TokenExpiredError


ENDPOINT = "https://fts3.example.org:8446"
TOKEN = "test-bearer-token"


# ---------------------------------------------------------------------------
# build_session
# ---------------------------------------------------------------------------

class TestBuildSession:
    def test_authorization_header_set(self):
        session = build_session(TOKEN, True)
        assert session.headers["Authorization"] == "Bearer {}".format(TOKEN)

    def test_ssl_verify_true(self):
        session = build_session(TOKEN, True)
        assert session.verify is True

    def test_ssl_verify_false(self):
        session = build_session(TOKEN, False)
        assert session.verify is False

    def test_ssl_verify_false_suppresses_insecure_warning(self):
        import urllib3
        with unittest.mock.patch.object(
            urllib3, "disable_warnings"
        ) as mock_disable:
            build_session(TOKEN, False)
        mock_disable.assert_called_once_with(urllib3.exceptions.InsecureRequestWarning)

    def test_ssl_verify_false_logs_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="fts_framework.fts.client"):
            build_session(TOKEN, False)
        assert any("SSL verification DISABLED" in r.message for r in caplog.records)

    def test_ssl_verify_ca_bundle_path(self):
        session = build_session(TOKEN, "/etc/pki/tls/certs/ca-bundle.crt")
        assert session.verify == "/etc/pki/tls/certs/ca-bundle.crt"

    def test_returns_requests_session(self):
        session = build_session(TOKEN, True)
        assert isinstance(session, requests.Session)

    def test_different_tokens_produce_different_headers(self):
        s1 = build_session("token-a", True)
        s2 = build_session("token-b", True)
        assert s1.headers["Authorization"] != s2.headers["Authorization"]


# ---------------------------------------------------------------------------
# fts_request_with_retry — success paths
# ---------------------------------------------------------------------------

class TestFtsRequestWithRetry:
    @resp_lib.activate
    def test_200_returned_immediately(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/whoami", status=200, json={"user": "test"})
        session = build_session(TOKEN, True)
        resp = fts_request_with_retry(session, "GET", ENDPOINT + "/whoami", max_retries=3)
        assert resp.status_code == 200

    @resp_lib.activate
    def test_404_returned_without_retry(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs/missing", status=404)
        session = build_session(TOKEN, True)
        resp = fts_request_with_retry(session, "GET", ENDPOINT + "/jobs/missing", max_retries=3)
        assert resp.status_code == 404
        assert len(resp_lib.calls) == 1

    @resp_lib.activate
    def test_500_returned_without_retry(self):
        # 500 is not in _TRANSIENT_STATUS_CODES — return immediately
        resp_lib.add(resp_lib.POST, ENDPOINT + "/jobs", status=500)
        session = build_session(TOKEN, True)
        resp = fts_request_with_retry(session, "POST", ENDPOINT + "/jobs",
                                      max_retries=3, initial_backoff=0)
        assert resp.status_code == 500
        assert len(resp_lib.calls) == 1

    @resp_lib.activate
    def test_transient_503_retried_then_succeeds(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=503)
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=503)
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=200, json=[])
        session = build_session(TOKEN, True)
        resp = fts_request_with_retry(session, "GET", ENDPOINT + "/jobs",
                                      max_retries=3, initial_backoff=0)
        assert resp.status_code == 200
        assert len(resp_lib.calls) == 3

    @resp_lib.activate
    def test_transient_429_retried(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=429)
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=200, json=[])
        session = build_session(TOKEN, True)
        resp = fts_request_with_retry(session, "GET", ENDPOINT + "/jobs",
                                      max_retries=3, initial_backoff=0)
        assert resp.status_code == 200

    @resp_lib.activate
    def test_transient_502_retried(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=502)
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=200, json=[])
        session = build_session(TOKEN, True)
        resp = fts_request_with_retry(session, "GET", ENDPOINT + "/jobs",
                                      max_retries=2, initial_backoff=0)
        assert resp.status_code == 200

    @resp_lib.activate
    def test_transient_504_retried(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=504)
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=200, json=[])
        session = build_session(TOKEN, True)
        resp = fts_request_with_retry(session, "GET", ENDPOINT + "/jobs",
                                      max_retries=2, initial_backoff=0)
        assert resp.status_code == 200

    @resp_lib.activate
    def test_all_retries_exhausted_raises_http_error(self):
        """When all retries are transient errors, raise HTTPError."""
        for _ in range(3):
            resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=503)
        session = build_session(TOKEN, True)
        with pytest.raises(requests.HTTPError) as exc_info:
            fts_request_with_retry(session, "GET", ENDPOINT + "/jobs",
                                   max_retries=3, initial_backoff=0)
        assert exc_info.value.response.status_code == 503
        assert len(resp_lib.calls) == 3

    @resp_lib.activate
    def test_transient_status_max_retries_one_raises(self):
        """max_retries=1 with a transient status: single attempt then raise."""
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs", status=503)
        session = build_session(TOKEN, True)
        with pytest.raises(requests.HTTPError):
            fts_request_with_retry(session, "GET", ENDPOINT + "/jobs",
                                   max_retries=1, initial_backoff=0)
        assert len(resp_lib.calls) == 1

    def test_connection_error_raises_after_retries(self):
        with resp_lib.RequestsMock() as rsps:
            rsps.add(resp_lib.GET, ENDPOINT + "/jobs",
                     body=requests.ConnectionError("network down"))
            rsps.add(resp_lib.GET, ENDPOINT + "/jobs",
                     body=requests.ConnectionError("network down"))
            session = build_session(TOKEN, True)
            with pytest.raises(requests.ConnectionError):
                fts_request_with_retry(session, "GET", ENDPOINT + "/jobs",
                                       max_retries=2, initial_backoff=0)

    def test_timeout_raises_after_retries(self):
        with resp_lib.RequestsMock() as rsps:
            for _ in range(2):
                rsps.add(resp_lib.GET, ENDPOINT + "/whoami",
                         body=requests.Timeout("timed out"))
            session = build_session(TOKEN, True)
            with pytest.raises(requests.Timeout):
                fts_request_with_retry(session, "GET", ENDPOINT + "/whoami",
                                       max_retries=2, initial_backoff=0)

    def test_max_retries_one_means_no_retry(self):
        with resp_lib.RequestsMock() as rsps:
            rsps.add(resp_lib.GET, ENDPOINT + "/jobs",
                     body=requests.ConnectionError("fail"))
            session = build_session(TOKEN, True)
            with pytest.raises(requests.ConnectionError):
                fts_request_with_retry(session, "GET", ENDPOINT + "/jobs",
                                       max_retries=1, initial_backoff=0)
            assert len(rsps.calls) == 1


# ---------------------------------------------------------------------------
# FTSClient
# ---------------------------------------------------------------------------

class TestFTSClientGet:
    @resp_lib.activate
    def test_get_returns_parsed_json(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/whoami",
                     json={"dn": "test"}, status=200)
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT, session)
        result = client.get("/whoami")
        assert result == {"dn": "test"}

    @resp_lib.activate
    def test_get_with_params(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs",
                     json=[], status=200)
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT, session)
        result = client.get("/jobs", params={"time_window": 0.1})
        assert result == []

    @resp_lib.activate
    def test_get_401_raises_token_expired(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/whoami", status=401)
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT, session)
        with pytest.raises(TokenExpiredError):
            client.get("/whoami")

    @resp_lib.activate
    def test_get_404_raises_http_error(self):
        """Non-2xx non-401 responses from GET raise HTTPError, not ValueError."""
        resp_lib.add(resp_lib.GET, ENDPOINT + "/jobs/missing", status=404, body="not found")
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT, session, max_retries=1)
        with pytest.raises(requests.HTTPError) as exc_info:
            client.get("/jobs/missing")
        assert exc_info.value.response.status_code == 404

    @resp_lib.activate
    def test_endpoint_trailing_slash_stripped(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/whoami",
                     json={"dn": "test"}, status=200)
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT + "/", session)
        result = client.get("/whoami")
        assert result == {"dn": "test"}

    @resp_lib.activate
    def test_path_without_leading_slash(self):
        resp_lib.add(resp_lib.GET, ENDPOINT + "/whoami",
                     json={"dn": "test"}, status=200)
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT, session)
        result = client.get("whoami")
        assert result == {"dn": "test"}


class TestFTSClientPost:
    @resp_lib.activate
    def test_post_returns_response(self):
        resp_lib.add(resp_lib.POST, ENDPOINT + "/jobs",
                     json={"job_id": "abc-123"}, status=200)
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT, session)
        resp = client.post("/jobs", {"files": []})
        assert resp.status_code == 200
        assert resp.json()["job_id"] == "abc-123"

    @resp_lib.activate
    def test_post_500_returned_not_raised(self):
        """500 must be returned to caller for 500-recovery logic, not raised."""
        resp_lib.add(resp_lib.POST, ENDPOINT + "/jobs", status=500, body="internal error")
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT, session, max_retries=1)
        resp = client.post("/jobs", {})
        assert resp.status_code == 500

    @resp_lib.activate
    def test_post_401_raises_token_expired(self):
        resp_lib.add(resp_lib.POST, ENDPOINT + "/jobs", status=401)
        session = build_session(TOKEN, True)
        client = FTSClient(ENDPOINT, session)
        with pytest.raises(TokenExpiredError):
            client.post("/jobs", {})
