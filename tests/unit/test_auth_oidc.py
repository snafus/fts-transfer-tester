"""Unit tests for fts_framework.auth.oidc."""

import pytest
import responses as responses_lib

from fts_framework.auth.oidc import fetch_token
from fts_framework.exceptions import ConfigError

_ENDPOINT = "https://iam.example.org/token"


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    """Record backoff sleeps instead of waiting."""
    calls = []
    monkeypatch.setattr("fts_framework.auth.oidc.time.sleep", calls.append)
    return calls


@responses_lib.activate
def test_returns_access_token():
    responses_lib.add(
        responses_lib.POST, _ENDPOINT,
        json={"access_token": "tok_abc123", "token_type": "Bearer"},
        status=200,
    )
    token = fetch_token(_ENDPOINT, "cid", "csecret", "openid profile", True)
    assert token == "tok_abc123"


@responses_lib.activate
def test_request_uses_client_credentials_grant():
    captured = []

    def _cb(request):
        captured.append(request.body)
        return (200, {}, '{"access_token": "t"}')

    responses_lib.add_callback(responses_lib.POST, _ENDPOINT, callback=_cb)
    fetch_token(_ENDPOINT, "my_client", "my_secret", "openid", True)

    body = captured[0]
    assert "grant_type=client_credentials" in body
    assert "client_id=my_client" in body
    assert "scope=openid" in body


@responses_lib.activate
def test_http_error_raises_config_error():
    responses_lib.add(
        responses_lib.POST, _ENDPOINT,
        json={"error": "unauthorized_client"},
        status=401,
    )
    with pytest.raises(ConfigError, match="HTTP 401"):
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True)


@responses_lib.activate
def test_missing_access_token_raises_config_error():
    responses_lib.add(
        responses_lib.POST, _ENDPOINT,
        json={"token_type": "Bearer"},
        status=200,
    )
    with pytest.raises(ConfigError, match="access_token"):
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True)


@responses_lib.activate
def test_non_json_response_raises_config_error():
    responses_lib.add(
        responses_lib.POST, _ENDPOINT,
        body="not json",
        status=200,
    )
    with pytest.raises(ConfigError):
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True)


@responses_lib.activate
def test_audience_included_when_provided():
    captured = []

    def _cb(request):
        captured.append(request.body)
        return (200, {}, '{"access_token": "t"}')

    responses_lib.add_callback(responses_lib.POST, _ENDPOINT, callback=_cb)
    fetch_token(_ENDPOINT, "cid", "csecret", "openid", True,
                audience="https://wlcg.cern.ch/jwt/v1/any")

    assert "audience=https" in captured[0]


@responses_lib.activate
def test_audience_omitted_when_none():
    captured = []

    def _cb(request):
        captured.append(request.body)
        return (200, {}, '{"access_token": "t"}')

    responses_lib.add_callback(responses_lib.POST, _ENDPOINT, callback=_cb)
    fetch_token(_ENDPOINT, "cid", "csecret", "openid", True, audience=None)

    assert "audience" not in captured[0]


def test_connection_error_raises_config_error():
    import requests
    import responses as resp_mod
    with resp_mod.RequestsMock() as rsps:
        rsps.add(
            resp_mod.POST, _ENDPOINT,
            body=requests.exceptions.ConnectionError("connection refused"),
        )
        with pytest.raises(ConfigError, match="failed"):
            fetch_token(_ENDPOINT, "cid", "csecret", "openid", True)


# ---------------------------------------------------------------------------
# Transient-failure retry
# ---------------------------------------------------------------------------

_BAD_GATEWAY = "<html><head><title>502 Bad Gateway</title></head></html>"


@responses_lib.activate
def test_transient_502_then_success_returns_token(sleeps):
    responses_lib.add(responses_lib.POST, _ENDPOINT, body=_BAD_GATEWAY, status=502)
    responses_lib.add(responses_lib.POST, _ENDPOINT, body=_BAD_GATEWAY, status=502)
    responses_lib.add(responses_lib.POST, _ENDPOINT,
                      json={"access_token": "tok_after_retry"}, status=200)

    token = fetch_token(_ENDPOINT, "cid", "csecret", "openid", True)

    assert token == "tok_after_retry"
    assert len(responses_lib.calls) == 3
    assert sleeps == [5, 10]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
@responses_lib.activate
def test_transient_statuses_are_retried(status, sleeps):
    responses_lib.add(responses_lib.POST, _ENDPOINT, body="busy", status=status)
    responses_lib.add(responses_lib.POST, _ENDPOINT,
                      json={"access_token": "t"}, status=200)

    assert fetch_token(_ENDPOINT, "cid", "csecret", "openid", True) == "t"
    assert len(responses_lib.calls) == 2


@responses_lib.activate
def test_persistent_502_raises_after_all_attempts(sleeps):
    responses_lib.add(responses_lib.POST, _ENDPOINT, body=_BAD_GATEWAY, status=502)

    with pytest.raises(ConfigError, match="HTTP 502") as excinfo:
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True)

    assert "gave up after 5 attempts" in str(excinfo.value)
    assert len(responses_lib.calls) == 5
    assert sleeps == [5, 10, 20, 40]


@responses_lib.activate
def test_backoff_is_capped(sleeps):
    responses_lib.add(responses_lib.POST, _ENDPOINT, body=_BAD_GATEWAY, status=502)

    with pytest.raises(ConfigError):
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True,
                    retry_max=6, retry_backoff_s=10)

    assert sleeps == [10, 20, 40, 60, 60, 60]


@responses_lib.activate
def test_retry_max_zero_disables_retry(sleeps):
    responses_lib.add(responses_lib.POST, _ENDPOINT, body=_BAD_GATEWAY, status=502)

    with pytest.raises(ConfigError, match="HTTP 502") as excinfo:
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True, retry_max=0)

    assert "gave up" not in str(excinfo.value)
    assert len(responses_lib.calls) == 1
    assert sleeps == []


@pytest.mark.parametrize("status", [400, 401, 403, 404])
@responses_lib.activate
def test_client_errors_are_not_retried(status, sleeps):
    responses_lib.add(responses_lib.POST, _ENDPOINT,
                      json={"error": "invalid_client"}, status=status)

    with pytest.raises(ConfigError, match="HTTP {}".format(status)):
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True)

    assert len(responses_lib.calls) == 1
    assert sleeps == []


@responses_lib.activate
def test_connection_error_then_success_returns_token(sleeps):
    import requests
    responses_lib.add(
        responses_lib.POST, _ENDPOINT,
        body=requests.exceptions.ConnectionError("connection reset"),
    )
    responses_lib.add(responses_lib.POST, _ENDPOINT,
                      json={"access_token": "t"}, status=200)

    assert fetch_token(_ENDPOINT, "cid", "csecret", "openid", True) == "t"
    assert sleeps == [5]


@responses_lib.activate
def test_timeout_is_retried(sleeps):
    import requests
    responses_lib.add(
        responses_lib.POST, _ENDPOINT,
        body=requests.exceptions.ReadTimeout("read timed out"),
    )
    responses_lib.add(responses_lib.POST, _ENDPOINT,
                      json={"access_token": "t"}, status=200)

    assert fetch_token(_ENDPOINT, "cid", "csecret", "openid", True) == "t"
    assert sleeps == [5]


@responses_lib.activate
def test_persistent_connection_error_raises_after_all_attempts(sleeps):
    import requests
    responses_lib.add(
        responses_lib.POST, _ENDPOINT,
        body=requests.exceptions.ConnectionError("connection refused"),
    )

    with pytest.raises(ConfigError, match="failed") as excinfo:
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True, retry_max=2)

    assert "gave up after 3 attempts" in str(excinfo.value)
    assert len(responses_lib.calls) == 3
    assert sleeps == [5, 10]


@responses_lib.activate
def test_ssl_error_is_not_retried(sleeps):
    import requests
    responses_lib.add(
        responses_lib.POST, _ENDPOINT,
        body=requests.exceptions.SSLError("certificate verify failed"),
    )

    with pytest.raises(ConfigError, match="certificate verify failed"):
        fetch_token(_ENDPOINT, "cid", "csecret", "openid", True)

    assert len(responses_lib.calls) == 1
    assert sleeps == []


@responses_lib.activate
def test_retry_warning_does_not_log_secret_or_body(sleeps, caplog):
    import logging
    responses_lib.add(responses_lib.POST, _ENDPOINT, body=_BAD_GATEWAY, status=502)
    responses_lib.add(responses_lib.POST, _ENDPOINT,
                      json={"access_token": "tok_secret_value"}, status=200)

    with caplog.at_level(logging.DEBUG, logger="fts_framework.auth.oidc"):
        fetch_token(_ENDPOINT, "cid", "very_secret", "openid", True)

    assert "attempt 1/5" in caplog.text
    assert "HTTP 502" in caplog.text
    assert "very_secret" not in caplog.text
    assert "tok_secret_value" not in caplog.text
    assert "Bad Gateway" not in caplog.text
