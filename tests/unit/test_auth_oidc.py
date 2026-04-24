"""Unit tests for fts_framework.auth.oidc."""

import pytest
import responses as responses_lib

from fts_framework.auth.oidc import fetch_token
from fts_framework.exceptions import ConfigError

_ENDPOINT = "https://iam.example.org/token"


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
