"""
Unit tests for X.509 proxy authentication (``auth.mode: x509``).

Covers fts_framework.auth.x509 (proxy path resolution, file validation,
local and delegated lifetime checks), the cert-aware session builders and
``ProxyExpiredError`` in fts_framework.fts.client, the x509 branch of the
config loader (no mixed mode), payload construction without storage tokens,
and the run_campaign pre-flight.
"""

import logging
from datetime import datetime

import pytest
import responses
import yaml

from fts_framework.auth import x509
from fts_framework.config.loader import identify_token_sources, load
from fts_framework.exceptions import (
    ConfigError,
    ProxyExpiredError,
    TokenExpiredError,
    X509CredentialError,
)
from fts_framework.fts.client import FTSClient, build_role_session, build_session
from fts_framework.fts.submission import build_payload

ENDPOINT = "https://fts.example.org:8446"

# Self-signed EC certificate, notAfter = Oct  8 12:14:47 2049 GMT.
_TEST_CERT = """-----BEGIN CERTIFICATE-----
MIIBxzCCAW2gAwIBAgIUNgX+8KmfjOQ4TUlRigJ0loStXkgwCgYIKoZIzj0EAwIw
OTEUMBIGCgmSJomT8ixkARkWBHRlc3QxITAfBgNVBAMMGGZ0cy1mcmFtZXdvcmsg
dGVzdCBwcm94eTAeFw0yNjEwMDkxMjE0NDdaFw00OTEwMDgxMjE0NDdaMDkxFDAS
BgoJkiaJk/IsZAEZFgR0ZXN0MSEwHwYDVQQDDBhmdHMtZnJhbWV3b3JrIHRlc3Qg
cHJveHkwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARBgKYzmE/JYzLPnAD0sSdo
EGusF7HxEKXOl7amHAag+5BllM2ToTLX9rjkXhGct0J9qdHVMVDGik5TYkn7pxYa
o1MwUTAdBgNVHQ4EFgQUF0D7mo0BuOYE2UGBpoV/gn1KlbwwHwYDVR0jBBgwFoAU
F0D7mo0BuOYE2UGBpoV/gn1KlbwwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQD
AgNIADBFAiEAgnTiuOIavPkwHoWKEqH9Ag7Wa14pBRmE3ZKxyon4/OcCIFhBBJ8s
jsSdizN5E2KhpiCRN5ew6aOLLF+JcuGQxgm3
-----END CERTIFICATE-----
"""
_TEST_CERT_NOT_AFTER = datetime(2049, 10, 8, 12, 14, 47)

# Structural placeholder only — never parsed as a key by these tests.
_FAKE_KEY = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"

_WHOAMI = {
    "user_dn": "/DC=test/CN=user",
    "delegation_id": "abc123",
    "voms_cred": ["/dteam/Role=NULL/Capability=NULL"],
}

_TOKEN_ENV_VARS = ("FTS_TOKEN", "FTS_SUBMIT_TOKEN", "SOURCE_READ_TOKEN",
                   "DEST_WRITE_TOKEN", "X509_USER_PROXY")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _TOKEN_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _write_proxy(tmp_path, content=None, name="x509up_test"):
    path = str(tmp_path / name)
    with open(path, "w") as fh:
        fh.write(_TEST_CERT + _FAKE_KEY if content is None else content)
    return path


def _client(proxy):
    return FTSClient(ENDPOINT, build_session(None, True, cert=proxy), max_retries=1)


# ---------------------------------------------------------------------------
# resolve_proxy_path
# ---------------------------------------------------------------------------

class TestResolveProxyPath:
    def test_cli_wins_over_everything(self, monkeypatch):
        monkeypatch.setenv("X509_USER_PROXY", "/env/proxy")
        assert x509.resolve_proxy_path("/cli/proxy", "/yaml/proxy") == "/cli/proxy"

    def test_env_wins_over_yaml(self, monkeypatch):
        monkeypatch.setenv("X509_USER_PROXY", "/env/proxy")
        assert x509.resolve_proxy_path(None, "/yaml/proxy") == "/env/proxy"

    def test_yaml_used_when_no_cli_or_env(self):
        assert x509.resolve_proxy_path(None, "/yaml/proxy") == "/yaml/proxy"

    def test_default_uid_path(self, monkeypatch):
        monkeypatch.setattr(x509.os, "getuid", lambda: 4242)
        assert x509.resolve_proxy_path() == "/tmp/x509up_u4242"


# ---------------------------------------------------------------------------
# validate_proxy_file
# ---------------------------------------------------------------------------

class TestValidateProxyFile:
    def test_valid_file_passes(self, tmp_path):
        x509.validate_proxy_file(_write_proxy(tmp_path))

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(ConfigError, match="not found"):
            x509.validate_proxy_file(str(tmp_path / "absent"))

    def test_no_certificate_raises(self, tmp_path):
        with pytest.raises(ConfigError, match="no PEM certificate"):
            x509.validate_proxy_file(_write_proxy(tmp_path, _FAKE_KEY))

    def test_no_private_key_raises(self, tmp_path):
        with pytest.raises(ConfigError, match="no PEM private key"):
            x509.validate_proxy_file(_write_proxy(tmp_path, _TEST_CERT))

    def test_pkcs8_encrypted_key_rejected(self, tmp_path):
        content = _TEST_CERT + (
            "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n"
            "-----END ENCRYPTED PRIVATE KEY-----\n"
        )
        with pytest.raises(ConfigError, match="encrypted private key"):
            x509.validate_proxy_file(_write_proxy(tmp_path, content))

    def test_legacy_encrypted_rsa_key_rejected(self, tmp_path):
        content = _TEST_CERT + (
            "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\n"
            "DEK-Info: AES-256-CBC,00\n\nAAAA\n-----END RSA PRIVATE KEY-----\n"
        )
        with pytest.raises(ConfigError, match="voms-proxy-init"):
            x509.validate_proxy_file(_write_proxy(tmp_path, content))


# ---------------------------------------------------------------------------
# Local proxy lifetime
# ---------------------------------------------------------------------------

class TestProxyLifetime:
    def test_not_after_decoded_from_pem(self, tmp_path):
        assert x509.proxy_not_after(_write_proxy(tmp_path)) == _TEST_CERT_NOT_AFTER

    def test_sufficient_lifetime_passes(self, tmp_path):
        x509.check_proxy_lifetime(
            _write_proxy(tmp_path), 3600, now=datetime(2049, 10, 8, 11, 0, 0),
        )

    def test_below_minimum_raises(self, tmp_path):
        with pytest.raises(X509CredentialError, match="voms-proxy-init"):
            x509.check_proxy_lifetime(
                _write_proxy(tmp_path), 3600, now=datetime(2049, 10, 8, 12, 0, 0),
            )

    def test_already_expired_reports_zero_remaining(self, tmp_path):
        with pytest.raises(X509CredentialError, match=" 0s remaining"):
            x509.check_proxy_lifetime(
                _write_proxy(tmp_path), 0, now=datetime(2050, 1, 1),
            )

    def test_undecodable_build_skips_with_warning(self, tmp_path, monkeypatch, caplog):
        monkeypatch.setattr(x509, "proxy_not_after", lambda path: None)
        with caplog.at_level(logging.WARNING, logger="fts_framework.auth.x509"):
            x509.check_proxy_lifetime(_write_proxy(tmp_path), 3600)
        assert "skipping local lifetime check" in caplog.text


# ---------------------------------------------------------------------------
# Delegated credential check
# ---------------------------------------------------------------------------

class TestCheckDelegation:
    @responses.activate
    def test_valid_delegation_returns_termination(self, tmp_path):
        responses.add(responses.GET, ENDPOINT + "/delegation/abc123",
                      json={"termination_time": "2026-10-10T12:00:00", "voms": True})
        result = x509.check_delegation(
            _client(_write_proxy(tmp_path)), _WHOAMI, 3600,
            now=datetime(2026, 10, 10, 10, 0, 0),
        )
        assert result == datetime(2026, 10, 10, 12, 0, 0)

    @pytest.mark.parametrize("value", [
        "2026-10-10T12:00:00.123456",
        "2026-10-10 12:00:00",
        "2026-10-10T12:00:00Z",
    ])
    @responses.activate
    def test_termination_time_formats(self, tmp_path, value):
        responses.add(responses.GET, ENDPOINT + "/delegation/abc123",
                      json={"termination_time": value})
        result = x509.check_delegation(
            _client(_write_proxy(tmp_path)), _WHOAMI, 0,
            now=datetime(2026, 10, 10, 10, 0, 0),
        )
        assert result.replace(microsecond=0) == datetime(2026, 10, 10, 12, 0, 0)

    @responses.activate
    def test_unparseable_termination_time_raises(self, tmp_path):
        responses.add(responses.GET, ENDPOINT + "/delegation/abc123",
                      json={"termination_time": "tomorrow"})
        with pytest.raises(X509CredentialError, match="Cannot parse"):
            x509.check_delegation(_client(_write_proxy(tmp_path)), _WHOAMI, 0)

    @responses.activate
    def test_below_minimum_raises(self, tmp_path):
        responses.add(responses.GET, ENDPOINT + "/delegation/abc123",
                      json={"termination_time": "2026-10-10T10:30:00"})
        with pytest.raises(X509CredentialError, match="Re-delegate"):
            x509.check_delegation(
                _client(_write_proxy(tmp_path)), _WHOAMI, 3600,
                now=datetime(2026, 10, 10, 10, 0, 0),
            )

    @responses.activate
    def test_null_body_means_no_delegation(self, tmp_path):
        responses.add(responses.GET, ENDPOINT + "/delegation/abc123", body="null",
                      content_type="application/json")
        with pytest.raises(X509CredentialError, match="No delegated credential"):
            x509.check_delegation(_client(_write_proxy(tmp_path)), _WHOAMI, 0)

    @responses.activate
    def test_404_means_no_delegation(self, tmp_path):
        responses.add(responses.GET, ENDPOINT + "/delegation/abc123", status=404)
        with pytest.raises(X509CredentialError, match="fts-rest-delegate"):
            x509.check_delegation(_client(_write_proxy(tmp_path)), _WHOAMI, 0)

    def test_missing_delegation_id_raises(self, tmp_path):
        with pytest.raises(X509CredentialError, match="no delegation_id"):
            x509.check_delegation(_client(_write_proxy(tmp_path)), {"user_dn": "x"}, 0)

    @responses.activate
    def test_401_raises_proxy_expired(self, tmp_path):
        responses.add(responses.GET, ENDPOINT + "/delegation/abc123", status=401)
        with pytest.raises(ProxyExpiredError):
            x509.check_delegation(_client(_write_proxy(tmp_path)), _WHOAMI, 0)

    @responses.activate
    def test_no_voms_attributes_warns(self, tmp_path, caplog):
        responses.add(responses.GET, ENDPOINT + "/delegation/abc123",
                      json={"termination_time": "2049-01-01T00:00:00"})
        whoami = dict(_WHOAMI, voms_cred=[])
        with caplog.at_level(logging.WARNING, logger="fts_framework.auth.x509"):
            x509.check_delegation(_client(_write_proxy(tmp_path)), whoami, 0)
        assert "no VOMS attributes" in caplog.text


# ---------------------------------------------------------------------------
# Sessions and client
# ---------------------------------------------------------------------------

class TestSessions:
    def test_cert_session_has_no_authorization_header(self):
        session = build_session(None, True, cert="/tmp/proxy")
        assert session.cert == "/tmp/proxy"
        assert "Authorization" not in session.headers

    def test_token_session_unchanged(self):
        session = build_session("tok", True)
        assert session.cert is None
        assert session.headers["Authorization"] == "Bearer tok"

    def test_role_session_x509_mode_uses_proxy_for_every_role(self):
        config = {
            "fts": {"ssl_verify": "/etc/grid-security/certificates"},
            "auth": {"mode": "x509", "x509_proxy": "/tmp/proxy"},
            "tokens": {},
        }
        for role in ("fts_submit", "source_read", "dest_write"):
            session = build_role_session(config, role)
            assert session.cert == "/tmp/proxy"
            assert session.verify == "/etc/grid-security/certificates"
            assert "Authorization" not in session.headers

    def test_role_session_defaults_to_token_mode(self):
        config = {"fts": {"ssl_verify": True}, "tokens": {"source_read": "tok-src"}}
        session = build_role_session(config, "source_read")
        assert session.headers["Authorization"] == "Bearer tok-src"

    @pytest.mark.parametrize("status", [401, 403])
    @responses.activate
    def test_cert_session_401_403_raises_proxy_expired(self, status):
        responses.add(responses.GET, ENDPOINT + "/whoami", status=status)
        with pytest.raises(ProxyExpiredError) as exc_info:
            _client("/tmp/proxy").get("/whoami")
        assert isinstance(exc_info.value, TokenExpiredError)
        assert "voms-proxy-init" in str(exc_info.value)

    @responses.activate
    def test_token_session_401_still_plain_token_expired(self):
        responses.add(responses.GET, ENDPOINT + "/whoami", status=401)
        client = FTSClient(ENDPOINT, build_session("tok", True), max_retries=1)
        with pytest.raises(TokenExpiredError) as exc_info:
            client.get("/whoami")
        assert not isinstance(exc_info.value, ProxyExpiredError)


# ---------------------------------------------------------------------------
# Config loader — x509 mode
# ---------------------------------------------------------------------------

def _x509_config_file(tmp_path, proxy, **overrides):
    data = {
        "run": {"test_label": "test_campaign"},
        "fts": {"endpoint": ENDPOINT, "ssl_verify": True},
        "auth": {"mode": "x509", "x509_proxy": proxy},
        "transfer": {
            "source_pfns_file": "sources.txt",
            "dst_prefix": "https://storage.example.org/data",
        },
        "output": {"base_dir": "runs"},
    }
    data.update(overrides)
    path = str(tmp_path / "config.yaml")
    with open(path, "w") as fh:
        yaml.dump(data, fh, default_flow_style=False)
    return path


class TestConfigX509:
    def test_yaml_mode_loads_without_tokens(self, tmp_path):
        proxy = _write_proxy(tmp_path)
        config = load(_x509_config_file(tmp_path, proxy))
        assert config["auth"]["mode"] == "x509"
        assert config["auth"]["x509_proxy"] == proxy
        assert config["auth"]["x509_min_lifetime_s"] == 3600
        assert config["tokens"] == {}

    def test_cli_proxy_forces_x509_mode(self, tmp_path):
        proxy = _write_proxy(tmp_path)
        path = _x509_config_file(tmp_path, None, auth={})
        config = load(path, x509_proxy=proxy)
        assert config["auth"]["mode"] == "x509"
        assert config["auth"]["x509_proxy"] == proxy

    def test_env_proxy_overrides_yaml(self, tmp_path, monkeypatch):
        env_proxy = _write_proxy(tmp_path, name="env_proxy")
        monkeypatch.setenv("X509_USER_PROXY", env_proxy)
        config = load(_x509_config_file(tmp_path, "/yaml/does/not/exist"))
        assert config["auth"]["x509_proxy"] == env_proxy

    def test_missing_proxy_file_raises(self, tmp_path):
        with pytest.raises(ConfigError, match="not found"):
            load(_x509_config_file(tmp_path, str(tmp_path / "absent")))

    def test_yaml_tokens_rejected(self, tmp_path):
        path = _x509_config_file(
            tmp_path, _write_proxy(tmp_path), tokens={"fts_submit": "tok"},
        )
        with pytest.raises(ConfigError, match="mixed"):
            load(path)

    @pytest.mark.parametrize("kwarg", [
        "token", "fts_submit_token", "source_read_token", "dest_write_token",
    ])
    def test_cli_tokens_rejected(self, tmp_path, kwarg):
        path = _x509_config_file(tmp_path, _write_proxy(tmp_path))
        with pytest.raises(ConfigError, match="cannot be combined"):
            load(path, **{kwarg: "tok"})

    def test_oidc_enabled_rejected(self, tmp_path):
        path = _x509_config_file(
            tmp_path, _write_proxy(tmp_path), oidc={"enabled": True},
        )
        with pytest.raises(ConfigError, match="oidc.enabled"):
            load(path)

    def test_env_tokens_ignored_with_warning(self, tmp_path, monkeypatch, caplog):
        monkeypatch.setenv("FTS_TOKEN", "env-tok")
        path = _x509_config_file(tmp_path, _write_proxy(tmp_path))
        with caplog.at_level(logging.WARNING, logger="fts_framework.config.loader"):
            config = load(path)
        assert config["tokens"] == {}
        assert "FTS_TOKEN" in caplog.text
        assert "env-tok" not in caplog.text

    def test_invalid_mode_rejected(self, tmp_path):
        path = _x509_config_file(tmp_path, None, auth={"mode": "kerberos"})
        with pytest.raises(ConfigError, match="auth.mode"):
            load(path, token="tok")

    def test_negative_min_lifetime_rejected(self, tmp_path):
        path = _x509_config_file(
            tmp_path, None,
            auth={"mode": "x509", "x509_proxy": _write_proxy(tmp_path),
                  "x509_min_lifetime_s": -1},
        )
        with pytest.raises(ConfigError, match="x509_min_lifetime_s"):
            load(path)

    def test_token_mode_still_requires_tokens(self, tmp_path):
        path = _x509_config_file(tmp_path, None, auth={"mode": "token"})
        with pytest.raises(ConfigError, match="tokens"):
            load(path)

    def test_identify_sources_reports_proxy(self, tmp_path):
        proxy = _write_proxy(tmp_path)
        sources = identify_token_sources(_x509_config_file(tmp_path, proxy))
        assert sources == {
            role: "x509 proxy ({})".format(proxy)
            for role in ("fts_submit", "source_read", "dest_write")
        }


# ---------------------------------------------------------------------------
# Submission payload — no storage tokens in x509 mode
# ---------------------------------------------------------------------------

class TestPayloadX509:
    def test_no_storage_tokens_in_payload(self, tmp_path):
        config = load(_x509_config_file(tmp_path, _write_proxy(tmp_path)))
        payload = build_payload(
            [("https://src/f1", "https://dst/f1")], {}, config, "run-1", 0, 0,
        )
        entry = payload["files"][0]
        assert "source_tokens" not in entry
        assert "destination_tokens" not in entry


# ---------------------------------------------------------------------------
# run_campaign pre-flight
# ---------------------------------------------------------------------------

def _x509_campaign_config():
    from tests.unit.test_runner import _base_config
    config = _base_config()
    config["tokens"] = {}
    config["auth"] = {"mode": "x509", "x509_proxy": "/tmp/proxy",
                      "x509_min_lifetime_s": 1800}
    return config


class TestRunCampaignPreflight:
    def _install(self, monkeypatch, tmp_path, whoami_response):
        from tests.unit import test_runner
        test_runner._install_run_campaign_mocks(monkeypatch, tmp_path)
        sessions = []

        def fake_build_session(token, ssl_verify, cert=None):
            sessions.append((token, cert))
            return object()

        monkeypatch.setattr("fts_framework.fts.client.build_session", fake_build_session)
        monkeypatch.setattr(
            "fts_framework.fts.client.FTSClient",
            lambda endpoint, session: test_runner._FakeClient([
                whoami_response,  # /whoami
                {"pairs": []},    # /optimizer/current
            ]),
        )
        return sessions

    def test_preflight_runs_and_campaign_completes(self, monkeypatch, tmp_path):
        from fts_framework.runner import run_campaign
        sessions = self._install(monkeypatch, tmp_path, _WHOAMI)
        calls = []
        monkeypatch.setattr(
            x509, "check_proxy_lifetime",
            lambda path, min_s: calls.append(("local", path, min_s)),
        )
        monkeypatch.setattr(
            x509, "check_delegation",
            lambda client, whoami, min_s: calls.append(("dlg", whoami, min_s)),
        )
        snapshot = run_campaign(_x509_campaign_config(), runs_dir=str(tmp_path))
        assert snapshot["threshold_passed"] is True
        assert calls == [("local", "/tmp/proxy", 1800), ("dlg", _WHOAMI, 1800)]
        assert sessions and all(s == (None, "/tmp/proxy") for s in sessions)

    def test_delegation_failure_aborts_before_submission(self, monkeypatch, tmp_path):
        import fts_framework.runner as runner_mod
        self._install(monkeypatch, tmp_path, _WHOAMI)
        monkeypatch.setattr(x509, "check_proxy_lifetime", lambda path, min_s: None)

        def fail(client, whoami, min_s):
            raise X509CredentialError("no delegation")

        monkeypatch.setattr(x509, "check_delegation", fail)
        submitted = []
        monkeypatch.setattr(runner_mod, "_submit_chunks",
                            lambda *a, **k: submitted.append(1) or [])
        with pytest.raises(X509CredentialError):
            runner_mod.run_campaign(_x509_campaign_config(), runs_dir=str(tmp_path))
        assert submitted == []

    def test_whoami_failure_is_fatal_in_x509_mode(self, monkeypatch, tmp_path):
        from fts_framework.runner import run_campaign
        self._install(monkeypatch, tmp_path, RuntimeError("whoami down"))
        monkeypatch.setattr(x509, "check_proxy_lifetime", lambda path, min_s: None)
        with pytest.raises(RuntimeError, match="whoami down"):
            run_campaign(_x509_campaign_config(), runs_dir=str(tmp_path))
