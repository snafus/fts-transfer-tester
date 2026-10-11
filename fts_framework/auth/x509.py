"""
fts_framework.auth.x509
~~~~~~~~~~~~~~~~~~~~~~~~
X.509 / VOMS proxy credential support (``auth.mode: x509``).

The framework does not create proxies or perform FTS3 delegation.  The user
runs ``voms-proxy-init`` and delegates to FTS3 out of band (e.g.
``fts-rest-delegate``) before starting a campaign.  This module:

* Resolves which proxy file to present as the TLS client certificate.
* Performs a light structural check on the proxy file.
* Checks the remaining lifetime of the local proxy and of the credential
  delegated to FTS3 (``GET /delegation/{delegation_id}``), failing fast if
  either is below ``auth.x509_min_lifetime_s``.

The proxy's private key is never read into a log message or exception.
"""

import logging
import os
import ssl
from datetime import datetime

from fts_framework.exceptions import ConfigError, X509CredentialError

logger = logging.getLogger(__name__)

_ENV_PROXY = "X509_USER_PROXY"

_TERMINATION_TIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
)


def default_proxy_path():
    # type: () -> str
    """Return the conventional proxy location ``/tmp/x509up_u<uid>``."""
    return "/tmp/x509up_u{}".format(os.getuid())


def resolve_proxy_path(cli_path=None, yaml_path=None):
    # type: (object, object) -> str
    """Return the proxy file path to use.

    Priority (highest wins): *cli_path*, ``$X509_USER_PROXY``, *yaml_path*,
    ``/tmp/x509up_u<uid>``.
    """
    if cli_path:
        return cli_path
    env_path = os.environ.get(_ENV_PROXY)
    if env_path:
        return env_path
    if yaml_path:
        return yaml_path
    return default_proxy_path()


def validate_proxy_file(path):
    # type: (str) -> None
    """Check that *path* is a readable PEM file holding a cert and a key.

    Raises:
        ConfigError: If the file is missing, unreadable, or lacks either a
            certificate or a private key block.
    """
    if not os.path.isfile(path):
        raise ConfigError(
            "X.509 proxy file not found: {!r}. Run voms-proxy-init or set "
            "auth.x509_proxy / X509_USER_PROXY.".format(path)
        )
    try:
        with open(path, "r") as fh:
            content = fh.read()
    except (IOError, OSError) as exc:
        raise ConfigError("Cannot read X.509 proxy file {!r}: {}".format(path, exc))
    if "-----BEGIN CERTIFICATE-----" not in content:
        raise ConfigError(
            "X.509 proxy file {!r} contains no PEM certificate".format(path)
        )
    if "PRIVATE KEY-----" not in content:
        raise ConfigError(
            "X.509 proxy file {!r} contains no PEM private key; a proxy file "
            "must hold both the proxy certificate and its key".format(path)
        )
    if "ENCRYPTED PRIVATE KEY-----" in content or "Proc-Type: 4,ENCRYPTED" in content:
        raise ConfigError(
            "X.509 file {!r} has an encrypted private key, so it is not a proxy "
            "(proxy keys are never encrypted). Create one with voms-proxy-init "
            "and point at that instead.".format(path)
        )


def proxy_not_after(path):
    # type: (str) -> object
    """Return the ``notAfter`` of the first certificate in *path* (naive UTC).

    Uses the stdlib's ``ssl._ssl._test_decode_cert`` (no ``cryptography``
    dependency).  Returns ``None`` if that helper is unavailable.
    """
    decode = getattr(getattr(ssl, "_ssl", None), "_test_decode_cert", None)
    if decode is None:
        return None
    not_after = decode(path)["notAfter"]
    return datetime.utcfromtimestamp(ssl.cert_time_to_seconds(not_after))


def check_proxy_lifetime(path, min_lifetime_s, now=None):
    # type: (str, int, object) -> None
    """Fail if the local proxy at *path* expires within *min_lifetime_s*.

    Raises:
        X509CredentialError: If remaining lifetime < *min_lifetime_s*.
    """
    not_after = proxy_not_after(path)
    if not_after is None:
        logger.warning(
            "Cannot determine local proxy expiry on this Python build; "
            "skipping local lifetime check"
        )
        return
    now = now or datetime.utcnow()
    remaining = int((not_after - now).total_seconds())
    if remaining < min_lifetime_s:
        raise X509CredentialError(
            "Local X.509 proxy {!r} has {}s remaining (expires {}Z), below "
            "auth.x509_min_lifetime_s={}. Renew with voms-proxy-init.".format(
                path, max(remaining, 0), not_after.isoformat(), min_lifetime_s,
            )
        )
    logger.info(
        "Local X.509 proxy %s valid until %sZ (%ds remaining)",
        path, not_after.isoformat(), remaining,
    )


def _parse_termination_time(value):
    # type: (str) -> datetime
    value = value.rstrip("Z")
    for fmt in _TERMINATION_TIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise X509CredentialError(
        "Cannot parse delegation termination_time {!r}".format(value)
    )


def check_delegation(fts_client, whoami, min_lifetime_s, now=None):
    # type: (object, dict, int, object) -> datetime
    """Verify a delegated credential exists on FTS3 with enough lifetime.

    Args:
        fts_client: ``FTSClient`` authenticated with the proxy.
        whoami (dict): Parsed ``GET /whoami`` response.
        min_lifetime_s (int): Minimum acceptable remaining lifetime.
        now (datetime or None): Override for the current UTC time (tests).

    Returns:
        datetime: Delegation termination time (naive UTC).

    Raises:
        X509CredentialError: If ``delegation_id`` is absent from *whoami*,
            no delegated credential exists, or its remaining lifetime is
            below *min_lifetime_s*.
    """
    dlg_id = (whoami or {}).get("delegation_id")
    if not dlg_id:
        raise X509CredentialError(
            "GET /whoami returned no delegation_id; FTS3 did not recognise "
            "the X.509 proxy as a client credential"
        )

    if not (whoami or {}).get("voms_cred"):
        logger.warning(
            "FTS3 reports no VOMS attributes for this proxy (dn=%s); storage "
            "authorisation may fail", whoami.get("user_dn"),
        )

    resp = fts_client.request("GET", "/delegation/{}".format(dlg_id))
    body = None
    if resp.status_code != 404:
        resp.raise_for_status()
        body = resp.json()
    if not body or not body.get("termination_time"):
        raise X509CredentialError(
            "No delegated credential on FTS3 for delegation_id {}. Delegate "
            "first, e.g.: fts-rest-delegate -s {} --hours 12".format(
                dlg_id, fts_client.endpoint,
            )
        )

    termination = _parse_termination_time(body["termination_time"])
    now = now or datetime.utcnow()
    remaining = int((termination - now).total_seconds())
    if remaining < min_lifetime_s:
        raise X509CredentialError(
            "Delegated credential {} on FTS3 has {}s remaining (expires {}Z), "
            "below auth.x509_min_lifetime_s={}. Re-delegate, e.g.: "
            "fts-rest-delegate -f -s {} --hours 12".format(
                dlg_id, max(remaining, 0), termination.isoformat(),
                min_lifetime_s, fts_client.endpoint,
            )
        )
    logger.info(
        "FTS3 delegated credential %s valid until %sZ (%ds remaining)",
        dlg_id, termination.isoformat(), remaining,
    )
    return termination
