"""
fts_framework.auth.oidc
~~~~~~~~~~~~~~~~~~~~~~~~
OIDC client-credentials token fetch.

POSTs to a token endpoint using the OAuth2 client_credentials grant, retrying
transient failures (connection errors, timeouts, HTTP 429 and 5xx) with
exponential backoff.  Never logs client secrets or returned token values.
"""

import logging
import time

import requests

from fts_framework.exceptions import ConfigError

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_S = 30

# Retries after the first attempt (0 = no retry) and the wait before the
# first retry.  The wait doubles per retry, capped at _MAX_BACKOFF_S.
DEFAULT_RETRY_MAX = 4
DEFAULT_RETRY_BACKOFF_S = 5
_MAX_BACKOFF_S = 60


def _is_transient_status(status_code):
    # type: (int) -> bool
    # Unlike POST /jobs, a client_credentials POST has no server-side effect
    # worth protecting, so every 5xx (including 500) is safe to retry.
    return status_code == 429 or status_code >= 500


def _post_with_retry(token_endpoint, data, ssl_verify, retry_max,
                     retry_backoff_s):
    # type: (str, dict, object, int, int) -> requests.Response
    """POST *data* to *token_endpoint*, retrying transient failures.

    Returns:
        requests.Response: A response with a 2xx status.

    Raises:
        ConfigError: On a non-transient failure, or when a transient failure
            persists through every attempt.
    """
    attempts = max(retry_max, 0) + 1
    backoff = retry_backoff_s

    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                token_endpoint,
                data=data,
                verify=ssl_verify,
                timeout=_REQUEST_TIMEOUT_S,
            )
        except requests.exceptions.SSLError as exc:
            # Subclass of ConnectionError, but a certificate problem does not
            # fix itself between attempts.
            raise ConfigError(
                "OIDC token request to {!r} failed: {}".format(token_endpoint, exc)
            )
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            reason = str(exc)
            error = "OIDC token request to {!r} failed: {}".format(
                token_endpoint, exc
            )
        except requests.exceptions.RequestException as exc:
            raise ConfigError(
                "OIDC token request to {!r} failed: {}".format(token_endpoint, exc)
            )
        else:
            if resp.ok:
                return resp
            reason = "HTTP {}".format(resp.status_code)
            error = "OIDC token endpoint {!r} returned HTTP {}: {}".format(
                token_endpoint, resp.status_code, resp.text[:200],
            )
            if not _is_transient_status(resp.status_code):
                raise ConfigError(error)

        if attempt == attempts:
            if attempts > 1:
                error = "{} (gave up after {} attempts)".format(error, attempts)
            raise ConfigError(error)

        logger.warning(
            "Transient OIDC token fetch failure from %s (attempt %d/%d): %s "
            "- retrying in %ds",
            token_endpoint, attempt, attempts, reason, backoff,
        )
        time.sleep(backoff)
        backoff = min(backoff * 2, _MAX_BACKOFF_S)


def fetch_token(token_endpoint, client_id, client_secret, scope, ssl_verify,
                audience=None, retry_max=DEFAULT_RETRY_MAX,
                retry_backoff_s=DEFAULT_RETRY_BACKOFF_S):
    # type: (str, str, str, str, object, object, int, int) -> str
    """Fetch a bearer token via OAuth2 client_credentials grant.

    Connection errors, timeouts and HTTP 429/5xx responses are retried up to
    *retry_max* times with exponential backoff.  Other failures (4xx such as
    bad client credentials or a rejected scope, TLS verification errors,
    malformed responses) fail immediately.

    Args:
        token_endpoint (str): HTTPS URL of the token endpoint.
        client_id (str): OAuth2 client ID.
        client_secret (str): OAuth2 client secret.
        scope (str): Space-separated scope string.
        ssl_verify (object): Passed directly to requests (True/False/CA path).
        audience (str or None): Optional audience claim to request.
        retry_max (int): Retries after the first attempt (0 = no retry).
        retry_backoff_s (int): Seconds to wait before the first retry;
            doubles on each subsequent retry, capped at 60s.

    Returns:
        str: The access_token value from the response.

    Raises:
        ConfigError: If the request fails or the response contains no
            ``access_token``.
    """
    logger.debug("Fetching OIDC token from %s scope=%r audience=%r",
                 token_endpoint, scope, audience)
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }
    if audience:
        data["audience"] = audience

    resp = _post_with_retry(
        token_endpoint, data, ssl_verify, retry_max, retry_backoff_s,
    )

    try:
        body = resp.json()
    except ValueError as exc:
        raise ConfigError(
            "OIDC token endpoint {!r} returned non-JSON response: {}".format(
                token_endpoint, exc
            )
        )

    token = body.get("access_token")
    if not token:
        raise ConfigError(
            "OIDC token endpoint {!r} response missing 'access_token'".format(
                token_endpoint
            )
        )

    logger.debug("OIDC token obtained from %s", token_endpoint)
    return token
