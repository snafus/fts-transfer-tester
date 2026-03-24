"""
fts_framework.fts.client
~~~~~~~~~~~~~~~~~~~~~~~~
Authenticated HTTP client for the FTS3 REST API.

Provides a ``requests.Session`` factory and an ``FTSClient`` wrapper that:

* Adds ``Authorization: Bearer <token>`` to every request.
* Controls SSL verification (system bundle, custom CA path, or disabled).
* Applies exponential-backoff retries for transient HTTP errors (429, 502,
  503, 504) and ``requests`` connection/timeout exceptions.
* Raises ``TokenExpiredError`` on HTTP 401 (token managed by FTS3; the
  framework does not refresh tokens).

All network I/O goes through ``FTSClient.request()``.  Higher-level modules
call ``FTSClient.get()`` and ``FTSClient.post()`` which return parsed JSON or
raise framework exceptions.

Usage::

    from fts_framework.fts.client import build_session, FTSClient
    session = build_session(token, config["fts"]["ssl_verify"])
    client = FTSClient(config["fts"]["endpoint"], session)
    job_list = client.get("/jobs", params={"time_window": 0.1})
"""

import logging
import time

import requests as req_lib

from fts_framework.exceptions import TokenExpiredError

logger = logging.getLogger(__name__)

# Timeout for all FTS3 REST calls in seconds.
_REQUEST_TIMEOUT_S = 30

# HTTP status codes treated as transient failures (eligible for retry).
_TRANSIENT_STATUS_CODES = frozenset([429, 502, 503, 504])

# Default retry parameters for fts_request_with_retry.
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_INITIAL_BACKOFF_S = 5


def build_session(token, ssl_verify):
    # type: (str, object) -> req_lib.Session
    """Create and return a ``requests.Session`` pre-configured for FTS3.

    Args:
        token (str): Bearer token string (opaque; not inspected or logged).
        ssl_verify (bool or str): SSL verification setting.  Pass ``True``
            for the system CA bundle, ``False`` to disable (insecure, warned),
            or a ``str`` path to a custom CA bundle.

    Returns:
        requests.Session: Session with Authorization header and verify set.
    """
    session = req_lib.Session()
    session.headers.update({"Authorization": "Bearer {}".format(token)})
    session.verify = ssl_verify

    if ssl_verify is False:
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except (ImportError, AttributeError):
            pass
        logger.warning(
            "SSL verification DISABLED for this session. "
            "Results must not be used as security-validated production benchmarks."
        )
    elif isinstance(ssl_verify, str):
        logger.info("SSL verification using custom CA bundle: %s", ssl_verify)
    else:
        logger.info("SSL verification enabled (system default CA bundle)")

    return session


def fts_request_with_retry(session, method, url,
                            max_retries=_DEFAULT_MAX_RETRIES,
                            initial_backoff=_DEFAULT_INITIAL_BACKOFF_S,
                            **kwargs):
    # type: (req_lib.Session, str, str, int, int, ...) -> req_lib.Response
    """Execute an HTTP request with exponential-backoff retry.

    Retries on:
    * ``requests.ConnectionError`` or ``requests.Timeout``
    * HTTP status codes in ``_TRANSIENT_STATUS_CODES`` (429, 502, 503, 504)

    Does **not** retry on 4xx client errors (except 429) or 500.

    Args:
        session (requests.Session): Pre-configured session.
        method (str): HTTP method (e.g. ``"GET"``, ``"POST"``).
        url (str): Absolute URL.
        max_retries (int): Total attempts (1 = no retry).
        initial_backoff (int): Seconds to sleep before the second attempt;
            doubles on each subsequent attempt.
        **kwargs: Forwarded to ``session.request()`` (e.g. ``json``, ``params``).

    Returns:
        requests.Response: Response for a non-transient status code.

    Raises:
        requests.HTTPError: When all retries are exhausted on a transient
            HTTP status (429, 502, 503, 504).
        requests.RequestException: On final failed connection/timeout attempt.
    """
    kwargs.setdefault("timeout", _REQUEST_TIMEOUT_S)
    backoff = initial_backoff
    last_exc = None  # type: req_lib.RequestException

    for attempt in range(max_retries):
        try:
            resp = session.request(method, url, **kwargs)
            if resp.status_code not in _TRANSIENT_STATUS_CODES:
                return resp
            # Transient status — log and maybe retry
            logger.warning(
                "Transient HTTP %d on %s %s (attempt %d/%d)",
                resp.status_code, method, url, attempt + 1, max_retries,
            )
            if attempt < max_retries - 1:
                logger.debug("Sleeping %ds before retry", backoff)
                time.sleep(backoff)
                backoff *= 2
        except (req_lib.ConnectionError, req_lib.Timeout) as exc:
            last_exc = exc
            logger.warning(
                "Request error on %s %s (attempt %d/%d): %s",
                method, url, attempt + 1, max_retries, exc,
            )
            if attempt < max_retries - 1:
                logger.debug("Sleeping %ds before retry", backoff)
                time.sleep(backoff)
                backoff *= 2

    # All attempts exhausted.  Re-raise the last connection/timeout exception,
    # or raise HTTPError for the last transient-status response.  Either way
    # the caller receives an exception, never a transient-status response.
    if last_exc is not None:
        raise last_exc
    # Reached only when every attempt returned a transient HTTP status.
    resp.raise_for_status()


class FTSClient(object):
    """Thin wrapper around a ``requests.Session`` for FTS3 REST calls.

    All requests go through ``request()``, which applies the retry wrapper and
    handles the ``401 TokenExpiredError`` check.  Callers receive parsed JSON
    from ``get()`` and ``post()`` or a framework exception.

    Args:
        endpoint (str): FTS3 REST endpoint base URL, e.g.
            ``"https://fts3.example.org:8446"``.  No trailing slash.
        session (requests.Session): Authenticated session from
            ``build_session()``.
        max_retries (int): Passed to ``fts_request_with_retry``.
    """

    def __init__(self, endpoint, session, max_retries=_DEFAULT_MAX_RETRIES):
        # type: (str, req_lib.Session, int) -> None
        self.endpoint = endpoint.rstrip("/")
        self.session = session
        self.max_retries = max_retries

    def _url(self, path):
        # type: (str) -> str
        """Return absolute URL for *path* (must start with '/')."""
        if not path.startswith("/"):
            path = "/" + path
        return self.endpoint + path

    def request(self, method, path, **kwargs):
        # type: (str, str, ...) -> req_lib.Response
        """Execute a retried HTTP request against the FTS3 endpoint.

        Args:
            method (str): HTTP method.
            path (str): API path (e.g. ``"/jobs"``).
            **kwargs: Forwarded to ``fts_request_with_retry``.

        Returns:
            requests.Response

        Raises:
            TokenExpiredError: On HTTP 401.
            requests.RequestException: On unrecoverable connection failure.
        """
        url = self._url(path)
        logger.debug("%s %s", method, url)
        resp = fts_request_with_retry(
            self.session, method, url,
            max_retries=self.max_retries,
            **kwargs
        )
        if resp.status_code == 401:
            raise TokenExpiredError()
        return resp

    def get(self, path, **kwargs):
        # type: (str, ...) -> object
        """GET *path* and return parsed JSON body.

        Args:
            path (str): API path.
            **kwargs: Forwarded to ``session.request()`` (e.g. ``params``).

        Returns:
            Parsed JSON (list or dict).

        Raises:
            TokenExpiredError: On 401.
            requests.RequestException: On connection failure.
        """
        resp = self.request("GET", path, **kwargs)
        resp.raise_for_status()
        logger.debug("GET %s → HTTP %d", path, resp.status_code)
        return resp.json()

    def post(self, path, payload, **kwargs):
        # type: (str, dict, ...) -> req_lib.Response
        """POST *payload* as JSON to *path* and return the raw response.

        The caller inspects the status code; ``post()`` does not raise on
        non-2xx (except 401) so that the submission layer can implement
        500-recovery logic.

        Args:
            path (str): API path.
            payload (dict): Request body, serialised as JSON.
            **kwargs: Additional keyword arguments for ``session.request()``.

        Returns:
            requests.Response

        Raises:
            TokenExpiredError: On 401.
            requests.RequestException: On connection failure.
        """
        resp = self.request("POST", path, json=payload, **kwargs)
        logger.debug("POST %s → HTTP %d", path, resp.status_code)
        return resp
