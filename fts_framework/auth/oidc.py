"""
fts_framework.auth.oidc
~~~~~~~~~~~~~~~~~~~~~~~~
OIDC client-credentials token fetch.

Performs a single POST to a token endpoint using the OAuth2 client_credentials
grant.  Never logs client secrets or returned token values.
"""

import logging

import requests

from fts_framework.exceptions import ConfigError

logger = logging.getLogger(__name__)


def fetch_token(token_endpoint, client_id, client_secret, scope, ssl_verify,
                audience=None):
    # type: (str, str, str, str, object, object) -> str
    """Fetch a bearer token via OAuth2 client_credentials grant.

    Args:
        token_endpoint (str): HTTPS URL of the token endpoint.
        client_id (str): OAuth2 client ID.
        client_secret (str): OAuth2 client secret.
        scope (str): Space-separated scope string.
        ssl_verify (object): Passed directly to requests (True/False/CA path).
        audience (str or None): Optional audience claim to request.

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
    try:
        resp = requests.post(
            token_endpoint,
            data=data,
            verify=ssl_verify,
            timeout=30,
        )
    except requests.exceptions.RequestException as exc:
        raise ConfigError(
            "OIDC token request to {!r} failed: {}".format(token_endpoint, exc)
        )

    if not resp.ok:
        raise ConfigError(
            "OIDC token endpoint {!r} returned HTTP {}: {}".format(
                token_endpoint, resp.status_code, resp.text[:200],
            )
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
