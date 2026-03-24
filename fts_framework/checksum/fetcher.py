"""
fts_framework.checksum.fetcher
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pre-submission ADLER32 checksum acquisition via WebDAV ``Want-Digest``.

Issues a ``HEAD`` request with ``Want-Digest: adler32`` against each source
PFN and parses the ``Digest`` response header.  Requests are executed in
parallel using a ``ThreadPoolExecutor`` sized by
``config["concurrency"]["want_digest_workers"]``.

Response format handling
------------------------
RFC 3230 specifies the ``Digest`` header value as base64-encoded.  However,
many grid/HEP storage implementations (StoRM WebDAV, dCache, EOS) return the
ADLER32 digest as an 8-character hexadecimal string instead.  This module
detects the format automatically:

1. Attempt to parse as hex (exactly 8 hex characters).
2. Fall back to base64 decode → hex encode.
3. If neither succeeds, raise ``ChecksumFetchError``.

The returned value is always ``"adler32:<8-char-lowercase-hex>"``.

Failure handling
----------------
If any PFN's checksum cannot be fetched, ``ChecksumFetchError`` is raised.
Pending (not yet started) futures are cancelled; in-progress worker threads
are allowed to finish naturally since Python's ``concurrent.futures`` provides
no preemptive cancellation for running threads.

Usage::

    from fts_framework.checksum.fetcher import fetch_all
    checksums = fetch_all(pfns, session, config)
"""

import base64
import binascii
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as req_lib

from fts_framework.exceptions import ChecksumFetchError

logger = logging.getLogger(__name__)

# Prefix used in the Digest header value (case-insensitive per RFC 3230)
_DIGEST_PREFIX = "adler32="

# HEAD request timeout in seconds.  Increase for slow or tape-backed sources.
_HEAD_TIMEOUT_S = 30


def fetch_all(pfns, session, config):
    # type: (list, req_lib.Session, dict) -> dict
    """Fetch ADLER32 checksums for all *pfns* in parallel.

    Args:
        pfns (list[str]): Source PFNs to checksum.  An empty list returns an
            empty dict without making any network calls.
        session (requests.Session): Authenticated session configured with the
            source-read token and SSL settings.
        config (dict): Validated framework config dict.

    Returns:
        dict[str, str]: Mapping of PFN → ``"adler32:<hex>"``.

    Raises:
        ChecksumFetchError: If any PFN's checksum cannot be fetched.  Pending
            futures are cancelled on first failure; the exception is re-raised
            after the executor has been shut down.
    """
    if not pfns:
        logger.debug("fetch_all called with empty PFN list; returning empty dict")
        return {}

    workers = config["concurrency"]["want_digest_workers"]
    results = {}    # type: dict

    logger.info(
        "Fetching ADLER32 checksums for %d PFNs (workers=%d)", len(pfns), workers
    )

    executor = ThreadPoolExecutor(max_workers=workers)
    future_to_pfn = {
        executor.submit(_fetch_one, pfn, session): pfn for pfn in pfns
    }

    try:
        for future in as_completed(future_to_pfn):
            pfn = future_to_pfn[future]
            checksum = future.result()   # re-raises ChecksumFetchError from worker
            results[pfn] = checksum
    except ChecksumFetchError:
        # Cancel futures that have not yet started
        for f in future_to_pfn:
            f.cancel()
        # Shut down without waiting; in-progress threads complete naturally
        executor.shutdown(wait=False)
        raise
    else:
        executor.shutdown(wait=True)

    logger.info("Checksum fetch complete: %d/%d successful", len(results), len(pfns))
    return results


def _fetch_one(pfn, session):
    # type: (str, req_lib.Session) -> str
    """Fetch the ADLER32 checksum for a single *pfn*.

    Args:
        pfn (str): Source PFN.
        session (requests.Session): Authenticated session.

    Returns:
        str: ``"adler32:<8-char-lowercase-hex>"``.

    Raises:
        ChecksumFetchError: On HTTP error, missing/malformed Digest header,
            or unparseable digest value.
    """
    try:
        response = session.head(
            pfn,
            headers={"Want-Digest": "adler32"},
            timeout=_HEAD_TIMEOUT_S,
        )
    except req_lib.RequestException as exc:
        raise ChecksumFetchError(pfn, "HEAD request failed: {}".format(exc))

    if response.status_code == 404:
        raise ChecksumFetchError(pfn, "Source file not found (HTTP 404)")

    if not response.ok:
        raise ChecksumFetchError(
            pfn,
            "HEAD returned HTTP {} {}".format(response.status_code, response.reason),
        )

    digest_header = response.headers.get("Digest", "")
    if not digest_header:
        raise ChecksumFetchError(
            pfn, "Server returned no Digest header in HEAD response"
        )

    hex_value = _parse_digest_header(pfn, digest_header)
    checksum = "adler32:{}".format(hex_value)
    logger.debug("Checksum for %s: %s", pfn, checksum)
    return checksum


def _parse_digest_header(pfn, header_value):
    # type: (str, str) -> str
    """Parse the ``Digest`` header value and return the ADLER32 hex string.

    Handles both hex and base64 encoded values from storage implementations.
    The ``Digest`` header may contain multiple algorithms separated by commas
    (e.g. ``"sha256=AAA, adler32=a1b2c3d4"``); only the adler32 entry is used.

    Prefix matching is case-insensitive (RFC 3230).  The slice to extract the
    value uses the original-case string at the same byte offset as the
    lowercase prefix (safe because all ASCII case variants of ``"adler32="``
    have the same length of 8 characters).

    Args:
        pfn (str): Source PFN (used in error messages only).
        header_value (str): Raw value of the ``Digest`` response header.

    Returns:
        str: 8-character lowercase hexadecimal ADLER32 digest.

    Raises:
        ChecksumFetchError: If the header cannot be parsed as a valid
            ADLER32 digest in either hex or base64 format.
    """
    prefix_len = len(_DIGEST_PREFIX)   # always 8 for "adler32="

    adler_part = None
    for part in header_value.split(","):
        stripped = part.strip()
        if stripped.lower().startswith(_DIGEST_PREFIX):
            # Slice from the original string at the prefix length offset.
            # Safe: all case variants of "adler32=" are 8 ASCII characters.
            adler_part = stripped[prefix_len:]
            break

    if adler_part is None:
        raise ChecksumFetchError(
            pfn,
            "Digest header does not contain an adler32 value: {!r}".format(header_value),
        )

    raw = adler_part.strip()

    # --- Attempt 1: hex (exactly 8 hex characters) ---
    if _is_hex_adler32(raw):
        return raw.lower()

    # --- Attempt 2: base64 → bytes → hex ---
    hex_value = _base64_to_hex(raw)
    if hex_value is not None:
        return hex_value

    raise ChecksumFetchError(
        pfn,
        "Cannot parse adler32 digest value {!r} as hex or base64. "
        "Expected 8 hex characters or a base64-encoded 4-byte value.".format(raw),
    )


def _is_hex_adler32(value):
    # type: (str) -> bool
    """Return True if *value* is a valid 8-character hexadecimal ADLER32 string."""
    if len(value) != 8:
        return False
    try:
        int(value, 16)
        return True
    except ValueError:
        return False


def _base64_to_hex(value):
    # type: (str) -> object
    """Decode *value* as base64 and return the 8-character hex string, or None.

    RFC 3230 encodes a 4-byte ADLER32 as 6 base64 characters (with ``==``
    padding to 8).  Servers may return with or without padding.

    Padding is normalised by stripping any trailing ``=`` characters and
    adding exactly the right amount (``-len % 4``), avoiding the corruption
    that would result from blindly appending ``"=="`` to an already-padded
    value.

    Returns ``None`` if decoding fails or the decoded byte length is not 4.
    """
    try:
        # Normalise padding: strip existing padding, compute correct amount
        stripped = value.rstrip("=")
        padding = "=" * (-len(stripped) % 4)
        decoded = base64.b64decode(stripped + padding)
    except (binascii.Error, ValueError):
        return None

    if len(decoded) != 4:
        return None

    return binascii.hexlify(decoded).decode("ascii")
