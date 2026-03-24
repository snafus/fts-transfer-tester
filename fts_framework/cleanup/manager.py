"""
fts_framework.cleanup.manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pre- and post-campaign cleanup via WebDAV HTTP DELETE.

Cleanup is performed directly against the destination storage endpoint using a
``requests.Session`` authenticated with the ``dest_write`` token.  FTS3 is not
involved.

Safety model
------------
- HTTP 404 is treated as success (file already absent — idempotent).
- HTTP 200 and 204 are normal success responses from WebDAV DELETE.
- Any other status code (including 5xx) is logged as a WARNING and skipped;
  cleanup failures **never** abort the campaign.
- The destination prefix directory itself is never deleted — only individual
  file URLs derived from the destination mapping.

Pre-cleanup (``before: true``)
    Deletes all destination URLs in the transfer mapping, regardless of
    whether a previous campaign left files there.

Post-cleanup (``after: true``)
    Deletes only destination URLs for files that reached ``file_state =
    "FINISHED"`` in the current campaign.

Both functions return an audit record list.  The caller is responsible for
persisting these to ``runs/<run_id>/cleanup_pre.json`` or
``cleanup_post.json``.

Usage::

    from fts_framework.cleanup.manager import cleanup_pre, cleanup_post
    from fts_framework.fts.client import build_session

    session = build_session(config["tokens"]["dest_write"], config["fts"]["ssl_verify"])
    pre_log = cleanup_pre(mapping, session, config)
    # ... run campaign ...
    post_log = cleanup_post(file_records, session, config)
"""

import logging

import requests as req_lib

logger = logging.getLogger(__name__)

# HTTP status codes that indicate a successful DELETE (idempotent).
_DELETE_OK_CODES = frozenset([200, 204, 404])

# Timeout for individual DELETE requests in seconds.
_DELETE_TIMEOUT_S = 30


def cleanup_pre(mapping, session, config):
    # type: (object, req_lib.Session, dict) -> list
    """Delete all destination URLs before the campaign.

    Deletes every URL in ``mapping.values()`` regardless of their current
    state.  Useful for ensuring a clean slate before a new test run when
    ``preserve_extension`` or ``dst_prefix`` changes might overlap with a
    previous run.

    Args:
        mapping: Source → destination ``OrderedDict`` as returned by
            ``destination.planner.plan()``.  Values are the destination URLs.
        session (requests.Session): Authenticated session with ``dest_write``
            token.
        config (dict): Validated framework config dict (used for logging
            context; not currently used for runtime control within this call).

    Returns:
        list[dict]: Audit records, one per URL attempted.  Each record has:
            ``url``, ``status_code``, ``success`` (bool), ``error``
            (str or None).
    """
    dest_urls = list(mapping.values())
    logger.info("Pre-cleanup: attempting DELETE on %d destination URL(s)", len(dest_urls))
    audit = _delete_urls(dest_urls, session, label="pre-cleanup")
    ok = sum(1 for r in audit if r["success"])
    logger.info("Pre-cleanup complete: %d/%d succeeded", ok, len(dest_urls))
    return audit


def cleanup_post(file_records, session, config):
    # type: (list, req_lib.Session, dict) -> list
    """Delete destination URLs for successfully transferred files.

    Only files with ``file_state == "FINISHED"`` are deleted.  Files that
    failed, were cancelled, or were not used are left untouched.

    Args:
        file_records (list[dict]): ``FileRecord`` dicts as returned by
            ``collector.harvest_all()``.
        session (requests.Session): Authenticated session with ``dest_write``
            token.
        config (dict): Validated framework config dict.

    Returns:
        list[dict]: Audit records (same schema as ``cleanup_pre``).
    """
    dest_urls = [
        fr["dest_surl"]
        for fr in file_records
        if fr.get("file_state") == "FINISHED" and fr.get("dest_surl")
    ]
    logger.info(
        "Post-cleanup: %d FINISHED file(s) to delete (of %d total file records)",
        len(dest_urls), len(file_records),
    )
    if not dest_urls:
        return []
    audit = _delete_urls(dest_urls, session, label="post-cleanup")
    ok = sum(1 for r in audit if r["success"])
    logger.info("Post-cleanup complete: %d/%d succeeded", ok, len(dest_urls))
    return audit


def _delete_urls(urls, session, label="cleanup"):
    # type: (list, req_lib.Session, str) -> list
    """Issue HTTP DELETE for each URL in *urls*, logging results.

    Args:
        urls (list[str]): Destination URLs to delete.
        session (requests.Session): Authenticated session.
        label (str): Label used in log messages (e.g. ``"pre-cleanup"``).

    Returns:
        list[dict]: One audit record per URL with keys ``url``,
            ``status_code`` (int or None on connection failure), ``success``
            (bool), and ``error`` (str or None).
    """
    audit = []
    for url in urls:
        record = _delete_one(url, session, label)
        audit.append(record)
    return audit


def _delete_one(url, session, label="cleanup"):
    # type: (str, req_lib.Session, str) -> dict
    """Issue a single HTTP DELETE and return an audit record.

    Args:
        url (str): Destination URL.
        session (requests.Session): Authenticated session.
        label (str): Context label for log messages.

    Returns:
        dict: Audit record with ``url``, ``status_code``, ``success``,
            ``error``.
    """
    try:
        http_url = url.replace("davs://", "https://", 1)
        resp = session.delete(http_url, timeout=_DELETE_TIMEOUT_S)
        status_code = resp.status_code
        if status_code in _DELETE_OK_CODES:
            if status_code == 404:
                logger.debug("[%s] DELETE %s → 404 (already absent, OK)", label, url)
            else:
                logger.debug("[%s] DELETE %s → %d OK", label, url, status_code)
            return {"url": url, "status_code": status_code, "success": True, "error": None}
        else:
            logger.warning(
                "[%s] DELETE %s → unexpected HTTP %d — skipping (non-fatal)",
                label, url, status_code,
            )
            return {
                "url": url,
                "status_code": status_code,
                "success": False,
                "error": "HTTP {}".format(status_code),
            }
    except req_lib.RequestException as exc:
        logger.warning(
            "[%s] DELETE %s → connection error: %s — skipping (non-fatal)",
            label, url, exc,
        )
        return {"url": url, "status_code": None, "success": False, "error": str(exc)}
