"""
fts_framework.fts.poller
~~~~~~~~~~~~~~~~~~~~~~~~
FTS3 job state polling loop.

Polls each active job via ``GET /jobs/{id}`` at exponentially increasing
intervals until every job reaches a terminal state or the campaign timeout
is exceeded.

Terminal states
---------------
``FINISHED``, ``FAILED``, ``FINISHEDDIRTY``, ``CANCELED``

``FINISHEDDIRTY`` (partial success) is treated as terminal and **not** as a
failure.  Per-file outcomes determine metrics; the job state is used only
to decide whether polling continues.

STAGING state
-------------
If a job enters ``STAGING`` state, it is treated as an unsupported error:
- The job is removed from the active set with ``status="STAGING_UNSUPPORTED"``.
- A warning is logged.
- No ``PollingTimeoutError`` or ``SubmissionError`` is raised; the collector
  will mark each file in the job as failed.

Usage::

    from fts_framework.fts.poller import poll_to_completion
    completed_subjobs = poll_to_completion(subjobs, fts_client, config)
"""

import logging
import time

import requests as req_lib

from fts_framework.exceptions import PollingTimeoutError, TokenExpiredError

logger = logging.getLogger(__name__)

# All FTS3 job states that end the polling loop for a given job.
TERMINAL_STATES = frozenset(["FINISHED", "FAILED", "FINISHEDDIRTY", "CANCELED"])

# FTS3 file-level states considered terminal for stuck-ACTIVE detection.
_FILE_TERMINAL_STATES = frozenset(["FINISHED", "FAILED", "CANCELED", "NOT_USED"])


def _derive_state_from_files(fts_client, job_id):
    # type: (object, str) -> object
    """Fetch file records for *job_id* and derive effective terminal state.

    Returns a terminal state string (``"FINISHED"``, ``"FAILED"``,
    ``"CANCELED"``, or ``"FINISHEDDIRTY"``) if all files are in terminal
    states, or ``None`` if any file is still non-terminal or the request
    fails.
    """
    try:
        files = fts_client.get("/jobs/{}/files".format(job_id))
    except Exception as exc:
        logger.warning(
            "stuck-ACTIVE check: GET /jobs/%s/files failed: %s — will retry next check",
            job_id, exc,
        )
        return None

    if not isinstance(files, list) or not files:
        logger.warning(
            "stuck-ACTIVE check: unexpected response type %s for job %s",
            type(files).__name__, job_id,
        )
        return None

    # Count occurrences of each file state for logging.
    state_counts = {}
    for f in files:
        fs = f.get("file_state", "UNKNOWN")
        state_counts[fs] = state_counts.get(fs, 0) + 1

    counts_str = "  ".join(
        "{} {}".format(v, k)
        for k, v in sorted(state_counts.items(), key=lambda x: x[0])
    )
    logger.info(
        "stuck-ACTIVE check job %s: %d files — %s",
        job_id, len(files), counts_str,
    )

    non_terminal = [
        f for f in files
        if f.get("file_state", "") not in _FILE_TERMINAL_STATES
    ]
    if non_terminal:
        return None

    # Derive effective job state from meaningful files (exclude NOT_USED)
    meaningful = [f for f in files if f.get("file_state") != "NOT_USED"]
    if not meaningful:
        return "FINISHED"

    states = set(f.get("file_state") for f in meaningful)
    if states == {"FINISHED"}:
        return "FINISHED"
    if states == {"FAILED"}:
        return "FAILED"
    if states == {"CANCELED"}:
        return "CANCELED"
    return "FINISHEDDIRTY"


def poll_to_completion(subjobs, fts_client, config):
    # type: (list, object, dict) -> list
    """Poll all jobs in *subjobs* until every job reaches a terminal state.

    Jobs already marked ``terminal=True`` in the input are skipped (they were
    completed before this call, e.g. on resume).

    Args:
        subjobs (list[dict]): ``SubjobRecord`` dicts as returned by
            ``submission.submit_all()``.  Each must have at least
            ``job_id``, ``chunk_index``, ``retry_round``, and ``terminal``.
        fts_client: ``FTSClient`` instance.
        config (dict): Validated framework config dict.

    Returns:
        list[dict]: Updated copy of *subjobs* with ``status`` and
            ``terminal`` fields set on every record.

    Raises:
        PollingTimeoutError: If ``campaign_timeout_s`` is exceeded before all
            jobs reach a terminal state.
        TokenExpiredError: Propagated from ``fts_client.get()`` on HTTP 401.
        requests.HTTPError: Propagated on unrecoverable HTTP error.
        requests.RequestException: Propagated on connection failure.
    """
    poll_cfg = config["polling"]
    interval = float(poll_cfg["initial_interval_s"])
    backoff_multiplier = float(poll_cfg["backoff_multiplier"])
    max_interval = float(poll_cfg["max_interval_s"])
    campaign_timeout_s = poll_cfg["campaign_timeout_s"]
    stuck_active_check_rounds = poll_cfg.get("stuck_active_check_rounds", 10)

    deadline = time.time() + campaign_timeout_s

    # Build a mutable index of non-terminal jobs.  We operate on the input
    # dicts directly so the caller sees the updates.
    active = {}
    for subjob in subjobs:
        if not subjob.get("terminal", False):
            active[subjob["job_id"]] = subjob

    if not active:
        logger.info("poll_to_completion: all jobs already terminal; nothing to poll")
        return subjobs

    logger.info(
        "Polling %d job(s) to completion (timeout=%ds, initial_interval=%ds)",
        len(active), campaign_timeout_s, int(interval),
    )

    poll_count = 0
    # Counts consecutive non-terminal poll rounds per job_id (for stuck detection).
    _nonterminal_rounds = {}

    while active:
        if time.time() > deadline:
            raise PollingTimeoutError(list(active.keys()))

        logger.info("Poll round %d: sleeping %ds", poll_count + 1, int(interval))
        time.sleep(interval)
        poll_count += 1

        for job_id in list(active.keys()):
            try:
                job_data = fts_client.get("/jobs/{}".format(job_id))
            except TokenExpiredError:
                raise
            except req_lib.RequestException as exc:
                # Re-raise permanent HTTP errors (non-transient status codes).
                # Swallow only: ConnectionError, Timeout, and HTTPError whose
                # status code is in the transient set (429/502/503/504) —
                # these arise when fts_request_with_retry exhausts its retries
                # on a gateway-level failure while the job has already finished.
                if isinstance(exc, req_lib.HTTPError):
                    status = getattr(
                        getattr(exc, "response", None), "status_code", None
                    )
                    if status not in (429, 502, 503, 504):
                        raise
                logger.warning(
                    "Transient error polling job %s: %s — will retry next round",
                    job_id, exc,
                )
                continue

            if not isinstance(job_data, dict):
                logger.warning(
                    "GET /jobs/%s returned unexpected type %s — skipping this round",
                    job_id, type(job_data).__name__,
                )
                continue

            job_state = job_data.get("job_state", "")
            logger.debug("job_id=%s state=%s (poll %d)", job_id, job_state, poll_count)

            if not job_state:
                logger.warning(
                    "GET /jobs/%s returned no job_state (response keys: %s) — "
                    "treating as non-terminal; will retry next round",
                    job_id, list(job_data.keys()),
                )
                continue

            if job_state in TERMINAL_STATES:
                active[job_id]["status"] = job_state
                active[job_id]["terminal"] = True
                logger.info(
                    "Job %s reached terminal state %s (chunk=%d, retry_round=%d)",
                    job_id,
                    job_state,
                    active[job_id].get("chunk_index", -1),
                    active[job_id].get("retry_round", 0),
                )
                del active[job_id]

            elif job_state == "STAGING":
                logger.warning(
                    "Job %s entered STAGING state — tape staging is unsupported; "
                    "marking as STAGING_UNSUPPORTED and proceeding",
                    job_id,
                )
                active[job_id]["status"] = "STAGING_UNSUPPORTED"
                active[job_id]["terminal"] = True
                del active[job_id]

            else:
                logger.debug("Job %s still in non-terminal state %s", job_id, job_state)
                if stuck_active_check_rounds > 0:
                    _nonterminal_rounds[job_id] = _nonterminal_rounds.get(job_id, 0) + 1
                    if _nonterminal_rounds[job_id] % stuck_active_check_rounds == 0:
                        derived = _derive_state_from_files(fts_client, job_id)
                        if derived is not None:
                            logger.warning(
                                "Job %s stuck in %s but all files terminal "
                                "— using derived state %s (non-terminal rounds: %d)",
                                job_id, job_state, derived,
                                _nonterminal_rounds[job_id],
                            )
                            active[job_id]["status"] = derived
                            active[job_id]["terminal"] = True
                            del active[job_id]

        # Back off for next round, capped at max_interval
        interval = min(interval * backoff_multiplier, max_interval)

    finished_count = sum(1 for s in subjobs if s.get("status") == "FINISHED")
    dirty_count = sum(1 for s in subjobs if s.get("status") == "FINISHEDDIRTY")
    failed_count = sum(
        1 for s in subjobs
        if s.get("status") in ("FAILED", "CANCELED", "STAGING_UNSUPPORTED")
    )
    logger.info(
        "Polling complete after %d round(s): FINISHED=%d FINISHEDDIRTY=%d "
        "FAILED/CANCELED/STAGING=%d",
        poll_count, finished_count, dirty_count, failed_count,
    )

    return subjobs
