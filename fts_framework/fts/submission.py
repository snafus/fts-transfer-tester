"""
fts_framework.fts.submission
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
FTS3 job submission: chunking, payload construction, and 500-recovery.

Responsibilities
----------------
* ``chunk()``          — split an ``OrderedDict`` mapping into equal-sized
                         sub-dicts (last chunk may be smaller).
* ``build_payload()``  — construct the FTS3 ``POST /jobs`` request body for
                         one chunk, including checksums, priorities, activity,
                         and ``job_metadata``.
* ``submit_with_500_recovery()`` — submit one chunk and, on HTTP 500, scan
                         ``GET /jobs`` for an already-persisted job matching
                         ``(run_id, chunk_index, retry_round)``.
500-recovery rationale
----------------------
FTS3 may persist a job internally but return HTTP 500 with no ``job_id``.
Blind resubmission would create duplicate jobs.  The recovery scan queries
``GET /jobs`` with a ``time_window`` parameter and matches on
``job_metadata.run_id``, ``job_metadata.chunk_index``, and
``job_metadata.retry_round`` — a triple guaranteed unique per submission
attempt.

Usage::

    from fts_framework.fts.submission import submit_with_500_recovery
    job_id = submit_with_500_recovery(fts_client, payload, config, run_id, 0, 0)
"""

import logging
import time
from collections import OrderedDict

from fts_framework.exceptions import SubmissionError

logger = logging.getLogger(__name__)

# Framework metadata keys reserved in job_metadata.  User-supplied
# job_metadata keys must not collide with these.
_FRAMEWORK_METADATA_KEYS = frozenset([
    "run_id", "chunk_index", "retry_round", "test_label",
])

# Seconds to wait after a 500 before scanning for the job.
_POST_500_SETTLE_S = 5

# Maximum chunk size enforced by FTS3.
_MAX_CHUNK_SIZE = 200


def chunk(items, size=_MAX_CHUNK_SIZE):
    # type: (OrderedDict, int) -> list
    """Split *items* into a list of ``OrderedDict`` chunks of at most *size*.

    Iteration order is preserved.  The last chunk may be smaller than *size*.

    Args:
        items (OrderedDict): Source→destination mapping as returned by
            ``destination.planner.plan()``.
        size (int): Maximum entries per chunk.  Must be >= 1.

    Returns:
        list[OrderedDict]: Non-empty list of ``OrderedDict`` chunks.

    Raises:
        ValueError: If *size* < 1 or *items* is empty.
    """
    if size < 1:
        raise ValueError("chunk size must be >= 1, got {}".format(size))
    if not items:
        raise ValueError("items must not be empty")

    keys = list(items.keys())
    chunks = []
    for i in range(0, len(keys), size):
        batch_keys = keys[i:i + size]
        batch = OrderedDict((k, items[k]) for k in batch_keys)
        chunks.append(batch)
    return chunks


def build_payload(chunk_mapping, checksums, config, run_id, chunk_index, retry_round):
    # type: (OrderedDict, dict, dict, str, int, int) -> dict
    """Build the FTS3 ``POST /jobs`` request body for one chunk.

    Constructs the ``files`` list (one entry per source→destination pair) and
    attaches framework metadata, priorities, and FTS3 transfer parameters.

    Args:
        chunk_mapping (OrderedDict): Source PFN → destination URL for this
            chunk only.
        checksums (dict): PFN → ``"adler32:<hex>"`` for all source PFNs.
            Only entries present in *chunk_mapping* are used.
        config (dict): Validated framework config dict.
        run_id (str): Unique run identifier (UUID or similar).
        chunk_index (int): Zero-based index of this chunk.
        retry_round (int): 0 for initial submission; ≥ 1 for framework retries.

    Returns:
        dict: FTS3 job payload ready for ``json=`` serialisation.
    """
    transfer_cfg = config["transfer"]
    retry_cfg = config.get("retry", {})

    checksum_algo = transfer_cfg.get("checksum_algorithm", "adler32")
    verify_checksum = transfer_cfg.get("verify_checksum", "both")

    files = []
    for src, dst in chunk_mapping.items():
        entry = {
            "sources": [src],
            "destinations": [dst],
        }
        if checksum_algo == "adler32" and src in checksums:
            # checksums[src] is "adler32:<hex>"; FTS3 expects this
            # "ALGO:VALUE" format in the checksum field.
            entry["checksum"] = checksums[src]
        files.append(entry)

    payload = {
        "files": files,
        "params": {
            "verify_checksum": verify_checksum,
            "retry": retry_cfg.get("fts_retry_max", 2),
            "priority": transfer_cfg.get("priority", 3),
            "job_metadata": _build_job_metadata(config, run_id, chunk_index, retry_round),
        },
    }

    # Optional FTS3 activity string — empty string "default" is included;
    # None means absent from config and is omitted from the payload.
    activity = transfer_cfg.get("activity")
    if activity:
        payload["params"]["activity"] = activity

    # Optional overwrite
    overwrite = transfer_cfg.get("overwrite", False)
    if overwrite:
        payload["params"]["overwrite"] = True

    # unmanaged_tokens=True prevents FTS3 from registering these tokens with its
    # lifecycle manager — without it FTS3 still attempts token exchange/refresh.
    if transfer_cfg.get("storage_tokens", False):
        payload["params"]["source_token"] = config["tokens"]["source_read"]
        payload["params"]["destination_token"] = config["tokens"]["dest_write"]
        payload["params"]["unmanaged_tokens"] = True

    logger.debug(
        "Built payload for chunk %d (retry_round=%d): %d files",
        chunk_index, retry_round, len(files),
    )
    return payload


def _build_job_metadata(config, run_id, chunk_index, retry_round):
    # type: (dict, str, int, int) -> dict
    """Return the ``job_metadata`` dict for a chunk submission.

    Framework keys (``run_id``, ``chunk_index``, ``retry_round``,
    ``test_label``) are always present.  User-supplied
    ``config["transfer"]["job_metadata"]`` values are merged in, with framework
    keys taking priority.

    Collisions between user-supplied keys and framework-reserved keys are
    logged as warnings; the framework value is used.

    Args:
        config (dict): Validated framework config dict.
        run_id (str): Unique run identifier.
        chunk_index (int): Zero-based chunk index.
        retry_round (int): Submission round (0 = initial).

    Returns:
        dict: Merged ``job_metadata`` dict.
    """
    user_meta = config["transfer"].get("job_metadata", {}) or {}

    # Warn about collisions
    collisions = _FRAMEWORK_METADATA_KEYS.intersection(user_meta.keys())
    if collisions:
        logger.warning(
            "User job_metadata keys %s collide with framework-reserved keys "
            "and will be overridden.", sorted(collisions)
        )

    metadata = {}
    metadata.update(user_meta)
    # Framework keys always win
    metadata["run_id"] = run_id
    metadata["chunk_index"] = chunk_index
    metadata["retry_round"] = retry_round
    metadata["test_label"] = config["run"]["test_label"]

    return metadata


def submit_with_500_recovery(fts_client, payload, config, run_id, chunk_index, retry_round):
    # type: (object, dict, dict, str, int, int) -> str
    """Submit one chunk payload and return the FTS3 job_id.

    On HTTP 200, the ``job_id`` is extracted from the response JSON.

    On HTTP 500, a recovery scan is performed:

    1. Wait ``_POST_500_SETTLE_S`` seconds for the FTS3 DB to settle.
    2. Query ``GET /jobs`` with a ``time_window`` (hours) covering the
       configured ``scan_window_s``.
    3. Match jobs whose ``job_metadata`` contains
       ``{"run_id": run_id, "chunk_index": chunk_index,
       "retry_round": retry_round}``.
    4. If exactly one match: use it.  If multiple: take the most recently
       submitted.  If zero: raise ``SubmissionError``.

    All other non-2xx responses raise ``SubmissionError`` immediately without
    scanning.

    Args:
        fts_client: ``FTSClient`` instance.
        payload (dict): FTS3 job payload (from ``build_payload()``).
        config (dict): Validated framework config dict.
        run_id (str): Unique run identifier (used for recovery matching).
        chunk_index (int): Zero-based chunk index (used for recovery matching).
        retry_round (int): Submission round (used for recovery matching).

    Returns:
        str: FTS3 job_id string.

    Raises:
        SubmissionError: If submission failed and recovery found no matching job,
            or if a non-500/non-200 HTTP error was returned.
        TokenExpiredError: Propagated from ``fts_client.post()``.
        requests.RequestException: On unrecoverable connection failure.
    """
    response = fts_client.post("/jobs", payload)

    if response.status_code == 200:
        job_id = response.json().get("job_id")
        logger.info(
            "Submitted chunk %d (retry_round=%d) → job_id=%s",
            chunk_index, retry_round, job_id,
        )
        return job_id

    if response.status_code == 500:
        logger.warning(
            "POST /jobs returned 500 for chunk %d (retry_round=%d) "
            "— waiting %ds then scanning for existing job",
            chunk_index, retry_round, _POST_500_SETTLE_S,
        )
        time.sleep(_POST_500_SETTLE_S)

        scan_window_s = config.get("submission", {}).get("scan_window_s", 300)
        # Round up to the nearest whole hour (minimum 1) — FTS3 expects an
        # integer time_window.  Consistent with resume/controller.py.
        scan_window_h = max(1, scan_window_s // 3600 + 1)

        # Build the query string manually: requests percent-encodes comma as
        # %2C which FTS3 does not accept — state_in must use literal commas.
        _states = "SUBMITTED,READY,ACTIVE,FINISHED,FAILED,FINISHEDDIRTY,CANCELED"
        _scan_path = "/jobs?time_window={}&state_in={}".format(
            scan_window_h, _states,
        )
        jobs = fts_client.get(_scan_path)

        if not isinstance(jobs, list):
            logger.error(
                "GET /jobs returned unexpected type %s during 500-recovery scan",
                type(jobs).__name__,
            )
            jobs = []

        matches = [
            j for j in jobs
            if isinstance(j.get("job_metadata"), dict)
            and j["job_metadata"].get("run_id") == run_id
            and j["job_metadata"].get("chunk_index") == chunk_index
            and j["job_metadata"].get("retry_round") == retry_round
        ]

        if len(matches) == 1:
            job_id = matches[0]["job_id"]
            logger.warning(
                "500-recovery: recovered job_id=%s for chunk %d (retry_round=%d)",
                job_id, chunk_index, retry_round,
            )
            return job_id

        if len(matches) > 1:
            matches.sort(key=lambda j: j.get("submit_time", ""), reverse=True)
            job_id = matches[0]["job_id"]
            logger.warning(
                "500-recovery: multiple matches (%d) for chunk %d — "
                "using most recent: %s",
                len(matches), chunk_index, job_id,
            )
            return job_id

        raise SubmissionError(
            chunk_index,
            500,
            "POST /jobs returned 500 and no matching job found in "
            "scan window of {}s".format(scan_window_s),
        )

    raise SubmissionError(
        chunk_index,
        response.status_code,
        "POST /jobs returned HTTP {}: {}".format(
            response.status_code, response.text
        ),
    )


