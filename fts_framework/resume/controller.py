"""
fts_framework.resume.controller
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Campaign resume and crash-recovery logic.

On startup, the runner checks whether a ``run_id`` directory already exists.
If it does, ``load()`` is called instead of running a fresh submission loop.

Resume algorithm (§18 in DESIGN.md)
------------------------------------
1. Load ``manifest.json``.
2. Enumerate all payload files in ``submitted_payloads/``.
3. For each payload file:
   - If a matching ``SubjobRecord`` with ``terminal=True`` exists in the
     manifest → skip (already done; harvesting occurs in the main flow).
   - If a matching record exists with ``terminal=False`` → add to the
     re-poll list (poller will re-attach to the in-flight job).
   - If **no** matching record exists (crash between payload persist and
     POST) → scan FTS3 for a pre-existing job, then POST if none found.
4. Return all non-terminal subjobs (original + newly submitted).

Duplicate submission prevention
---------------------------------
Before resubmitting a payload whose subjob is absent from the manifest, the
controller performs a pre-scan:
``GET /jobs?time_window=<scan_window_s>&state_in=SUBMITTED,...``
and matches on ``(job_metadata.run_id, job_metadata.chunk_index,
job_metadata.retry_round)``.  If a match is found the job_id is recovered
without a second POST.

Usage::

    from fts_framework.resume.controller import load, run_exists
    if run_exists(run_id):
        subjobs = load(run_id, fts_client, config)
"""

import json
import logging
import os
import re
import time
from datetime import datetime

import requests as req_lib

from fts_framework.exceptions import SubmissionError, TokenExpiredError
from fts_framework.persistence import store

logger = logging.getLogger(__name__)

# Payload filename pattern: chunk_0000_r0.json
_PAYLOAD_RE = re.compile(r"^chunk_(\d{4})_r(\d+)\.json$")

# States to scan when checking whether a job was created during recovery
_SCAN_STATES = "SUBMITTED,READY,ACTIVE,FINISHED,FAILED,FINISHEDDIRTY"


def run_exists(run_id, runs_dir=store._DEFAULT_RUNS_DIR):
    # type: (str, str) -> bool
    """Return True if *run_id* has a persisted manifest.

    Args:
        run_id (str): Run identifier to check.
        runs_dir (str): Base directory for run outputs.

    Returns:
        bool: ``True`` if ``runs/<run_id>/manifest.json`` exists.
    """
    path = os.path.join(runs_dir, run_id, "manifest.json")
    return os.path.isfile(path)


def load(run_id, fts_client, config, runs_dir=store._DEFAULT_RUNS_DIR):
    # type: (str, object, dict, str) -> list
    """Load a prior run and return subjobs ready for re-polling.

    Terminal subjobs (``terminal=True``) are included in the return value
    but are skipped by the poller; they are present so the caller can
    harvest their file records.

    Args:
        run_id (str): Run identifier to resume.
        fts_client: ``FTSClient`` instance.
        config (dict): Validated framework config dict.
        runs_dir (str): Base directory for run outputs.

    Returns:
        list[dict]: ``SubjobRecord`` dicts.  All non-terminal jobs are
            present; newly resubmitted jobs are appended.

    Raises:
        ResumeError: If ``manifest.json`` is missing or corrupt.
        SubmissionError: If a missing chunk cannot be resubmitted.
        TokenExpiredError: Propagated from ``fts_client``.
    """
    manifest = store.load_manifest(run_id, runs_dir)
    subjobs = manifest.get("subjobs", [])

    logger.info(
        "Resuming run %s: %d subjob(s) in manifest, completed=%s",
        run_id, len(subjobs), manifest.get("completed", False),
    )

    # Index existing subjobs by (chunk_index, retry_round) for O(1) lookup
    existing = {}
    for s in subjobs:
        key = (s.get("chunk_index", -1), s.get("retry_round", 0))
        existing[key] = s

    # Enumerate persisted payloads to find any that were not submitted
    payload_dir = os.path.join(runs_dir, run_id, "submitted_payloads")
    new_subjobs = []

    if os.path.isdir(payload_dir):
        for filename in sorted(os.listdir(payload_dir)):
            m = _PAYLOAD_RE.match(filename)
            if not m:
                continue
            chunk_index = int(m.group(1))
            retry_round = int(m.group(2))
            key = (chunk_index, retry_round)

            if key in existing:
                # Subjob record exists; nothing to resubmit
                continue

            # Payload exists but no matching subjob → crash between persist
            # and POST.  Attempt recovery or resubmission.
            payload_path = os.path.join(payload_dir, filename)
            rel_path = os.path.join("submitted_payloads", filename)
            logger.warning(
                "Payload %s has no matching subjob in manifest — "
                "attempting recovery/resubmission",
                filename,
            )
            subjob = _recover_or_submit(
                fts_client, config, run_id, chunk_index, retry_round,
                payload_path, rel_path,
            )
            new_subjobs.append(subjob)
            existing[key] = subjob

    if new_subjobs:
        store.update_manifest(run_id, new_subjobs, runs_dir)
        logger.info("Resume: resubmitted %d missing chunk(s)", len(new_subjobs))

    # Return terminal + non-terminal; poller skips terminal ones
    return list(existing.values())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _recover_or_submit(fts_client, config, run_id, chunk_index, retry_round,
                       payload_path, rel_path):
    # type: (object, dict, str, int, int, str, str) -> dict
    """Return a SubjobRecord for *chunk_index*/*retry_round*.

    Scans FTS3 first to avoid duplicate submissions, then POSTs the payload
    if no pre-existing job is found.

    Args:
        fts_client: ``FTSClient`` instance.
        config (dict): Validated framework config dict.
        run_id (str): Current run identifier.
        chunk_index (int): Chunk index of the missing subjob.
        retry_round (int): Retry round of the missing subjob.
        payload_path (str): Absolute path to the persisted payload JSON.
        rel_path (str): Relative path stored in ``SubjobRecord.payload_path``.

    Returns:
        dict: New or recovered ``SubjobRecord``.

    Raises:
        SubmissionError: If the job cannot be submitted or recovered.
        TokenExpiredError: Propagated from ``fts_client``.
    """
    scan_window_s = config.get("submission", {}).get("scan_window_s", 300)

    # --- Pre-scan for pre-existing job ---
    job_id = _scan_for_job(fts_client, run_id, chunk_index, retry_round,
                           scan_window_s)
    if job_id is not None:
        logger.info(
            "Recovered job_id=%s for chunk=%d retry_round=%d via pre-scan",
            job_id, chunk_index, retry_round,
        )
        return _make_subjob(job_id, run_id, chunk_index, retry_round,
                            payload_path, rel_path)

    # --- No pre-existing job found — POST the payload ---
    try:
        with open(payload_path, "r") as fh:
            payload = json.load(fh)
    except (IOError, ValueError) as exc:
        raise SubmissionError(
            chunk_index, 0,
            "Cannot load payload {!r}: {}".format(payload_path, exc),
        )

    resp = fts_client.post("/jobs", json=payload)

    if resp.status_code == 200:
        job_id = resp.json().get("job_id", "")
        logger.info(
            "Resume resubmission: chunk=%d retry_round=%d → job_id=%s",
            chunk_index, retry_round, job_id,
        )
        return _make_subjob(job_id, run_id, chunk_index, retry_round,
                            payload_path, rel_path)

    if resp.status_code == 500:
        # POST may have succeeded despite the 500 — do a post-scan
        logger.warning(
            "Resume resubmission: chunk=%d got HTTP 500; scanning for job",
            chunk_index,
        )
        time.sleep(5)
        job_id = _scan_for_job(fts_client, run_id, chunk_index, retry_round,
                                scan_window_s)
        if job_id is not None:
            logger.info(
                "Recovered job_id=%s after 500 for chunk=%d retry_round=%d",
                job_id, chunk_index, retry_round,
            )
            return _make_subjob(job_id, run_id, chunk_index, retry_round,
                                payload_path, rel_path)
        raise SubmissionError(
            chunk_index, 500,
            "HTTP 500 on resubmission and no matching job found in scan",
        )

    raise SubmissionError(
        chunk_index, resp.status_code,
        "Unexpected HTTP {} on resume resubmission".format(resp.status_code),
    )


def _scan_for_job(fts_client, run_id, chunk_index, retry_round, scan_window_s):
    # type: (object, str, int, int, int) -> object
    """Query FTS3 for a job matching the given metadata triple.

    Args:
        fts_client: ``FTSClient`` instance.
        run_id (str): Run identifier embedded in job_metadata.
        chunk_index (int): Chunk index embedded in job_metadata.
        retry_round (int): Retry round embedded in job_metadata.
        scan_window_s (int): How far back to scan (in seconds).

    Returns:
        str or None: ``job_id`` of the best match, or ``None`` if not found.
    """
    scan_window_h = max(1, scan_window_s // 3600 + 1)
    path = "/jobs?time_window={}&state_in={}".format(scan_window_h, _SCAN_STATES)

    try:
        jobs = fts_client.get(path)
    except TokenExpiredError:
        raise
    except (req_lib.RequestException, Exception) as exc:
        logger.warning("Pre-scan GET %s failed: %s — skipping scan", path, exc)
        return None

    if not isinstance(jobs, list):
        return None

    candidates = []
    for job in jobs:
        meta = job.get("job_metadata") or {}
        try:
            ci = int(meta.get("chunk_index", -1))
            rr = int(meta.get("retry_round", -1))
        except (TypeError, ValueError):
            continue
        if (
            str(meta.get("run_id", "")) == str(run_id)
            and ci == chunk_index
            and rr == retry_round
        ):
            candidates.append(job)

    if not candidates:
        return None

    # Take the most-recent job on multiple matches (highest submitted_at)
    candidates.sort(key=lambda j: j.get("submit_time") or "", reverse=True)
    return candidates[0].get("job_id")


def _make_subjob(job_id, run_id, chunk_index, retry_round, payload_path, rel_path):
    # type: (str, str, int, int, str, str) -> dict
    """Build a SubjobRecord dict for a recovered or newly submitted job."""
    return {
        "job_id": job_id,
        "chunk_index": chunk_index,
        "retry_round": retry_round,
        "run_id": run_id,
        "submitted_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "file_count": _payload_file_count(payload_path),
        "status": "",
        "terminal": False,
        "payload_path": rel_path,
    }


def _payload_file_count(payload_path):
    # type: (str) -> int
    """Return the number of files in a persisted payload, or 0 on error."""
    try:
        with open(payload_path, "r") as fh:
            payload = json.load(fh)
        return len(payload.get("files", []))
    except (IOError, ValueError):
        return 0
