"""
fts_framework.fts.canceller
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cancel FTS3 jobs by job ID.

Sends ``DELETE /jobs/{job_id}`` for each ID.  FTS3 returns:

- 200 — cancellation accepted (job may still be running briefly)
- 404 — job not found or already terminal (treated as success)
- Other 4xx/5xx — logged as a warning; cancellation continues

Usage::

    from fts_framework.fts.canceller import cancel_jobs
    results = cancel_jobs(job_ids, fts_client)
"""

import logging

logger = logging.getLogger(__name__)


def cancel_jobs(job_ids, fts_client):
    # type: (list, object) -> list
    """Send DELETE /jobs/{id} for each job_id in *job_ids*.

    Args:
        job_ids (list[str]): FTS3 job IDs to cancel.
        fts_client: ``FTSClient`` instance.

    Returns:
        list[dict]: One audit record per job_id with keys:
            ``job_id`` (str), ``status_code`` (int or None),
            ``cancelled`` (bool), ``error`` (str or None).
    """
    results = []
    for job_id in job_ids:
        record = {"job_id": job_id, "status_code": None, "cancelled": False, "error": None}
        try:
            resp = fts_client.delete("/jobs/{}".format(job_id))
            record["status_code"] = resp.status_code
            if resp.status_code in (200, 204):
                record["cancelled"] = True
                logger.info("Cancelled job %s (HTTP %d)", job_id, resp.status_code)
            elif resp.status_code == 404:
                record["cancelled"] = True
                logger.info("Job %s already terminal or not found (HTTP 404)", job_id)
            else:
                record["error"] = "HTTP {}".format(resp.status_code)
                logger.warning("Cancel %s failed: HTTP %d", job_id, resp.status_code)
        except Exception as exc:
            record["error"] = str(exc)
            logger.warning("Cancel %s failed: %s", job_id, exc)
        results.append(record)
    return results


def _collect_from_runs_dir(runs_dir):
    # type: (str) -> list
    """Scan *runs_dir* for every manifest.json and return non-terminal job IDs."""
    import json
    import os

    if not os.path.isdir(runs_dir):
        logger.warning("runs_dir %s does not exist — no jobs to cancel", runs_dir)
        return []

    job_ids = []
    seen = set()
    for entry in sorted(os.listdir(runs_dir)):
        manifest_path = os.path.join(runs_dir, entry, "manifest.json")
        if not os.path.exists(manifest_path):
            continue
        try:
            with open(manifest_path) as fh:
                manifest = json.load(fh)
        except Exception as exc:
            logger.warning("Cannot read manifest %s: %s", manifest_path, exc)
            continue
        for subjob in manifest.get("subjobs", []):
            jid = subjob.get("job_id")
            if not jid or jid in seen:
                continue
            seen.add(jid)
            if not subjob.get("terminal", False):
                job_ids.append(jid)
    return job_ids


def collect_job_ids_from_sequence(sequence_dir, runs_dir="runs"):
    # type: (str, str) -> list
    """Walk a sequence directory and return all non-terminal job IDs.

    Reads ``state.json`` to find every trial ``run_id``, then reads each
    run's ``manifest.json`` to collect subjobs that are not yet terminal
    (i.e. still in SUBMITTED state or similar).

    Args:
        sequence_dir (str): Path to the sequence output directory.
        runs_dir (str): Base directory for run outputs.

    Returns:
        list[str]: Deduplicated list of job IDs suitable for cancellation.
            Includes all non-terminal jobs across all trials.
    """
    import json
    import os

    state_path = os.path.join(sequence_dir, "state.json")
    if not os.path.exists(state_path):
        logger.warning(
            "No state.json found in %s — sequence may not have initialised yet; "
            "falling back to scanning runs_dir: %s",
            sequence_dir, runs_dir,
        )
        return _collect_from_runs_dir(runs_dir)

    with open(state_path) as fh:
        state = json.load(fh)

    # Use the runs_dir recorded in the state if available
    stored_runs_dir = state.get("runs_dir", runs_dir)

    job_ids = []
    seen = set()

    for case in state.get("cases", []):
        for trial in case.get("trials", []):
            run_id = trial.get("run_id")
            if not run_id:
                continue
            manifest_path = os.path.join(stored_runs_dir, run_id, "manifest.json")
            if not os.path.exists(manifest_path):
                logger.debug("No manifest for run %s — skipping", run_id)
                continue
            with open(manifest_path) as fh:
                manifest = json.load(fh)
            for subjob in manifest.get("subjobs", []):
                jid = subjob.get("job_id")
                if not jid or jid in seen:
                    continue
                seen.add(jid)
                if not subjob.get("terminal", False):
                    job_ids.append(jid)

    return job_ids
