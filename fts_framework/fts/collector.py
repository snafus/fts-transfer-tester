"""
fts_framework.fts.collector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
REST harvesting of per-file, per-retry, and data-management records.

All metric computation is performed downstream by ``metrics.engine``; this
module only fetches, normalises into canonical dicts, and returns the records.

Data hierarchy
--------------
For each terminal ``SubjobRecord``:

1. ``GET /jobs/{id}/files`` — one ``FileRecord`` per transfer pair.  This is
   the **authoritative** source for all metrics.
2. ``GET /jobs/{id}/files/{fid}/retries`` — zero or more ``RetryRecord``
   dicts per file.
3. ``GET /jobs/{id}/dm`` — one ``DMRecord`` per data-management operation
   (may be empty).

``STAGING_UNSUPPORTED`` jobs are harvested normally; file records will carry
``file_state="STAGING"`` which the metrics engine counts as failed.

Usage::

    from fts_framework.fts.collector import harvest_all
    file_records, retry_records, dm_records = harvest_all(subjobs, fts_client)
"""

import logging

import requests as req_lib

from fts_framework.exceptions import TokenExpiredError
from fts_framework.persistence import store

logger = logging.getLogger(__name__)


def harvest_all(subjobs, fts_client, run_id=None, runs_dir=None):
    # type: (list, object, object, object) -> tuple
    """Harvest file, retry, and DM records for all terminal *subjobs*.

    Non-terminal subjobs (``terminal=False``) are skipped with a warning.

    Args:
        subjobs (list[dict]): ``SubjobRecord`` dicts; at minimum each must
            have ``job_id``, ``chunk_index``, ``retry_round``, and
            ``terminal``.
        fts_client: ``FTSClient`` instance.
        run_id (str): Run identifier; when provided, raw REST responses are
            written to ``raw/files/``, ``raw/retries/``, and ``raw/dm/``
            before processing (raw-data-first invariant).
        runs_dir (str): Base directory for run outputs.

    Returns:
        tuple: ``(file_records, retry_records, dm_records)`` where each
            element is a flat ``list`` of dicts.

    Raises:
        TokenExpiredError: Propagated from ``fts_client.get()``.
        requests.HTTPError: Propagated on unrecoverable HTTP error.
        requests.RequestException: Propagated on connection failure.
    """
    all_file_records = []
    all_retry_records = []
    all_dm_records = []

    for subjob in subjobs:
        if not subjob.get("terminal", False):
            logger.warning(
                "harvest_all: skipping non-terminal job %s (chunk=%d)",
                subjob.get("job_id"), subjob.get("chunk_index", -1),
            )
            continue

        job_id = subjob["job_id"]
        chunk_index = subjob.get("chunk_index", 0)
        retry_round = subjob.get("retry_round", 0)

        logger.debug(
            "Harvesting job %s (chunk=%d, retry_round=%d, status=%s)",
            job_id, chunk_index, retry_round, subjob.get("status", "?"),
        )

        # -------------------------------------------------------------------
        # 1. File records (authoritative for all metrics)
        # -------------------------------------------------------------------
        file_records = _harvest_files(
            fts_client, job_id, chunk_index, retry_round, run_id, runs_dir,
        )
        all_file_records.extend(file_records)

        # -------------------------------------------------------------------
        # 2. Retry records (one request per file that has retries)
        # -------------------------------------------------------------------
        for fr in file_records:
            file_id = fr["file_id"]
            retries = _harvest_retries(fts_client, job_id, file_id, run_id, runs_dir)
            all_retry_records.extend(retries)

        # -------------------------------------------------------------------
        # 3. Data-management records
        # -------------------------------------------------------------------
        dm_records = _harvest_dm(fts_client, job_id, run_id, runs_dir)
        all_dm_records.extend(dm_records)

    logger.info(
        "Harvest complete: %d file records, %d retry records, %d DM records",
        len(all_file_records), len(all_retry_records), len(all_dm_records),
    )
    return all_file_records, all_retry_records, all_dm_records


def _harvest_files(fts_client, job_id, chunk_index, retry_round,
                   run_id=None, runs_dir=None):
    # type: (object, str, int, int, object, object) -> list
    """Fetch and normalise file records for *job_id*.

    Args:
        fts_client: ``FTSClient`` instance.
        job_id (str): FTS3 job ID.
        chunk_index (int): Framework chunk index for provenance.
        retry_round (int): Framework retry round for provenance.
        run_id (str): When provided, raw response is written to disk first.
        runs_dir (str): Base directory for run outputs.

    Returns:
        list[dict]: Normalised ``FileRecord`` dicts.
    """
    raw = fts_client.get("/jobs/{}/files".format(job_id))

    if run_id:
        store.write_raw(run_id, "files", job_id + ".json", raw,
                        runs_dir=runs_dir or store._DEFAULT_RUNS_DIR)

    if not isinstance(raw, list):
        logger.error(
            "GET /jobs/%s/files returned unexpected type %s — treating as empty",
            job_id, type(raw).__name__,
        )
        return []

    records = []
    for item in raw:
        record = _normalise_file_record(item, job_id, chunk_index, retry_round)
        records.append(record)

    logger.debug("Harvested %d file records for job %s", len(records), job_id)
    return records


def _normalise_file_record(item, job_id, chunk_index, retry_round):
    # type: (dict, str, int, int) -> dict
    """Build a canonical ``FileRecord`` dict from a raw FTS3 file entry.

    Missing numeric fields default to 0 / 0.0 rather than ``None`` so that
    the metrics engine can perform arithmetic unconditionally.  Missing string
    fields default to ``""``.

    Args:
        item (dict): Raw dict from the FTS3 ``/jobs/{id}/files`` response.
        job_id (str): Owning job ID.
        chunk_index (int): Framework chunk index.
        retry_round (int): Framework retry round.

    Returns:
        dict: Normalised ``FileRecord``.
    """
    return {
        # Identity
        "job_id": job_id,
        "file_id": item.get("file_id", 0),
        "chunk_index": chunk_index,
        "retry_round": retry_round,

        # Transfer addresses
        "source_surl": item.get("source_surl", ""),
        "dest_surl": item.get("dest_surl", ""),

        # State
        "file_state": item.get("file_state", ""),
        "reason": item.get("reason") or "",

        # Timestamps
        "start_time": item.get("start_time") or "",
        "finish_time": item.get("finish_time") or "",
        "staging_start": None,     # reserved for future tape support
        "staging_finished": None,

        # Transfer metrics (agent-reported)
        "filesize": int(item.get("filesize") or 0),
        "tx_duration": float(item.get("tx_duration") or 0.0),
        # FTS3 agent reports throughput in MiB/s; convert to bytes/s for
        # consistent internal representation.
        "throughput": float(item.get("throughput") or 0.0) * 1048576,

        # Computed metrics (populated by MetricsEngine, not here)
        "throughput_wire": 0.0,
        "throughput_wall": 0.0,
        "wall_duration_s": 0.0,

        # Checksum
        "checksum": item.get("checksum") or "",

        # Metadata
        "job_metadata": item.get("job_metadata") or {},
        "file_metadata": item.get("file_metadata") or {},
    }


def _harvest_retries(fts_client, job_id, file_id, run_id=None, runs_dir=None):
    # type: (object, str, int, object, object) -> list
    """Fetch retry history for a single file.

    Args:
        fts_client: ``FTSClient`` instance.
        job_id (str): FTS3 job ID.
        file_id (int): FTS3 file ID.
        run_id (str): When provided, raw response is written to disk first.
        runs_dir (str): Base directory for run outputs.

    Returns:
        list[dict]: ``RetryRecord`` dicts (empty list if no retries).
    """
    raw = fts_client.get("/jobs/{}/files/{}/retries".format(job_id, file_id))

    if run_id:
        filename = "{}_{}.json".format(job_id, file_id)
        store.write_raw(run_id, "retries", filename, raw,
                        runs_dir=runs_dir or store._DEFAULT_RUNS_DIR)

    if not isinstance(raw, list):
        logger.debug(
            "No retry records for job %s file %s (type=%s)",
            job_id, file_id, type(raw).__name__,
        )
        return []

    records = []
    for item in raw:
        record = {
            "job_id": job_id,
            "file_id": file_id,
            "attempt": int(item.get("attempt") or 0),
            "datetime": item.get("datetime") or "",
            "reason": item.get("reason") or "",
            "transfer_host": item.get("transfer_host") or "",
        }
        records.append(record)

    return records


def _harvest_dm(fts_client, job_id, run_id=None, runs_dir=None):
    # type: (object, str, object, object) -> list
    """Fetch data-management records for *job_id*.

    Args:
        fts_client: ``FTSClient`` instance.
        job_id (str): FTS3 job ID.
        run_id (str): When provided, raw response is written to disk first.
        runs_dir (str): Base directory for run outputs.

    Returns:
        list[dict]: Raw DM record dicts (pass-through; metrics engine is not
            currently defined for DM records).
    """
    try:
        raw = fts_client.get("/jobs/{}/dm".format(job_id))
    except TokenExpiredError:
        raise
    except req_lib.RequestException:
        # DM endpoint may return 404 for non-DM jobs (raises HTTPError from
        # FTSClient.get() → raise_for_status()).  Treat as empty; DM records
        # are supplementary and their absence must not abort the harvest.
        logger.debug("GET /jobs/%s/dm failed — treating DM records as empty", job_id)
        return []

    if run_id:
        store.write_raw(run_id, "dm", job_id + ".json", raw,
                        runs_dir=runs_dir or store._DEFAULT_RUNS_DIR)

    if not isinstance(raw, list):
        return []

    return list(raw)
