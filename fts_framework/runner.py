"""
fts_framework.runner
~~~~~~~~~~~~~~~~~~~~~
Top-level campaign orchestrator.

This module wires all sub-components together in the correct sequence:

1.  Initialise run directory
2.  Validate token (``GET /whoami``)
3.  Log FTS3 link optimizer state (``GET /optimizer/current``)
4.  Detect resume or start fresh run
5.  (Fresh only) Load inventory, plan destinations, fetch checksums,
    write manifest, run pre-cleanup
6.  Submit all chunks (``POST /jobs``), persisting each payload before
    transmission (raw-data-first invariant)
7.  Poll all jobs to terminal state
8.  Persist terminal job states to ``raw/jobs/``
9.  Harvest file, retry, and DM records; persist normalised records
10. Framework retry loop (if ``framework_retry_max > 0``)
11. Compute metrics snapshot
12. Generate all configured reports
13. Run post-cleanup (if ``cleanup.after: true``)
14. Mark run completed in manifest

Entry point::

    fts-run path/to/config.yaml

Programmatic usage::

    from fts_framework.runner import run_campaign
    from fts_framework.config.loader import load
    snapshot = run_campaign(load("config.yaml"))
"""

import argparse
import logging
import sys
import uuid
from collections import OrderedDict
from datetime import datetime

from fts_framework.checksum import fetcher as checksum_fetcher
from fts_framework.exceptions import SubmissionError, TokenExpiredError
from fts_framework.cleanup import manager as cleanup_manager
from fts_framework.config import loader as config_loader
from fts_framework.destination import planner as dest_planner
from fts_framework.fts import client as fts_client_mod
from fts_framework.fts import collector
from fts_framework.fts import poller
from fts_framework.fts.submission import build_payload
from fts_framework.fts.submission import chunk as chunk_mapping
from fts_framework.fts.submission import submit_with_500_recovery
from fts_framework.inventory import loader as inventory_loader
from fts_framework.metrics import engine as metrics_engine
from fts_framework.persistence import store
from fts_framework.reporting import renderer
from fts_framework.resume import controller as resume_controller

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def generate_run_id():
    # type: () -> str
    """Return a unique run ID: ``{YYYYMMDD_HHMMSS}_{8-hex-chars}``.

    Example: ``20260323_143201_a3f7c1b9``
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    return "{}_{}".format(timestamp, short_uuid)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fts_monitor_base(endpoint):
    # type: (str) -> str
    """Derive the FTS WebMonitor base URL from the REST *endpoint*.

    FTS3 REST API is conventionally on port 8446; the WebMonitor on 8449.
    Returns an empty string if port 8446 is not found in the endpoint.
    """
    if ":8446" in endpoint:
        return endpoint.replace(":8446", ":8449") + "/fts3/ftsmon/#/job/"
    return ""


def _log_optimizer_state(fts_client):
    # type: (object) -> None
    """Fetch and log the FTS3 link optimizer state.  Non-fatal on error."""
    try:
        optimizer = fts_client.get("/optimizer/current")
        logger.debug("FTS3 optimizer state at campaign start: %s", optimizer)
    except TokenExpiredError:
        raise
    except Exception as exc:
        logger.warning("Could not fetch /optimizer/current: %s", exc)


def _submit_chunks(mapping, checksums, config, run_id, retry_round,
                   fts_client, runs_dir):
    # type: (OrderedDict, dict, dict, str, int, object, str) -> list
    """Chunk *mapping*, persist each payload, and submit to FTS3.

    Implements the raw-data-first invariant: ``store.write_payload()`` is
    called for every chunk **before** the ``POST /jobs`` request.

    Args:
        mapping (OrderedDict): Source PFN → destination URL for this round.
        checksums (dict): PFN → ``"adler32:<hex>"``.  May be empty on resume.
        config (dict): Validated framework config dict.
        run_id (str): Unique run identifier.
        retry_round (int): 0 for initial submission; ≥1 for framework retries.
        fts_client: ``FTSClient`` instance.
        runs_dir (str): Base directory for run outputs.

    Returns:
        list[dict]: ``SubjobRecord`` dicts, one per chunk.
    """
    monitor_base = _fts_monitor_base(config.get("fts", {}).get("endpoint", ""))
    chunk_size = config.get("transfer", {}).get("chunk_size", 200)
    chunks = chunk_mapping(mapping, size=chunk_size)

    logger.info(
        "Submitting %d chunk(s) (chunk_size=%d, retry_round=%d) for run_id=%s",
        len(chunks), chunk_size, retry_round, run_id,
    )

    subjobs = []
    for chunk_index, chunk_map in enumerate(chunks):
        payload = build_payload(
            chunk_map, checksums, config, run_id, chunk_index, retry_round,
        )

        # Persist payload BEFORE POST — raw-data-first invariant
        payload_path = store.write_payload(
            run_id, chunk_index, retry_round, payload, runs_dir=runs_dir,
        )

        try:
            job_id = submit_with_500_recovery(
                fts_client, payload, config, run_id, chunk_index, retry_round,
            )
        except SubmissionError as exc:
            logger.error(
                "Chunk %d/%d submission failed (retry_round=%d): %s — "
                "recording as SUBMISSION_FAILED and continuing",
                chunk_index + 1, len(chunks), retry_round, exc,
            )
            subjobs.append({
                "job_id": None,
                "chunk_index": chunk_index,
                "run_id": run_id,
                "retry_round": retry_round,
                "submitted_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "file_count": len(chunk_map),
                "status": "SUBMISSION_FAILED",
                "terminal": True,
                "payload_path": payload_path,
                "fts_monitor_url": "",
            })
            continue

        subjob = {
            "job_id": job_id,
            "chunk_index": chunk_index,
            "run_id": run_id,
            "retry_round": retry_round,
            "submitted_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "file_count": len(chunk_map),
            "status": "SUBMITTED",
            "terminal": False,
            "payload_path": payload_path,
            "fts_monitor_url": (monitor_base + job_id) if monitor_base else "",
        }
        subjobs.append(subjob)

        logger.info(
            "Chunk %d/%d submitted: job_id=%s (%d files)",
            chunk_index + 1, len(chunks), job_id, len(chunk_map),
        )

    logger.info(
        "All %d chunk(s) submitted for run_id=%s (retry_round=%d)",
        len(subjobs), run_id, retry_round,
    )
    return subjobs


def _persist_terminal_job_states(subjobs, fts_client, run_id, runs_dir):
    # type: (list, object, str, str) -> None
    """Fetch and persist the terminal job state JSON for each completed job.

    Writes one file per job to ``runs/<run_id>/raw/jobs/<job_id>.json``.
    Errors are logged as warnings and never raise; raw job state data is
    supplementary (file records are authoritative for metrics).
    """
    for subjob in subjobs:
        if not subjob.get("terminal"):
            continue
        job_id = subjob.get("job_id")
        if not job_id:
            continue
        try:
            job_data = fts_client.get("/jobs/{}".format(job_id))
            store.write_raw(
                run_id, "jobs", "{}.json".format(job_id),
                job_data, runs_dir=runs_dir,
            )
        except Exception as exc:
            logger.warning(
                "Could not persist terminal job state for %s: %s", job_id, exc,
            )


def _merge_file_records(existing, new_records):
    # type: (list, list) -> list
    """Merge retry-round file records into the existing record list.

    For each source PFN that appears in *new_records*, replace the
    corresponding entry in *existing*.  Files not present in the retry
    batch (e.g. already FINISHED) are preserved unchanged.

    Args:
        existing (list[dict]): File records from all previous rounds.
        new_records (list[dict]): File records from the latest retry round.

    Returns:
        list[dict]: Merged list with at most one record per source PFN.
    """
    new_by_src = {r["source_surl"]: r for r in new_records}
    result = []
    for fr in existing:
        result.append(new_by_src.get(fr["source_surl"], fr))
    return result


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_campaign(config, runs_dir=store._DEFAULT_RUNS_DIR):
    # type: (dict, str) -> dict
    """Execute a complete FTS3 transfer campaign.

    Args:
        config (dict): Validated framework config dict (from
            ``config.loader.load()``).
        runs_dir (str): Base directory for run outputs.  Defaults to
            ``"runs"`` relative to the current working directory.

    Returns:
        dict: Final ``MetricsSnapshot`` dict.

    Raises:
        Any unhandled exception from the underlying modules propagates
        directly to the caller.  Callers should wrap in try/except and
        handle cleanup if required.
    """
    ssl_verify = config.get("fts", {}).get("ssl_verify", True)
    endpoint = config["fts"]["endpoint"]

    fts_session = fts_client_mod.build_session(
        config["tokens"]["fts_submit"], ssl_verify,
    )
    fts_client = fts_client_mod.FTSClient(endpoint, fts_session)

    run_id = config.get("run", {}).get("run_id") or generate_run_id()

    # -----------------------------------------------------------------------
    # Step 1: Token validation
    # -----------------------------------------------------------------------
    try:
        whoami = fts_client.get("/whoami")
        logger.debug("Token identity validated: %s", whoami)
    except TokenExpiredError:
        raise
    except Exception as exc:
        logger.warning("GET /whoami failed (continuing): %s", exc)

    # -----------------------------------------------------------------------
    # Step 2: Log optimizer state
    # -----------------------------------------------------------------------
    _log_optimizer_state(fts_client)

    # -----------------------------------------------------------------------
    # Step 3: Resume or fresh run
    # -----------------------------------------------------------------------
    if resume_controller.run_exists(run_id, runs_dir=runs_dir):
        logger.info("Resuming existing run %s", run_id)
        subjobs = resume_controller.load(
            run_id, fts_client, config, runs_dir=runs_dir,
        )
        manifest = store.load_manifest(run_id, runs_dir=runs_dir)
        mapping = OrderedDict(manifest.get("destination_mapping", {}))
        # Checksums are not persisted; leave empty — retry round will
        # submit without checksum (FTS3 skips verification if absent)
        checksums = {}

    else:
        logger.info("Starting new run %s", run_id)
        store.init_run_directory(run_id, config, runs_dir=runs_dir)

        pfns, supplied_checksums = inventory_loader.load(
            config["transfer"]["source_pfns_file"]
        )
        max_files = config.get("transfer", {}).get("max_files")
        if max_files is not None and len(pfns) > max_files:
            logger.info(
                "max_files=%d applied: using first %d of %d PFNs",
                max_files, max_files, len(pfns),
            )
            pfns = pfns[:max_files]
            supplied_checksums = {k: v for k, v in supplied_checksums.items()
                                  if k in pfns}
        mapping = dest_planner.plan(pfns, config)

        verify_checksum = config.get("transfer", {}).get("verify_checksum")
        if verify_checksum in ("none", "target"):
            logger.info(
                "verify_checksum=%s — skipping pre-submission checksum fetch",
                verify_checksum,
            )
            checksums = {}
        elif supplied_checksums:
            logger.info(
                "Using %d pre-supplied checksum(s) from inventory file "
                "— skipping Want-Digest fetch",
                len(supplied_checksums),
            )
            checksums = supplied_checksums
        else:
            source_session = fts_client_mod.build_session(
                config["tokens"]["source_read"], ssl_verify,
            )
            checksums = checksum_fetcher.fetch_all(
                list(mapping.keys()), source_session, config
            )

        store.write_manifest(
            run_id, mapping, config,
            fts_monitor_base=_fts_monitor_base(endpoint),
            runs_dir=runs_dir,
        )

        # -----------------------------------------------------------------------
        # Step 4: Pre-cleanup
        # -----------------------------------------------------------------------
        cleanup_cfg = config.get("cleanup", {})
        if cleanup_cfg.get("before", False):
            cleanup_session = fts_client_mod.build_session(
                config["tokens"]["dest_write"], ssl_verify,
            )
            logger.info("Running pre-cleanup")
            pre_audit = cleanup_manager.cleanup_pre(mapping, cleanup_session, config)
            store.write_cleanup_audit(run_id, "pre", pre_audit, runs_dir=runs_dir)

        # -----------------------------------------------------------------------
        # Step 5: Submit all chunks
        # -----------------------------------------------------------------------
        subjobs = _submit_chunks(
            mapping, checksums, config, run_id, 0, fts_client, runs_dir,
        )
        store.update_manifest(run_id, subjobs, runs_dir=runs_dir)

    # -----------------------------------------------------------------------
    # Step 6: Poll to terminal state
    # -----------------------------------------------------------------------
    logger.info("Polling %d subjob(s) to completion", len(subjobs))
    subjobs = poller.poll_to_completion(subjobs, fts_client, config)
    store.update_manifest(run_id, subjobs, runs_dir=runs_dir)

    # -----------------------------------------------------------------------
    # Step 7: Persist terminal job state snapshots
    # -----------------------------------------------------------------------
    _persist_terminal_job_states(subjobs, fts_client, run_id, runs_dir)

    # -----------------------------------------------------------------------
    # Step 8: Harvest file, retry, and DM records
    # -----------------------------------------------------------------------
    file_records, retry_records, dm_records = collector.harvest_all(
        subjobs, fts_client, run_id=run_id, runs_dir=runs_dir,
    )
    store.write_normalized(
        run_id, file_records, retry_records, dm_records, runs_dir=runs_dir,
    )

    # -----------------------------------------------------------------------
    # Step 9: Framework retry loop
    # -----------------------------------------------------------------------
    all_subjobs = list(subjobs)
    framework_retry_max = config.get("retry", {}).get("framework_retry_max", 0)
    retry_round = 1

    while framework_retry_max > 0 and retry_round <= framework_retry_max:
        failed = [
            fr for fr in file_records
            if fr["file_state"] in ("FAILED", "CANCELED")
        ]
        if not failed:
            logger.info(
                "Framework retry round %d: no failed files — stopping retry loop",
                retry_round,
            )
            break

        logger.info(
            "Framework retry round %d: resubmitting %d failed file(s)",
            retry_round, len(failed),
        )

        failed_sources = {fr["source_surl"] for fr in failed}
        retry_mapping = OrderedDict(
            (src, dst) for src, dst in mapping.items()
            if src in failed_sources
        )

        retry_subjobs = _submit_chunks(
            retry_mapping, checksums, config, run_id, retry_round,
            fts_client, runs_dir,
        )
        store.update_manifest(run_id, retry_subjobs, runs_dir=runs_dir)

        retry_subjobs = poller.poll_to_completion(
            retry_subjobs, fts_client, config,
        )
        store.update_manifest(run_id, retry_subjobs, runs_dir=runs_dir)
        _persist_terminal_job_states(retry_subjobs, fts_client, run_id, runs_dir)
        all_subjobs.extend(retry_subjobs)

        new_file_records, new_retry_records, new_dm_records = collector.harvest_all(
            retry_subjobs, fts_client, run_id=run_id, runs_dir=runs_dir,
        )
        file_records = _merge_file_records(file_records, new_file_records)
        retry_records = retry_records + new_retry_records
        dm_records = dm_records + new_dm_records

        store.write_normalized(
            run_id, file_records, retry_records, dm_records, runs_dir=runs_dir,
        )
        retry_round += 1

    # -----------------------------------------------------------------------
    # Step 10: Compute metrics
    # -----------------------------------------------------------------------
    snapshot = metrics_engine.compute(file_records, retry_records, config, run_id)
    snapshot["ssl_verify_disabled"] = ssl_verify is False

    # -----------------------------------------------------------------------
    # Step 11: Generate reports
    # -----------------------------------------------------------------------
    renderer.render_all(
        snapshot, config,
        subjobs=all_subjobs,
        file_records=file_records,
        runs_dir=runs_dir,
    )

    # -----------------------------------------------------------------------
    # Step 12: Post-cleanup
    # -----------------------------------------------------------------------
    cleanup_cfg = config.get("cleanup", {})
    if cleanup_cfg.get("after", False):
        cleanup_session = fts_client_mod.build_session(
            config["tokens"]["dest_write"], ssl_verify,
        )
        logger.info("Running post-cleanup")
        post_audit = cleanup_manager.cleanup_post(file_records, cleanup_session, config)
        store.write_cleanup_audit(run_id, "post", post_audit, runs_dir=runs_dir)

    # -----------------------------------------------------------------------
    # Step 13: Mark completed
    # -----------------------------------------------------------------------
    store.mark_completed(run_id, runs_dir=runs_dir)
    logger.info(
        "Campaign complete: run_id=%s  threshold_passed=%s",
        run_id, snapshot.get("threshold_passed"),
    )
    return snapshot


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    # type: () -> None
    """CLI entry point for the ``fts-run`` console script."""
    parser = argparse.ArgumentParser(
        prog="fts-run",
        description="FTS3 REST transfer test framework",
    )
    parser.add_argument(
        "config",
        help="Path to campaign YAML config file",
    )
    parser.add_argument(
        "--runs-dir",
        default=store._DEFAULT_RUNS_DIR,
        metavar="DIR",
        help="Base directory for run outputs (default: runs/)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    parser.add_argument(
        "--token",
        default=None,
        metavar="TOKEN",
        help="Bearer token for all roles (overrides FTS_TOKEN env var and YAML tokens section)",
    )
    parser.add_argument(
        "--fts-submit-token",
        default=None,
        metavar="TOKEN",
        help="Bearer token for FTS3 job submission (overrides FTS_SUBMIT_TOKEN env var)",
    )
    parser.add_argument(
        "--source-read-token",
        default=None,
        metavar="TOKEN",
        help="Bearer token for source storage reads (overrides SOURCE_READ_TOKEN env var)",
    )
    parser.add_argument(
        "--dest-write-token",
        default=None,
        metavar="TOKEN",
        help="Bearer token for destination storage writes (overrides DEST_WRITE_TOKEN env var)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    config = config_loader.load(
        args.config,
        token=args.token,
        fts_submit_token=args.fts_submit_token,
        source_read_token=args.source_read_token,
        dest_write_token=args.dest_write_token,
    )

    try:
        snapshot = run_campaign(config, runs_dir=args.runs_dir)
    except Exception as exc:
        logger.error("Campaign failed: %s", exc, exc_info=True)
        sys.exit(1)

    sys.exit(0 if snapshot.get("threshold_passed") else 1)
