"""
fts_framework.persistence.store
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
All disk I/O for the framework.  No metric computation is performed here.

Responsibilities
----------------
- Create the ``runs/<run_id>/`` directory tree on first call.
- Write a token-redacted copy of the config as ``config.yaml``.
- Write and atomically update ``manifest.json``.
- Write raw REST responses under ``raw/{category}/``.
- Write submitted job payloads under ``submitted_payloads/``.
- Write normalised records under ``normalized/``.
- Write the ``MetricsSnapshot`` under ``metrics/``.
- Write cleanup audit logs (``cleanup_pre.json``, ``cleanup_post.json``).

Atomic writes
-------------
All writes to ``manifest.json`` use a write-then-rename pattern so the file
is never left in a half-written state.  Other JSON writes use direct open
because they are append-only (a new file per raw response) and are considered
safe to re-fetch if truncated.

Tokens
------
Tokens (``config["tokens"]["fts_submit"]``, ``source_read``, ``dest_write``)
are never written to disk.  ``_redact_config`` deep-copies the config dict
and replaces every value under ``config["tokens"]`` with ``"<REDACTED>"``.

Usage::

    from fts_framework.persistence import store
    store.init_run_directory(run_id, config)
    store.write_manifest(run_id, dest_mapping, config)
    store.update_manifest(run_id, subjobs)
    store.write_raw(run_id, "files", job_id + ".json", raw_list)
    store.write_normalized(run_id, file_records, retry_records, dm_records)
    store.write_metrics(run_id, snapshot)
    store.mark_completed(run_id)
"""

import copy
import hashlib
import json
import logging
import os
import tempfile

import yaml

from fts_framework.exceptions import ResumeError

logger = logging.getLogger(__name__)

# Default base directory for run outputs (relative to cwd, or override in tests)
_DEFAULT_RUNS_DIR = "runs"

# Sub-directories created inside every run directory
_RUN_SUBDIRS = [
    "submitted_payloads",
    os.path.join("raw", "jobs"),
    os.path.join("raw", "files"),
    os.path.join("raw", "retries"),
    os.path.join("raw", "dm"),
    "normalized",
    "metrics",
    "reports",
]


# ---------------------------------------------------------------------------
# Directory initialisation
# ---------------------------------------------------------------------------

def init_run_directory(run_id, config, runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, dict, str) -> str
    """Create the ``runs/<run_id>/`` tree and write the redacted config.

    Args:
        run_id (str): Unique run identifier.
        config (dict): Validated framework config dict.
        runs_dir (str): Base directory for run outputs.  Default ``"runs"``.

    Returns:
        str: Absolute path to the run directory.
    """
    run_dir = os.path.join(runs_dir, run_id)
    for sub in [run_dir] + [os.path.join(run_dir, s) for s in _RUN_SUBDIRS]:
        os.makedirs(sub, exist_ok=True)

    _write_redacted_config(run_dir, config)

    logger.info("Run directory initialised: %s", run_dir)
    return run_dir


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def write_manifest(run_id, dest_mapping, config, fts_monitor_base="",
                   runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, dict, dict, str, str) -> None
    """Write the initial ``manifest.json`` for a new run.

    Called once before submission begins.  ``subjobs`` starts empty; call
    :func:`update_manifest` after each job is submitted.

    Args:
        run_id (str): Unique run identifier.
        dest_mapping (list): ``[(src_pfn, dest_url)]`` pairs from
            ``destination.planner``.
        config (dict): Validated framework config dict.
        fts_monitor_base (str): FTS WebMonitor base URL (optional).
        runs_dir (str): Base directory for run outputs.
    """
    from datetime import datetime
    manifest = {
        "run_id": run_id,
        "test_label": config["run"]["test_label"],
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "config_hash": _config_hash(config),
        "fts_endpoint": config.get("fts", {}).get("endpoint", ""),
        "fts_monitor_base": fts_monitor_base,
        "ssl_verify_disabled": config.get("fts", {}).get("ssl_verify", True) is False,
        "destination_mapping": [list(pair) for pair in dest_mapping],
        "subjobs": [],
        "completed": False,
    }
    _atomic_write_json(_manifest_path(run_id, runs_dir), manifest)
    logger.debug("Initial manifest written for run %s", run_id)


def update_manifest(run_id, subjobs, runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, list, str) -> None
    """Merge *subjobs* into the persisted manifest.

    Existing subjobs with the same ``job_id`` are replaced; new ones are
    appended.  The write is atomic (write-then-rename).

    Args:
        run_id (str): Unique run identifier.
        subjobs (list[dict]): ``SubjobRecord`` dicts to persist.
        runs_dir (str): Base directory for run outputs.
    """
    manifest = load_manifest(run_id, runs_dir)
    # Separate existing entries into keyed (have job_id) and unkeyed.
    existing = {s["job_id"]: s for s in manifest["subjobs"] if s.get("job_id")}
    no_id = [s for s in manifest["subjobs"] if not s.get("job_id")]
    for subjob in subjobs:
        jid = subjob.get("job_id")
        if jid:
            existing[jid] = subjob
        else:
            # Subjob without job_id (e.g. pending pre-submission): preserve
            no_id.append(subjob)
    manifest["subjobs"] = list(existing.values()) + no_id
    _atomic_write_json(_manifest_path(run_id, runs_dir), manifest)
    logger.debug("Manifest updated: %d subjob(s) for run %s", len(manifest["subjobs"]), run_id)


def mark_completed(run_id, runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, str) -> None
    """Set ``completed = True`` in the manifest.

    Args:
        run_id (str): Unique run identifier.
        runs_dir (str): Base directory for run outputs.
    """
    manifest = load_manifest(run_id, runs_dir)
    manifest["completed"] = True
    _atomic_write_json(_manifest_path(run_id, runs_dir), manifest)
    logger.info("Run %s marked completed", run_id)


def load_manifest(run_id, runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, str) -> dict
    """Load and return the ``manifest.json`` for *run_id*.

    Args:
        run_id (str): Unique run identifier.
        runs_dir (str): Base directory for run outputs.

    Returns:
        dict: Parsed manifest dict.

    Raises:
        ResumeError: If the manifest file is missing or contains invalid JSON.
    """
    path = _manifest_path(run_id, runs_dir)
    if not os.path.isfile(path):
        raise ResumeError(path, "file not found")
    try:
        with open(path, "r") as fh:
            return json.load(fh)
    except (ValueError, IOError) as exc:
        raise ResumeError(path, str(exc))


# ---------------------------------------------------------------------------
# Raw REST responses
# ---------------------------------------------------------------------------

def write_raw(run_id, category, filename, data, runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, str, str, object, str) -> None
    """Persist a raw REST response before any processing.

    Args:
        run_id (str): Unique run identifier.
        category (str): Sub-directory under ``raw/``.  One of
            ``"jobs"``, ``"files"``, ``"retries"``, ``"dm"``.
        filename (str): Filename within the category dir (e.g. ``"abc123.json"``).
        data: JSON-serialisable object (list or dict from FTS3 response).
        runs_dir (str): Base directory for run outputs.
    """
    path = os.path.join(runs_dir, run_id, "raw", category, filename)
    _write_json(path, data)
    logger.debug("Raw response written: %s", path)


# ---------------------------------------------------------------------------
# Submitted payloads
# ---------------------------------------------------------------------------

def write_payload(run_id, chunk_index, retry_round, payload,
                  runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, int, int, dict, str) -> str
    """Persist the POST body for a chunk before transmission.

    Storage tokens (``source_token``, ``destination_token``) are redacted
    from the persisted copy; the caller's dict is not modified.

    Args:
        run_id (str): Unique run identifier.
        chunk_index (int): Zero-based chunk index.
        retry_round (int): Framework retry round (0 = initial submission).
        payload (dict): The full FTS3 job payload dict.
        runs_dir (str): Base directory for run outputs.

    Returns:
        str: Relative path (from run root) of the written file, suitable for
            storage in ``SubjobRecord["payload_path"]``.
    """
    filename = "chunk_{:04d}_r{}.json".format(chunk_index, retry_round)
    rel_path = os.path.join("submitted_payloads", filename)
    abs_path = os.path.join(runs_dir, run_id, rel_path)
    _write_json(abs_path, _redact_payload(payload))
    logger.debug("Payload written: %s", abs_path)
    return rel_path


# ---------------------------------------------------------------------------
# Normalised records
# ---------------------------------------------------------------------------

def write_normalized(run_id, file_records, retry_records, dm_records,
                     runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, list, list, list, str) -> None
    """Write normalised record lists to ``normalized/``.

    Args:
        run_id (str): Unique run identifier.
        file_records (list[dict]): Normalised ``FileRecord`` dicts.
        retry_records (list[dict]): Normalised ``RetryRecord`` dicts.
        dm_records (list[dict]): Raw DM record dicts (pass-through).
        runs_dir (str): Base directory for run outputs.
    """
    norm_dir = os.path.join(runs_dir, run_id, "normalized")
    _write_json(os.path.join(norm_dir, "file_records.json"), file_records)
    _write_json(os.path.join(norm_dir, "retry_records.json"), retry_records)
    _write_json(os.path.join(norm_dir, "dm_records.json"), dm_records)
    logger.debug(
        "Normalised records written: %d files, %d retries, %d DM",
        len(file_records), len(retry_records), len(dm_records),
    )


# ---------------------------------------------------------------------------
# Metrics snapshot
# ---------------------------------------------------------------------------

def write_metrics(run_id, snapshot, runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, dict, str) -> None
    """Write the ``MetricsSnapshot`` to ``metrics/snapshot.json``.

    Args:
        run_id (str): Unique run identifier.
        snapshot (dict): ``MetricsSnapshot`` dict from ``metrics.engine.compute``.
        runs_dir (str): Base directory for run outputs.
    """
    path = os.path.join(runs_dir, run_id, "metrics", "snapshot.json")
    _write_json(path, snapshot)
    logger.info("Metrics snapshot written: %s", path)


# ---------------------------------------------------------------------------
# Cleanup audit logs
# ---------------------------------------------------------------------------

def write_report(run_id, filename, content, runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, str, str, str) -> None
    """Write a text report file to ``reports/``.

    Args:
        run_id (str): Unique run identifier.
        filename (str): Filename within ``reports/`` (e.g. ``"report.md"``).
        content (str): Text content to write.
        runs_dir (str): Base directory for run outputs.
    """
    path = os.path.join(runs_dir, run_id, "reports", filename)
    with open(path, "w") as fh:
        fh.write(content)
    logger.debug("Report written: %s", path)


def write_cleanup_audit(run_id, phase, audit_list, runs_dir=_DEFAULT_RUNS_DIR):
    # type: (str, str, list, str) -> None
    """Write cleanup audit records to ``cleanup_{phase}.json``.

    Args:
        run_id (str): Unique run identifier.
        phase (str): ``"pre"`` or ``"post"``.
        audit_list (list[dict]): Audit records from ``cleanup.manager``.
        runs_dir (str): Base directory for run outputs.
    """
    filename = "cleanup_{}.json".format(phase)
    path = os.path.join(runs_dir, run_id, filename)
    _write_json(path, audit_list)
    logger.debug("Cleanup audit (%s) written: %s", phase, path)


# ---------------------------------------------------------------------------
# Token redaction (public — used by tests and runner)
# ---------------------------------------------------------------------------

def redact_config(config):
    # type: (dict) -> dict
    """Return a deep copy of *config* with all token values replaced.

    Replaces every value under ``config["tokens"]`` with ``"<REDACTED>"``.
    All other config keys are preserved verbatim.

    Args:
        config (dict): Validated framework config dict.

    Returns:
        dict: Deep copy with tokens redacted.
    """
    redacted = copy.deepcopy(config)
    tokens = redacted.get("tokens")
    if isinstance(tokens, dict):
        for key in tokens:
            tokens[key] = "<REDACTED>"
    return redacted


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _manifest_path(run_id, runs_dir):
    # type: (str, str) -> str
    return os.path.join(runs_dir, run_id, "manifest.json")


def _redact_payload(payload):
    # type: (dict) -> dict
    """Return a deep copy of *payload* with storage token values replaced.

    Replaces ``source_token`` and ``destination_token`` under ``params`` with
    ``"<REDACTED>"``.  All other payload keys are preserved verbatim.
    """
    redacted = copy.deepcopy(payload)
    params = redacted.get("params")
    if isinstance(params, dict):
        for key in ("source_token", "destination_token"):
            if key in params:
                params[key] = "<REDACTED>"
    return redacted


def _write_redacted_config(run_dir, config):
    # type: (str, dict) -> None
    """Write a token-redacted config as ``config.yaml`` inside *run_dir*."""
    redacted = redact_config(config)
    path = os.path.join(run_dir, "config.yaml")
    with open(path, "w") as fh:
        yaml.dump(redacted, fh, default_flow_style=False, allow_unicode=True)
    logger.debug("Redacted config written: %s", path)


def _write_json(path, data):
    # type: (str, object) -> None
    """Write *data* as indented JSON to *path*."""
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2, default=str)


def _atomic_write_json(path, data):
    # type: (str, object) -> None
    """Write *data* as JSON to *path* atomically via a temp file and rename.

    The rename is atomic on POSIX; on Windows it overwrites if the destination
    exists (Python 3.3+ ``os.replace``).
    """
    dir_name = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(data, fh, indent=2, default=str)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _config_hash(config):
    # type: (dict) -> str
    """Return a stable SHA-256 fingerprint of the redacted config.

    Tokens are excluded before hashing so the hash is safe to store in the
    manifest.

    Returns:
        str: ``"sha256:<hex>"``
    """
    redacted = redact_config(config)
    serialised = json.dumps(redacted, sort_keys=True, default=str).encode("utf-8")
    digest = hashlib.sha256(serialised).hexdigest()
    return "sha256:{}".format(digest)
