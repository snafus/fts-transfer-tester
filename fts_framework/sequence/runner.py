"""
fts_framework.sequence.runner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Outer execution loop for sequence campaigns.

For each pending (case, trial) pair:

1. Deep-copy the baseline config.
2. Apply case parameter overrides via dot-notation keys.
3. Assign a fresh ``run_id`` so each trial is a distinct campaign.
4. Call ``run_campaign()``.
5. Mark the trial completed or failed; log and continue on failure.

Resumption
----------
Pass ``resume_dir`` to reload an existing ``state.json`` and skip all
``completed`` trials.  Trials that were in ``running`` state (i.e. the
process crashed mid-run) are treated as pending and restarted.
"""

import copy
import logging
import os
import queue
import shutil
import threading
import uuid
from datetime import datetime

from fts_framework.config import loader as config_loader
from fts_framework.exceptions import ConfigError
from fts_framework.runner import generate_run_id, run_campaign
from fts_framework.sequence import loader as seq_loader
from fts_framework.sequence import reporter as seq_reporter
from fts_framework.sequence import state as seq_state

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_sequence_id(label=None):
    # type: (object) -> str
    timestamp  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    if label:
        return "{}_{}_{}".format(timestamp, short_uuid, label)
    return "{}_{}".format(timestamp, short_uuid)


# Config keys whose path component appears in OIDC scope templates.
_SCOPE_PREFIX_VARS = {
    "transfer.dst_prefix":    "{dst_prefix_path}",
    "transfer.source_prefix": "{src_prefix_path}",
}


def _build_trial_config(baseline_config, case_params):
    # type: (dict, dict) -> dict
    """Deep-copy *baseline_config* and apply *case_params* overrides.

    Always re-fetches all OIDC tokens so that long sequences don't run later
    trials with expired tokens.  Scope templates are re-evaluated after any
    override that changes a prefix key.
    """
    config = copy.deepcopy(baseline_config)
    for dotkey, value in case_params.items():
        seq_loader.apply_override(config, dotkey, value)
    config.setdefault("run", {})["run_id"] = None
    _refresh_oidc_tokens(config, set(case_params.keys()))
    return config


def _refresh_oidc_tokens(config, overridden_keys):
    # type: (dict, set) -> None
    """Re-fetch all OIDC tokens for every trial.

    Tokens have a finite lifetime (typically 1h).  Refreshing unconditionally
    at the start of each trial prevents 403 errors in long sequences where the
    initial token has expired by the time a later trial runs.

    If a sweep parameter changes a scope-template key (e.g. ``transfer.dst_prefix``),
    the affected roles must be re-fetched with the updated scope regardless —
    this is handled as part of the unconditional refresh.
    """
    oidc_cfg = config.get("oidc", {})
    if not oidc_cfg.get("enabled"):
        return

    all_roles = list((oidc_cfg.get("roles") or {}).keys())
    if not all_roles:
        return

    logger.debug("Refreshing OIDC tokens for all roles: %s", all_roles)
    config_loader.refresh_oidc_tokens_for_roles(config, all_roles)


def _print_interrupt_hint(sequence_dir, params_file):
    # type: (str, str) -> None
    """Print resume/cancel instructions to stderr after a KeyboardInterrupt."""
    import sys
    sys.stderr.write("\n\nInterrupted.\n")
    sys.stderr.write("  Sequence state saved. In-flight FTS3 jobs continue running.\n\n")
    sys.stderr.write("  Resume:      fts-sequence {} --resume {}\n".format(
        params_file, sequence_dir))
    sys.stderr.write("  Cancel jobs: fts-sequence --cancel-jobs {}\n\n".format(
        sequence_dir))


def _write_params_copy(sequence_dir, params_file):
    # type: (str, str) -> None
    """Copy the sequence params file into the sequence directory."""
    dest = os.path.join(sequence_dir, "params.yaml")
    shutil.copy2(params_file, dest)


# ---------------------------------------------------------------------------
# Per-trial execution (shared by both the sequential and parallel_cases
# execution paths, so behavior is identical regardless of mode)
# ---------------------------------------------------------------------------

def _run_one_trial(state, sequence_dir, runs_dir, baseline_config,
                    case_index, trial_index, params_file):
    # type: (dict, str, str, dict, int, int, str) -> None
    """Build config for one (case, trial) pair and run its campaign.

    Marks the trial running/completed/failed in ``state.json`` as it goes.
    On ``KeyboardInterrupt``, prints the resume/cancel hint and re-raises.
    On any other exception, marks the trial failed, logs, and returns
    normally so the caller can continue with the next trial.
    """
    case        = state["cases"][case_index]
    trial       = case["trials"][trial_index]
    case_params = case["params"]
    n_cases     = len(state["cases"])
    n_trials    = state["trials"]

    logger.info(
        "Case %d/%d  trial %d/%d  params=%s",
        case_index + 1, n_cases,
        trial_index + 1, n_trials,
        case_params,
    )

    trial_config = _build_trial_config(baseline_config, case_params)

    # For RUNNING trials (process was interrupted mid-campaign), reuse the
    # stored run_id so run_campaign() can resume the partial run via its
    # internal resume logic.  For PENDING trials, generate a fresh run_id.
    existing_run_id = trial.get("run_id")
    if trial["status"] == seq_state.RUNNING and existing_run_id:
        run_id = existing_run_id
        logger.info(
            "Resuming interrupted trial (case=%d trial=%d run_id=%s)",
            case_index, trial_index, run_id,
        )
    else:
        run_id = "c{:02d}_t{:02d}_{}".format(
            case_index, trial_index, generate_run_id()
        )
    trial_config["run"]["run_id"] = run_id

    seq_state.mark_running(
        sequence_dir, state, case_index, trial_index, run_id,
    )

    try:
        run_campaign(trial_config, runs_dir=runs_dir)
        seq_state.mark_completed(
            sequence_dir, state, case_index, trial_index,
        )
        logger.info(
            "Case %d trial %d completed: run_id=%s",
            case_index, trial_index, run_id,
        )
    except KeyboardInterrupt:
        _print_interrupt_hint(sequence_dir, params_file)
        raise
    except Exception as exc:
        seq_state.mark_failed(
            sequence_dir, state, case_index, trial_index, exc,
        )
        logger.error(
            "Case %d trial %d FAILED (run_id=%s): %s — continuing",
            case_index, trial_index, run_id, exc,
            exc_info=True,
        )


def _run_case_sequential(case_index, state, sequence_dir, runs_dir,
                          baseline_config, params_file):
    # type: (int, dict, str, str, dict, str) -> None
    """Run every pending/running trial of one case, strictly in order.

    Used as the unit of work for a parallel_cases worker thread: a whole
    case is owned by one thread for its full duration, so trials of the
    same case never run concurrently with each other.
    """
    for trial in state["cases"][case_index]["trials"]:
        if trial["status"] in (seq_state.PENDING, seq_state.RUNNING):
            _run_one_trial(
                state, sequence_dir, runs_dir, baseline_config,
                case_index, trial["trial_index"], params_file,
            )


def _validate_destination_naming_for_parallel(baseline_config, cases):
    # type: (dict, list) -> None
    """Require every case to resolve to a non-sequential destination_naming.

    parallel_cases mode runs multiple cases concurrently. If two cases share
    a resolved dst_prefix/test_label (common in sweeps that vary something
    else) and use the default 'sequential' naming, both would independently
    plan testfile_000000, testfile_000001, ... under the same destination
    directory — two live FTS3 jobs targeting the same path at the same
    time. Raise before any trial runs rather than let that happen.

    Does not reuse _build_trial_config(): that also refreshes OIDC tokens
    (real HTTP calls), which this pure validation pass must not trigger.
    """
    for case in cases:
        trial_config = copy.deepcopy(baseline_config)
        for dotkey, value in case["params"].items():
            seq_loader.apply_override(trial_config, dotkey, value)
        naming = trial_config["transfer"].get("destination_naming", "sequential")
        if naming == "sequential":
            raise ConfigError(
                "sequence.execution.mode=parallel_cases requires "
                "transfer.destination_naming to be 'uuid' or 'date' for "
                "every case (case_index={} resolves to 'sequential'); "
                "concurrent cases sharing a destination prefix would "
                "otherwise collide.".format(case["case_index"])
            )


def _run_parallel_cases(pending, state, sequence_dir, runs_dir,
                         baseline_config, params_file, max_workers):
    # type: (list, dict, str, str, dict, str, int) -> None
    """Run distinct cases concurrently (up to max_workers at a time).

    Each case is claimed by exactly one worker thread, which runs all of
    that case's pending trials sequentially via _run_case_sequential(). Uses
    a hand-rolled daemon-thread pool rather than
    concurrent.futures.ThreadPoolExecutor: CPython's ThreadPoolExecutor
    worker threads are non-daemon and are joined by an atexit-style hook
    before the interpreter exits, which would make Ctrl-C block until every
    in-flight case's *current* trial finishes (campaigns can run for
    hours). Daemon threads let the process exit immediately on interrupt,
    exactly like today's single-trial serial-mode behavior — abandoned
    trials are left 'running' and are resumed the same way an interrupted
    serial run is resumed today.
    """
    case_indices = []
    seen = set()
    for case_index, _trial_index in pending:
        if case_index not in seen:
            seen.add(case_index)
            case_indices.append(case_index)

    work_q = queue.Queue()  # type: queue.Queue
    for case_index in case_indices:
        work_q.put(case_index)

    def _worker():
        while True:
            try:
                case_index = work_q.get_nowait()
            except queue.Empty:
                return
            try:
                _run_case_sequential(
                    case_index, state, sequence_dir, runs_dir,
                    baseline_config, params_file,
                )
            finally:
                work_q.task_done()

    n_workers = min(max_workers, len(case_indices))
    threads = [
        threading.Thread(target=_worker, daemon=True)
        for _ in range(n_workers)
    ]
    for t in threads:
        t.start()

    try:
        for t in threads:
            while t.is_alive():
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        _print_interrupt_hint(sequence_dir, params_file)
        raise


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_sequence(params_file, resume_dir=None, runs_dir=None,
                 token=None, fts_submit_token=None,
                 source_read_token=None, dest_write_token=None):
    # type: (str, object, object, object, object, object, object) -> str
    """Execute (or resume) a parameter-sweep sequence of campaigns.

    Args:
        params_file (str): Path to the sequence parameter YAML file.
        resume_dir (str | None): Path to an existing sequence output
            directory to resume.  ``None`` starts a new sequence.
        runs_dir (str | None): Base directory for individual run outputs.
            Defaults to the framework default (``"runs"``).
        token (str | None): Shared bearer token for all roles.
        fts_submit_token (str | None): Per-role FTS3 submission token.
        source_read_token (str | None): Per-role source-read token.
        dest_write_token (str | None): Per-role destination-write token.

    Returns:
        str: Path to the sequence output directory.
    """
    # Load and validate sequence params
    seq_params = seq_loader.load(params_file)

    # Load baseline config (tokens resolved here — never written to state.json)
    baseline_config = config_loader.load(
        seq_params["baseline_config_path"],
        token=token,
        fts_submit_token=fts_submit_token,
        source_read_token=source_read_token,
        dest_write_token=dest_write_token,
    )

    # Determine / create sequence directory, then resolve runs_dir.
    # runs_dir defaults to <sequence_dir>/runs/ so all outputs are
    # self-contained within the sequence directory.
    if resume_dir:
        sequence_dir = resume_dir
        state = seq_state.load(sequence_dir)
        # On resume, use the stored runs_dir unless the caller overrides it.
        runs_dir = runs_dir or state.get("runs_dir") or os.path.join(
            sequence_dir, "runs"
        )
        logger.info(
            "Resuming sequence %s from %s",
            state["sequence_id"], sequence_dir,
        )
    else:
        sequence_id  = _generate_sequence_id(seq_params.get("label"))
        output_base  = seq_params["output_base_dir"]
        sequence_dir = os.path.join(output_base, sequence_id)
        runs_dir     = runs_dir or os.path.join(sequence_dir, "runs")
        os.makedirs(os.path.join(sequence_dir, "reports"), exist_ok=True)
        os.makedirs(runs_dir, exist_ok=True)
        _write_params_copy(sequence_dir, params_file)
        state = seq_state.create(
            sequence_dir, sequence_id, seq_params,
            seq_params["cases"], seq_params["trials"],
            runs_dir=runs_dir,
        )
        logger.info(
            "New sequence: id=%s  cases=%d  trials=%d  total_runs=%d",
            sequence_id,
            len(seq_params["cases"]),
            seq_params["trials"],
            len(seq_params["cases"]) * seq_params["trials"],
        )

    pending = seq_state.pending_trials(state)
    logger.info("%d trial(s) pending", len(pending))

    if seq_params.get("execution_mode", "sequential") == "parallel_cases":
        _validate_destination_naming_for_parallel(baseline_config, state["cases"])
        max_workers = seq_params["max_workers"]
        logger.info(
            "Executing %d case(s) in parallel (max_workers=%d)",
            len(state["cases"]), max_workers,
        )
        _run_parallel_cases(
            pending, state, sequence_dir, runs_dir,
            baseline_config, params_file, max_workers,
        )
    else:
        for case_index, trial_index in pending:
            _run_one_trial(
                state, sequence_dir, runs_dir, baseline_config,
                case_index, trial_index, params_file,
            )

    # Always regenerate summary reports (even on partial completion)
    logger.info("Generating sequence summary reports")
    seq_reporter.generate_summary(sequence_dir, state, runs_dir=runs_dir)

    completed = sum(
        1
        for case in state["cases"]
        for trial in case["trials"]
        if trial["status"] == seq_state.COMPLETED
    )
    total = len(seq_params["cases"]) * seq_params["trials"]
    logger.info(
        "Sequence finished: %s  completed=%d/%d",
        sequence_dir, completed, total,
    )
    return sequence_dir
