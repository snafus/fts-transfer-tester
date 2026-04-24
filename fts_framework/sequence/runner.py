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
import shutil
import uuid
from datetime import datetime

from fts_framework.config import loader as config_loader
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


def _build_trial_config(baseline_config, case_params):
    # type: (dict, dict) -> dict
    """Deep-copy *baseline_config* and apply *case_params* overrides."""
    config = copy.deepcopy(baseline_config)
    for dotkey, value in case_params.items():
        seq_loader.apply_override(config, dotkey, value)
    # Each trial must generate a fresh run_id.
    config.setdefault("run", {})["run_id"] = None
    return config


def _write_params_copy(sequence_dir, params_file):
    # type: (str, str) -> None
    """Copy the sequence params file into the sequence directory."""
    dest = os.path.join(sequence_dir, "params.yaml")
    shutil.copy2(params_file, dest)


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

    for case_index, trial_index in pending:
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
        except Exception as exc:
            seq_state.mark_failed(
                sequence_dir, state, case_index, trial_index, exc,
            )
            logger.error(
                "Case %d trial %d FAILED (run_id=%s): %s — continuing",
                case_index, trial_index, run_id, exc,
                exc_info=True,
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
