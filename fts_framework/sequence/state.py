"""
fts_framework.sequence.state
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
State persistence for sequence runs.

``state.json`` tracks every (case, trial) pair so that interrupted sequences
can be resumed without re-running completed trials.

Schema::

    {
      "sequence_id": "20260324_abc123_scale_test",
      "sequence_label": "scale_test",
      "baseline_config": "config/my_campaign.yaml",
      "runs_dir": "runs",
      "created_at": "2026-03-24T10:00:00Z",
      "sweep_mode": "cartesian",
      "trials": 3,
      "cases": [
        {
          "case_index": 0,
          "params": {"transfer.max_files": 100},
          "trials": [
            {
              "trial_index": 0,
              "run_id": "20260324_def456",
              "status": "completed",
              "error": null,
              "completed_at": "2026-03-24T10:05:00Z"
            },
            ...
          ]
        }
      ]
    }

Trial statuses
--------------
- ``pending``   — not yet started
- ``running``   — started but not finished; treated as *pending* on resume
                  (handles process crash mid-run)
- ``completed`` — ``run_campaign()`` returned without exception
- ``failed``    — ``run_campaign()`` raised an exception; error logged
"""

import json
import os
from datetime import datetime


_STATE_FILENAME = "state.json"

PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"


def _now_iso():
    # type: () -> str
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _write(sequence_dir, state):
    # type: (str, dict) -> None
    # Write to a temp file then rename for atomicity: a process crash during
    # the write must never leave state.json in a partial/corrupt state.
    path = os.path.join(sequence_dir, _STATE_FILENAME)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as fh:
        json.dump(state, fh, indent=2)
    os.replace(tmp_path, path)


def create(sequence_dir, sequence_id, seq_params, cases, trials,
           runs_dir="runs"):
    # type: (str, str, dict, list, int, str) -> dict
    """Create a new state dict and write ``state.json`` to *sequence_dir*.

    Args:
        sequence_dir (str): Sequence output directory (must already exist).
        sequence_id (str): Unique identifier for this sequence run.
        seq_params (dict): Parsed sequence params dict (from
            ``sequence.loader.load()``).
        cases (list[dict]): Ordered list of parameter override dicts.
        trials (int): Number of trials per case.
        runs_dir (str): Base directory for individual run outputs.

    Returns:
        dict: The new state dict (also written to disk).
    """
    state = {
        "sequence_id": sequence_id,
        "sequence_label": seq_params.get("label"),
        "baseline_config": seq_params["baseline_config_path"],
        "runs_dir": runs_dir,
        "created_at": _now_iso(),
        "sweep_mode": seq_params.get("sweep_mode", "cartesian"),
        "trials": trials,
        "cases": [],
    }

    for i, case_params in enumerate(cases):
        trial_list = []
        for j in range(trials):
            trial_list.append({
                "trial_index": j,
                "run_id": None,
                "status": PENDING,
                "error": None,
                "completed_at": None,
            })
        state["cases"].append({
            "case_index": i,
            "params": case_params,
            "trials": trial_list,
        })

    _write(sequence_dir, state)
    return state


def load(sequence_dir):
    # type: (str) -> dict
    """Load ``state.json`` from *sequence_dir*.

    Returns:
        dict: State dict.

    Raises:
        IOError / FileNotFoundError: if ``state.json`` does not exist.
    """
    path = os.path.join(sequence_dir, _STATE_FILENAME)
    with open(path, "r") as fh:
        return json.load(fh)


def mark_running(sequence_dir, state, case_index, trial_index, run_id):
    # type: (str, dict, int, int, str) -> None
    """Mark a trial as *running* and record its run_id."""
    state["cases"][case_index]["trials"][trial_index]["status"] = RUNNING
    state["cases"][case_index]["trials"][trial_index]["run_id"] = run_id
    _write(sequence_dir, state)


def mark_completed(sequence_dir, state, case_index, trial_index):
    # type: (str, dict, int, int) -> None
    """Mark a trial as *completed*."""
    state["cases"][case_index]["trials"][trial_index]["status"] = COMPLETED
    state["cases"][case_index]["trials"][trial_index]["completed_at"] = _now_iso()
    _write(sequence_dir, state)


def mark_failed(sequence_dir, state, case_index, trial_index, error):
    # type: (str, dict, int, int, object) -> None
    """Mark a trial as *failed* and record the error message."""
    state["cases"][case_index]["trials"][trial_index]["status"] = FAILED
    state["cases"][case_index]["trials"][trial_index]["error"] = str(error)
    _write(sequence_dir, state)


def reset_failed_to_pending(sequence_dir, state):
    # type: (str, dict) -> int
    """Reset all *failed* trials to *pending* so they will be retried.

    Clears ``run_id``, ``error``, and ``completed_at`` on each reset trial.
    Writes the updated state to disk atomically.

    Args:
        sequence_dir (str): Sequence output directory.
        state (dict): Current state dict (mutated in place).

    Returns:
        int: Number of trials reset.
    """
    count = 0
    for case in state["cases"]:
        for trial in case["trials"]:
            if trial["status"] == FAILED:
                trial["status"] = PENDING
                trial["run_id"] = None
                trial["error"] = None
                trial["completed_at"] = None
                count += 1
    if count:
        _write(sequence_dir, state)
    return count


def pending_trials(state):
    # type: (dict) -> list
    """Return list of ``(case_index, trial_index)`` that need to run.

    Both ``pending`` and ``running`` trials are returned; ``running`` entries
    represent trials that were interrupted mid-run and must be retried.

    Returns:
        list[tuple[int, int]]: Ordered list of (case_index, trial_index).
    """
    result = []
    for case in state["cases"]:
        for trial in case["trials"]:
            if trial["status"] in (PENDING, RUNNING):
                result.append((case["case_index"], trial["trial_index"]))
    return result
