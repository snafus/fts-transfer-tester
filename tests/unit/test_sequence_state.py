"""Unit tests for fts_framework.sequence.state."""

import json
import os
import tempfile

import pytest

from fts_framework.sequence import state as seq_state
from fts_framework.sequence.state import (
    COMPLETED,
    FAILED,
    PENDING,
    RUNNING,
    create,
    load,
    mark_completed,
    mark_failed,
    mark_running,
    pending_trials,
)


def _seq_params(baseline="config/x.yaml", label=None, sweep_mode="cartesian"):
    return {
        "baseline_config_path": baseline,
        "label":                label,
        "sweep_mode":           sweep_mode,
    }


def _make_cases(n):
    return [{"transfer.max_files": (i + 1) * 100} for i in range(n)]


# ---------------------------------------------------------------------------
# create / load
# ---------------------------------------------------------------------------

class TestCreateAndLoad:
    def test_state_json_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            create(tmp, "seq_001", _seq_params(), _make_cases(2), trials=3)
            assert os.path.isfile(os.path.join(tmp, "state.json"))

    def test_correct_case_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "seq_001", _seq_params(), _make_cases(3), trials=2)
        assert len(state["cases"]) == 3

    def test_correct_trial_count_per_case(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "seq_001", _seq_params(), _make_cases(2), trials=4)
        for case in state["cases"]:
            assert len(case["trials"]) == 4

    def test_all_trials_initially_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "seq_001", _seq_params(), _make_cases(2), trials=3)
        for case in state["cases"]:
            for trial in case["trials"]:
                assert trial["status"] == PENDING
                assert trial["run_id"] is None

    def test_sequence_id_stored(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "my_seq_id", _seq_params(), _make_cases(1), trials=1)
        assert state["sequence_id"] == "my_seq_id"

    def test_sweep_mode_stored(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "s", _seq_params(sweep_mode="zip"),
                           _make_cases(1), trials=1)
        assert state["sweep_mode"] == "zip"

    def test_runs_dir_stored(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "s", _seq_params(), _make_cases(1), trials=1,
                           runs_dir="/data/runs")
        assert state["runs_dir"] == "/data/runs"

    def test_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = create(tmp, "seq_rt", _seq_params(label="lbl"),
                              _make_cases(2), trials=2)
            loaded = load(tmp)
        assert loaded["sequence_id"] == "seq_rt"
        assert loaded["sequence_label"] == "lbl"
        assert len(loaded["cases"]) == 2

    def test_load_missing_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            with pytest.raises((IOError, OSError)):
                load(tmp)


# ---------------------------------------------------------------------------
# mark_running / mark_completed / mark_failed
# ---------------------------------------------------------------------------

class TestMutations:
    def _fresh(self, tmp, n_cases=2, trials=2):
        return create(tmp, "s", _seq_params(), _make_cases(n_cases),
                      trials=trials)

    def test_mark_running_sets_status_and_run_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self._fresh(tmp)
            mark_running(tmp, state, 0, 0, "run_abc")
            assert state["cases"][0]["trials"][0]["status"] == RUNNING
            assert state["cases"][0]["trials"][0]["run_id"] == "run_abc"

    def test_mark_running_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self._fresh(tmp)
            mark_running(tmp, state, 0, 0, "run_abc")
            reloaded = load(tmp)
        assert reloaded["cases"][0]["trials"][0]["status"] == RUNNING
        assert reloaded["cases"][0]["trials"][0]["run_id"] == "run_abc"

    def test_mark_completed_sets_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self._fresh(tmp)
            mark_running(tmp, state, 0, 0, "run_abc")
            mark_completed(tmp, state, 0, 0)
            assert state["cases"][0]["trials"][0]["status"] == COMPLETED

    def test_mark_completed_sets_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self._fresh(tmp)
            mark_completed(tmp, state, 0, 0)
            assert state["cases"][0]["trials"][0]["completed_at"] is not None

    def test_mark_failed_sets_status_and_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self._fresh(tmp)
            mark_failed(tmp, state, 1, 1, ValueError("boom"))
            assert state["cases"][1]["trials"][1]["status"] == FAILED
            assert "boom" in state["cases"][1]["trials"][1]["error"]

    def test_mark_failed_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self._fresh(tmp)
            mark_failed(tmp, state, 0, 1, RuntimeError("oops"))
            reloaded = load(tmp)
        assert reloaded["cases"][0]["trials"][1]["status"] == FAILED
        assert "oops" in reloaded["cases"][0]["trials"][1]["error"]

    def test_mutations_do_not_affect_other_trials(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self._fresh(tmp)
            mark_completed(tmp, state, 0, 0)
            # trial (0,1), (1,0), (1,1) must still be pending
            assert state["cases"][0]["trials"][1]["status"] == PENDING
            assert state["cases"][1]["trials"][0]["status"] == PENDING


# ---------------------------------------------------------------------------
# pending_trials
# ---------------------------------------------------------------------------

class TestPendingTrials:
    def test_all_pending_initially(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "s", _seq_params(), _make_cases(2), trials=2)
        result = pending_trials(state)
        assert sorted(result) == [(0, 0), (0, 1), (1, 0), (1, 1)]

    def test_running_is_included(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "s", _seq_params(), _make_cases(1), trials=2)
            mark_running(tmp, state, 0, 0, "r1")
        result = pending_trials(state)
        assert (0, 0) in result

    def test_completed_excluded(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "s", _seq_params(), _make_cases(1), trials=2)
            mark_completed(tmp, state, 0, 0)
        result = pending_trials(state)
        assert (0, 0) not in result
        assert (0, 1) in result

    def test_failed_excluded(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "s", _seq_params(), _make_cases(1), trials=2)
            mark_failed(tmp, state, 0, 0, "err")
        result = pending_trials(state)
        assert (0, 0) not in result

    def test_empty_when_all_completed(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "s", _seq_params(), _make_cases(1), trials=1)
            mark_completed(tmp, state, 0, 0)
        assert pending_trials(state) == []

    def test_order_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = create(tmp, "s", _seq_params(), _make_cases(2), trials=2)
            mark_completed(tmp, state, 0, 0)
        result = pending_trials(state)
        assert result == [(0, 1), (1, 0), (1, 1)]
