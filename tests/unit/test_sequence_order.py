"""
Unit tests for sequence trial execution order (``sequence.execution.order``).

trial_major (default in sequential mode) runs one round of every case before
the next round; case_major runs all trials of a case before the next case.
parallel_cases is case-major only: it defaults to case_major and rejects an
explicit trial_major.
"""

import os

import pytest
import yaml

from fts_framework.exceptions import ConfigError
from fts_framework.sequence.loader import load
from fts_framework.sequence.state import (
    create,
    mark_completed,
    mark_running,
    pending_trials,
    skip_cases_from,
)
from tests.unit.test_sequence_runner import (
    _install_parallel_mocks,
    _make_cases,
    _seq_params,
)


# ---------------------------------------------------------------------------
# Loader resolution
# ---------------------------------------------------------------------------

def _load(tmp_path, execution=None):
    baseline = {
        "fts": {"endpoint": "https://fts.example.org:8446"},
        "tokens": {"fts_submit": "t", "source_read": "t", "dest_write": "t"},
        "transfer": {"source_pfns_file": "s.txt", "dst_prefix": "https://dst.example.org"},
    }
    bp = str(tmp_path / "baseline.yaml")
    with open(bp, "w") as fh:
        yaml.dump(baseline, fh)
    seq = {"trials": 2, "sweep": {"parameters": {"transfer.copy_mode": ["pull", "push"]}}}
    if execution is not None:
        seq["execution"] = execution
    pp = str(tmp_path / "params.yaml")
    with open(pp, "w") as fh:
        yaml.dump({"baseline_config": bp, "sequence": seq}, fh)
    return load(pp)


class TestLoaderOrder:
    def test_sequential_defaults_to_trial_major(self, tmp_path):
        assert _load(tmp_path)["execution_order"] == "trial_major"

    def test_sequential_explicit_mode_defaults_to_trial_major(self, tmp_path):
        result = _load(tmp_path, {"mode": "sequential"})
        assert result["execution_order"] == "trial_major"

    @pytest.mark.parametrize("order", ["trial_major", "case_major"])
    def test_sequential_explicit_order(self, tmp_path, order):
        assert _load(tmp_path, {"order": order})["execution_order"] == order

    def test_parallel_cases_defaults_to_case_major(self, tmp_path):
        result = _load(tmp_path, {"mode": "parallel_cases"})
        assert result["execution_order"] == "case_major"

    def test_parallel_cases_explicit_case_major_allowed(self, tmp_path):
        result = _load(tmp_path, {"mode": "parallel_cases", "order": "case_major"})
        assert result["execution_order"] == "case_major"

    def test_parallel_cases_rejects_trial_major(self, tmp_path):
        with pytest.raises(ConfigError, match="trial_major is not supported with mode=parallel_cases"):
            _load(tmp_path, {"mode": "parallel_cases", "order": "trial_major"})

    @pytest.mark.parametrize("bad", ["round_robin", "TRIAL_MAJOR", 1])
    def test_invalid_order_rejected(self, tmp_path, bad):
        with pytest.raises(ConfigError, match="sequence.execution.order"):
            _load(tmp_path, {"order": bad})


# ---------------------------------------------------------------------------
# pending_trials ordering
# ---------------------------------------------------------------------------

def _state(tmp_path, n_cases=3, trials=2):
    seq_dir = str(tmp_path / "seq")
    os.makedirs(seq_dir)
    return seq_dir, create(seq_dir, "seq_001", _seq_params(), _make_cases(n_cases), trials=trials)


class TestPendingTrialsOrder:
    def test_trial_major(self, tmp_path):
        _, state = _state(tmp_path)
        assert pending_trials(state, order="trial_major") == [
            (0, 0), (1, 0), (2, 0), (0, 1), (1, 1), (2, 1),
        ]

    def test_case_major(self, tmp_path):
        _, state = _state(tmp_path)
        assert pending_trials(state, order="case_major") == [
            (0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1),
        ]

    def test_function_default_is_case_major(self, tmp_path):
        _, state = _state(tmp_path)
        assert pending_trials(state) == pending_trials(state, order="case_major")

    def test_trial_major_mixed_statuses(self, tmp_path):
        seq_dir, state = _state(tmp_path)
        for c in range(3):
            mark_running(seq_dir, state, c, 0, "r{}".format(c))
            mark_completed(seq_dir, state, c, 0)
        mark_running(seq_dir, state, 1, 1, "crashed")
        skip_cases_from(seq_dir, state, 2)  # skips pending c2t1
        # Round 0 done; c1t1 interrupted (running) is retried; c2t1 skipped.
        assert pending_trials(state, order="trial_major") == [(0, 1), (1, 1)]


# ---------------------------------------------------------------------------
# Runner dispatch
# ---------------------------------------------------------------------------

def _run(monkeypatch, tmp_path, execution_order, n_cases=3, trials=2):
    seen = []

    def _capture(config, runs_dir="runs"):
        seen.append(config["run"]["run_id"][:7])

    _install_parallel_mocks(
        monkeypatch, tmp_path, n_cases=n_cases, trials=trials, max_workers=1,
        run_campaign_fn=_capture, destination_naming="sequential",
        execution_mode="sequential", execution_order=execution_order,
    )
    from fts_framework.sequence.runner import run_sequence
    run_sequence("params.yaml", runs_dir=str(tmp_path))
    return seen


class TestRunnerOrder:
    def test_trial_major_runs_rounds(self, monkeypatch, tmp_path):
        assert _run(monkeypatch, tmp_path, "trial_major") == [
            "c00_t00", "c01_t00", "c02_t00", "c00_t01", "c01_t01", "c02_t01",
        ]

    def test_case_major_runs_cases(self, monkeypatch, tmp_path):
        assert _run(monkeypatch, tmp_path, "case_major") == [
            "c00_t00", "c00_t01", "c01_t00", "c01_t01", "c02_t00", "c02_t01",
        ]

    def test_missing_order_defaults_to_trial_major(self, monkeypatch, tmp_path):
        assert _run(monkeypatch, tmp_path, None)[:3] == ["c00_t00", "c01_t00", "c02_t00"]

    def test_resume_mid_round_continues_round(self, monkeypatch, tmp_path):
        seen = []

        def _capture(config, runs_dir="runs"):
            seen.append(config["run"]["run_id"])

        _install_parallel_mocks(
            monkeypatch, tmp_path, n_cases=2, trials=2, max_workers=1,
            run_campaign_fn=_capture, destination_naming="sequential",
            execution_mode="sequential", execution_order="trial_major",
        )
        seq_dir = str(tmp_path / "resume_seq")
        os.makedirs(os.path.join(seq_dir, "reports"))
        state = create(seq_dir, "seq_001", _seq_params(), _make_cases(2), trials=2)
        mark_running(seq_dir, state, 0, 0, "done-00")
        mark_completed(seq_dir, state, 0, 0)
        mark_running(seq_dir, state, 1, 0, "crashed-10")

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", resume_dir=seq_dir, runs_dir=str(tmp_path))

        assert seen[0] == "crashed-10"
        assert [r[:7] for r in seen[1:]] == ["c00_t01", "c01_t01"]
