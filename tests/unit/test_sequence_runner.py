"""Unit tests for fts_framework.sequence.runner.

run_campaign is replaced with a lightweight fake so no real HTTP calls are
made.  Tests focus on resume logic: run_id reuse for RUNNING trials, skipping
COMPLETED/FAILED trials, and correct sequencing.
"""

import os
import tempfile
import threading
import time

import pytest

from fts_framework.sequence import state as seq_state
from fts_framework.sequence.state import create, mark_running, mark_completed, mark_failed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seq_params(baseline="config/x.yaml", label=None):
    return {
        "baseline_config_path": baseline,
        "label": label,
        "sweep_mode": "cartesian",
    }


def _make_cases(n):
    return [{"transfer.max_files": (i + 1) * 100} for i in range(n)]


def _base_config():
    return {
        "run": {"test_label": "test", "run_id": None},
        "fts": {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
        "tokens": {"fts_submit": "t", "source_read": "t", "dest_write": "t"},
        "transfer": {
            "source_pfns_file": "sources.txt",
            "dst_prefix": "https://dst.example.org/data",
            "checksum_algorithm": "adler32",
            "verify_checksum": "both",
            "overwrite": False,
            "max_files": None,
            "chunk_size": 200,
            "priority": 3,
            "activity": None,
            "job_metadata": {},
            "unmanaged_tokens": False,
        },
        "concurrency": {"want_digest_workers": 1},
        "submission": {"scan_window_s": 300},
        "polling": {
            "initial_interval_s": 0,
            "backoff_multiplier": 1.0,
            "max_interval_s": 0,
            "campaign_timeout_s": 3600,
        },
        "cleanup": {"before": False, "after": False},
        "retry": {"fts_retry_max": 0, "framework_retry_max": 0,
                  "min_success_threshold": 0.0},
        "output": {
            "base_dir": "runs",
            "timeseries_bucket_s": 60,
            "reports": {
                "console": False, "json": False, "markdown": False,
                "html": False, "csv": False, "timeseries_csv": False,
            },
        },
    }


def _install_mocks(monkeypatch, tmp_path, run_campaign_fn=None):
    """Patch run_sequence dependencies in sequence.runner's namespace."""
    import fts_framework.sequence.runner as runner_mod
    import fts_framework.config.loader as loader_mod

    monkeypatch.setattr(
        loader_mod, "load",
        lambda path, **kwargs: _base_config(),
    )
    monkeypatch.setattr(
        runner_mod.seq_loader, "load",
        lambda path: {
            "baseline_config_path": "config/x.yaml",
            "label": "test",
            "output_base_dir": str(tmp_path),
            "sweep_mode": "cartesian",
            "trials": 1,
            "cases": [{"transfer.max_files": 100}],
        },
    )
    if run_campaign_fn is None:
        run_campaign_fn = lambda config, runs_dir="runs": None
    monkeypatch.setattr(runner_mod, "run_campaign", run_campaign_fn)
    monkeypatch.setattr(runner_mod.seq_reporter, "generate_summary",
                        lambda seq_dir, state, runs_dir="runs": None)
    monkeypatch.setattr(runner_mod, "_write_params_copy", lambda seq_dir, params_file: None)


# ---------------------------------------------------------------------------
# Resume: RUNNING trial reuses existing run_id
# ---------------------------------------------------------------------------

class TestResumeRunId:
    def test_pending_trial_gets_fresh_run_id(self, tmp_path, monkeypatch):
        """A PENDING trial always receives a freshly generated run_id."""
        seen_run_ids = []

        def _capture(config, runs_dir="runs"):
            seen_run_ids.append(config["run"]["run_id"])

        _install_mocks(monkeypatch, tmp_path, run_campaign_fn=_capture)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))

        assert len(seen_run_ids) == 1
        assert seen_run_ids[0] is not None

    def test_run_id_prefixed_with_case_and_trial(self, tmp_path, monkeypatch):
        """Fresh run_id must start with c{case:02d}_t{trial:02d}_ for readability."""
        seen_run_ids = []

        def _capture(config, runs_dir="runs"):
            seen_run_ids.append(config["run"]["run_id"])

        _install_mocks(monkeypatch, tmp_path, run_campaign_fn=_capture)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))

        assert seen_run_ids[0].startswith("c00_t00_")

    def test_running_trial_reuses_stored_run_id(self, tmp_path, monkeypatch):
        """A RUNNING trial (interrupted mid-campaign) must reuse its stored
        run_id so run_campaign() can resume the partial campaign."""
        seen_run_ids = []

        def _capture(config, runs_dir="runs"):
            seen_run_ids.append(config["run"]["run_id"])

        _install_mocks(monkeypatch, tmp_path, run_campaign_fn=_capture)

        # Create a sequence directory with one trial left in RUNNING state
        seq_dir = os.path.join(str(tmp_path), "my_sequence")
        os.makedirs(os.path.join(seq_dir, "reports"), exist_ok=True)
        state = create(seq_dir, "seq_001", _seq_params(), _make_cases(1), trials=1)
        mark_running(seq_dir, state, 0, 0, "existing-run-id-abc")

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", resume_dir=seq_dir, runs_dir=str(tmp_path))

        assert len(seen_run_ids) == 1
        assert seen_run_ids[0] == "existing-run-id-abc"

    def test_running_trial_without_run_id_gets_fresh(self, tmp_path, monkeypatch):
        """A RUNNING trial with no stored run_id (corrupted state) generates
        a fresh run_id rather than passing None to run_campaign."""
        seen_run_ids = []

        def _capture(config, runs_dir="runs"):
            seen_run_ids.append(config["run"]["run_id"])

        _install_mocks(monkeypatch, tmp_path, run_campaign_fn=_capture)

        seq_dir = os.path.join(str(tmp_path), "my_sequence")
        os.makedirs(os.path.join(seq_dir, "reports"), exist_ok=True)
        state = create(seq_dir, "seq_001", _seq_params(), _make_cases(1), trials=1)
        # Manually set status=running but leave run_id as None
        state["cases"][0]["trials"][0]["status"] = seq_state.RUNNING
        from fts_framework.sequence.state import _write
        _write(seq_dir, state)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", resume_dir=seq_dir, runs_dir=str(tmp_path))

        assert len(seen_run_ids) == 1
        assert seen_run_ids[0] is not None


# ---------------------------------------------------------------------------
# Resume: completed and failed trials are skipped
# ---------------------------------------------------------------------------

class TestResumeSkipping:
    def test_completed_trial_not_retried(self, tmp_path, monkeypatch):
        call_count = [0]

        def _count(config, runs_dir="runs"):
            call_count[0] += 1

        _install_mocks(monkeypatch, tmp_path, run_campaign_fn=_count)

        seq_dir = os.path.join(str(tmp_path), "my_sequence")
        os.makedirs(os.path.join(seq_dir, "reports"), exist_ok=True)
        state = create(seq_dir, "seq_001", _seq_params(), _make_cases(1), trials=1)
        mark_running(seq_dir, state, 0, 0, "run-done")
        mark_completed(seq_dir, state, 0, 0)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", resume_dir=seq_dir, runs_dir=str(tmp_path))

        assert call_count[0] == 0

    def test_failed_trial_not_retried(self, tmp_path, monkeypatch):
        call_count = [0]

        def _count(config, runs_dir="runs"):
            call_count[0] += 1

        _install_mocks(monkeypatch, tmp_path, run_campaign_fn=_count)

        seq_dir = os.path.join(str(tmp_path), "my_sequence")
        os.makedirs(os.path.join(seq_dir, "reports"), exist_ok=True)
        state = create(seq_dir, "seq_001", _seq_params(), _make_cases(1), trials=1)
        mark_running(seq_dir, state, 0, 0, "run-err")
        mark_failed(seq_dir, state, 0, 0, RuntimeError("oops"))

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", resume_dir=seq_dir, runs_dir=str(tmp_path))

        assert call_count[0] == 0

    def test_only_pending_trials_run_on_partial_resume(self, tmp_path, monkeypatch):
        """3 trials: first completed, second running (crashed), third pending.
        Resume should run trials 2 and 3 only."""
        seen_run_ids = []

        def _capture(config, runs_dir="runs"):
            seen_run_ids.append(config["run"]["run_id"])

        import fts_framework.sequence.runner as runner_mod
        import fts_framework.config.loader as loader_mod
        monkeypatch.setattr(loader_mod, "load", lambda path, **kwargs: _base_config())
        monkeypatch.setattr(
            runner_mod.seq_loader, "load",
            lambda path: {
                "baseline_config_path": "config/x.yaml",
                "label": "test",
                "output_base_dir": str(tmp_path),
                "sweep_mode": "cartesian",
                "trials": 3,
                "cases": [{"transfer.max_files": 100}],
            },
        )
        monkeypatch.setattr(runner_mod, "run_campaign", _capture)
        monkeypatch.setattr(runner_mod.seq_reporter, "generate_summary",
                            lambda seq_dir, state, runs_dir="runs": None)

        seq_dir = os.path.join(str(tmp_path), "my_sequence")
        os.makedirs(os.path.join(seq_dir, "reports"), exist_ok=True)
        state = create(seq_dir, "seq_001", _seq_params(), _make_cases(1), trials=3)
        mark_running(seq_dir, state, 0, 0, "run-done")
        mark_completed(seq_dir, state, 0, 0)
        mark_running(seq_dir, state, 0, 1, "run-crashed")
        # trial 2 remains PENDING

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", resume_dir=seq_dir, runs_dir=str(tmp_path))

        assert len(seen_run_ids) == 2
        # Crashed trial reuses its run_id; pending trial gets a fresh one
        assert seen_run_ids[0] == "run-crashed"
        assert seen_run_ids[1] != "run-crashed"
        assert seen_run_ids[1] is not None


# ---------------------------------------------------------------------------
# Per-trial OIDC token re-fetch when dst_prefix is overridden
# ---------------------------------------------------------------------------

class TestOidcPerTrialRefresh:
    def test_dest_write_token_refreshed_when_dst_prefix_overridden(
        self, tmp_path, monkeypatch
    ):
        """When a case overrides transfer.dst_prefix and dest_write scope uses
        {dst_prefix_path}, a fresh OIDC token must be fetched for that trial."""
        import fts_framework.sequence.runner as runner_mod
        import fts_framework.config.loader as loader_mod
        import fts_framework.auth.oidc as oidc_mod

        fetch_calls = []

        def _fake_fetch(endpoint, client_id, client_secret, scope, ssl_verify,
                        audience=None):
            fetch_calls.append(scope)
            return "tok_for_{}".format(scope.replace(" ", "_"))

        monkeypatch.setattr(oidc_mod, "fetch_token", _fake_fetch)

        base_cfg = _base_config()
        base_cfg["transfer"]["dst_prefix"] = "https://dst.example.org/base"
        base_cfg["transfer"]["source_prefix"] = None
        base_cfg["oidc"] = {
            "enabled": True,
            "env_file": str(tmp_path / "nonexistent.env"),
            "roles": {
                "fts_submit": {
                    "token_endpoint":    "https://iam.example.org/token",
                    "client_id_var":     "FTS_CLIENT_ID",
                    "client_secret_var": "FTS_CLIENT_SECRET",
                    "scope":             "openid profile",
                },
                "source_read": {
                    "token_endpoint":    "https://iam.example.org/token",
                    "client_id_var":     "SRC_CLIENT_ID",
                    "client_secret_var": "SRC_CLIENT_SECRET",
                    "scope":             "openid storage.read:/",
                },
                "dest_write": {
                    "token_endpoint":    "https://iam.example.org/token",
                    "client_id_var":     "DST_CLIENT_ID",
                    "client_secret_var": "DST_CLIENT_SECRET",
                    "scope":             "openid storage.modify:{dst_prefix_path}",
                },
            },
        }
        # Pre-populate tokens as if baseline load already ran
        base_cfg["tokens"] = {
            "fts_submit":  "baseline_fts_tok",
            "source_read": "baseline_src_tok",
            "dest_write":  "baseline_dst_tok",
        }

        monkeypatch.setenv("FTS_CLIENT_ID",     "cid")
        monkeypatch.setenv("FTS_CLIENT_SECRET", "csec")
        monkeypatch.setenv("SRC_CLIENT_ID",     "cid")
        monkeypatch.setenv("SRC_CLIENT_SECRET", "csec")
        monkeypatch.setenv("DST_CLIENT_ID",     "cid")
        monkeypatch.setenv("DST_CLIENT_SECRET", "csec")

        trial_configs = []

        def _capture(config, runs_dir="runs"):
            trial_configs.append(config["tokens"]["dest_write"])

        monkeypatch.setattr(
            runner_mod.seq_loader, "load",
            lambda path: {
                "baseline_config_path": "config/x.yaml",
                "label": "test",
                "output_base_dir": str(tmp_path),
                "sweep_mode": "cartesian",
                "trials": 1,
                "cases": [
                    {"transfer.dst_prefix": "https://dst.example.org/case0"},
                    {"transfer.dst_prefix": "https://dst.example.org/case1"},
                ],
            },
        )
        monkeypatch.setattr(loader_mod, "load", lambda path, **kwargs: base_cfg)
        monkeypatch.setattr(runner_mod, "run_campaign", _capture)
        monkeypatch.setattr(runner_mod.seq_reporter, "generate_summary",
                            lambda seq_dir, state, runs_dir="runs": None)
        monkeypatch.setattr(runner_mod, "_write_params_copy",
                            lambda seq_dir, params_file: None)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))

        # Each trial must have fetched a fresh dest_write token with the
        # overridden dst_prefix path in the scope.
        assert len(trial_configs) == 2
        assert "case0" in trial_configs[0]
        assert "case1" in trial_configs[1]
        assert trial_configs[0] != trial_configs[1]

    def test_tokens_always_refreshed_per_trial(
        self, tmp_path, monkeypatch
    ):
        """OIDC tokens are refreshed unconditionally for every trial, even when
        the sweep parameter does not affect a scope template — prevents 403s
        from token expiry mid-sequence."""
        import fts_framework.sequence.runner as runner_mod
        import fts_framework.config.loader as loader_mod
        import fts_framework.auth.oidc as oidc_mod

        fetch_calls = []
        monkeypatch.setattr(oidc_mod, "fetch_token",
                            lambda *a, **kw: fetch_calls.append(1) or "tok")

        base_cfg = _base_config()
        base_cfg["transfer"]["dst_prefix"] = "https://dst.example.org/base"
        base_cfg["oidc"] = {
            "enabled": True,
            "env_file": str(tmp_path / "nonexistent.env"),
            "roles": {
                "dest_write": {
                    "token_endpoint":    "https://iam.example.org/token",
                    "client_id_var":     "DST_CLIENT_ID",
                    "client_secret_var": "DST_CLIENT_SECRET",
                    "scope":             "openid storage.modify:/",  # no template
                },
            },
        }
        base_cfg["tokens"] = {
            "fts_submit":  "fts_tok",
            "source_read": "src_tok",
            "dest_write":  "dst_tok",
        }

        monkeypatch.setenv("DST_CLIENT_ID",     "cid")
        monkeypatch.setenv("DST_CLIENT_SECRET", "csec")

        monkeypatch.setattr(
            runner_mod.seq_loader, "load",
            lambda path: {
                "baseline_config_path": "config/x.yaml",
                "label": "test",
                "output_base_dir": str(tmp_path),
                "sweep_mode": "cartesian",
                "trials": 1,
                "cases": [
                    {"transfer.max_files": 100},
                    {"transfer.max_files": 200},
                ],
            },
        )
        monkeypatch.setattr(loader_mod, "load", lambda path, **kwargs: base_cfg)
        monkeypatch.setattr(runner_mod, "run_campaign", lambda config, runs_dir="runs": None)
        monkeypatch.setattr(runner_mod.seq_reporter, "generate_summary",
                            lambda seq_dir, state, runs_dir="runs": None)
        monkeypatch.setattr(runner_mod, "_write_params_copy",
                            lambda seq_dir, params_file: None)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))

        # One fetch per trial (2 cases × 1 trial = 2), even though max_files
        # does not affect any scope template.
        assert len(fetch_calls) == 2


# ---------------------------------------------------------------------------
# destination_naming as a sweep parameter (cartesian and zip)
# ---------------------------------------------------------------------------

class TestDestinationNamingSweep:
    """Sweeping transfer.destination_naming requires no sequence-module code
    changes — generate_cases()/apply_override() are key-agnostic — but this
    proves the interaction actually works end-to-end rather than just by
    inspection."""

    def test_zip_mode_pairs_destination_naming_with_max_files(self):
        from fts_framework.sequence.loader import generate_cases
        sweep = {
            "mode": "zip",
            "parameters": {
                "transfer.destination_naming": ["sequential", "uuid"],
                "transfer.max_files": [10, 20],
            },
        }
        cases = generate_cases(sweep)
        assert cases == [
            {"transfer.destination_naming": "sequential", "transfer.max_files": 10},
            {"transfer.destination_naming": "uuid", "transfer.max_files": 20},
        ]

    def test_cartesian_mode_crosses_destination_naming_with_max_files(self):
        from fts_framework.sequence.loader import generate_cases
        sweep = {
            "mode": "cartesian",
            "parameters": {
                "transfer.destination_naming": ["sequential", "uuid"],
                "transfer.max_files": [10, 20],
            },
        }
        cases = generate_cases(sweep)
        assert len(cases) == 4
        naming_values = {c["transfer.destination_naming"] for c in cases}
        assert naming_values == {"sequential", "uuid"}
        # every (naming, max_files) combination present exactly once
        combos = {(c["transfer.destination_naming"], c["transfer.max_files"]) for c in cases}
        assert combos == {
            ("sequential", 10), ("sequential", 20),
            ("uuid", 10), ("uuid", 20),
        }

    def test_build_trial_config_applies_destination_naming_override(self):
        from fts_framework.sequence.runner import _build_trial_config
        baseline = _base_config()
        baseline["transfer"]["destination_naming"] = "sequential"  # explicit default

        trial_uuid = _build_trial_config(baseline, {"transfer.destination_naming": "uuid"})
        trial_seq = _build_trial_config(baseline, {"transfer.destination_naming": "sequential"})

        assert trial_uuid["transfer"]["destination_naming"] == "uuid"
        assert trial_seq["transfer"]["destination_naming"] == "sequential"
        # baseline itself must not be mutated (deep-copy contract)
        assert baseline["transfer"]["destination_naming"] == "sequential"

    def test_build_trial_config_sets_new_key_when_baseline_lacks_it(self):
        """A baseline config predating this feature has no destination_naming
        key at all; apply_override must still be able to set it (only the
        intermediate section, 'transfer', needs to pre-exist)."""
        from fts_framework.sequence.runner import _build_trial_config
        baseline = _base_config()
        assert "destination_naming" not in baseline["transfer"]

        trial = _build_trial_config(baseline, {"transfer.destination_naming": "uuid"})
        assert trial["transfer"]["destination_naming"] == "uuid"

    def test_per_case_config_produces_correct_planner_naming(self):
        """End-to-end: a zip-swept destination_naming value, once applied to
        a trial config via _build_trial_config, actually changes the
        destination.planner.plan() naming style."""
        from fts_framework.sequence.loader import generate_cases
        from fts_framework.sequence.runner import _build_trial_config
        from fts_framework.destination.planner import plan

        sweep = {
            "mode": "zip",
            "parameters": {"transfer.destination_naming": ["sequential", "uuid"]},
        }
        cases = generate_cases(sweep)
        baseline = _base_config()
        baseline["transfer"]["preserve_extension"] = False
        pfns = ["https://src.example.org/f{:02d}.dat".format(i) for i in range(3)]

        trial_configs = [_build_trial_config(baseline, case) for case in cases]
        mappings = [plan(pfns, cfg) for cfg in trial_configs]

        sequential_dsts = [dst for src, dst in mappings[0]]
        uuid_dsts = [dst for src, dst in mappings[1]]

        assert all(d.endswith("testfile_{:06d}".format(i)) for i, d in enumerate(sequential_dsts))
        assert not any("testfile_000000" in d or "testfile_000001" in d for d in uuid_dsts)
        assert len(set(uuid_dsts)) == 3


# ---------------------------------------------------------------------------
# parallel_cases execution mode
# ---------------------------------------------------------------------------

def _install_parallel_mocks(monkeypatch, tmp_path, n_cases, trials, max_workers,
                             run_campaign_fn, destination_naming="uuid",
                             execution_mode="parallel_cases", execution_order=None):
    """Like _install_mocks, but for multi-case parallel_cases scenarios with
    a configurable transfer.destination_naming (the guardrail this mode
    validates)."""
    import fts_framework.sequence.runner as runner_mod
    import fts_framework.config.loader as loader_mod

    def _load_baseline(path, **kwargs):
        cfg = _base_config()
        cfg["transfer"]["destination_naming"] = destination_naming
        return cfg

    seq_params = {
        "baseline_config_path": "config/x.yaml",
        "label": "test",
        "output_base_dir": str(tmp_path),
        "sweep_mode": "cartesian",
        "trials": trials,
        "cases": _make_cases(n_cases),
        "execution_mode": execution_mode,
        "max_workers": max_workers,
    }
    if execution_order is not None:
        seq_params["execution_order"] = execution_order

    monkeypatch.setattr(loader_mod, "load", _load_baseline)
    monkeypatch.setattr(runner_mod.seq_loader, "load", lambda path: seq_params)
    monkeypatch.setattr(runner_mod, "run_campaign", run_campaign_fn)
    monkeypatch.setattr(runner_mod.seq_reporter, "generate_summary",
                        lambda seq_dir, state, runs_dir="runs": None)
    monkeypatch.setattr(runner_mod, "_write_params_copy", lambda seq_dir, params_file: None)


class TestParallelCasesExecution:
    def test_all_pending_trials_run_exactly_once(self, tmp_path, monkeypatch):
        seen = []
        lock = threading.Lock()

        def _capture(config, runs_dir="runs"):
            with lock:
                seen.append(config["run"]["run_id"])

        _install_parallel_mocks(monkeypatch, tmp_path, n_cases=4, trials=2,
                                 max_workers=3, run_campaign_fn=_capture)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))

        assert len(seen) == 8
        assert len(set(seen)) == 8
        expected_prefixes = {
            "c{:02d}_t{:02d}_".format(c, t) for c in range(4) for t in range(2)
        }
        got_prefixes = {run_id[:8] for run_id in seen}
        assert got_prefixes == expected_prefixes

    def test_trials_within_a_case_run_in_ascending_order(self, tmp_path, monkeypatch):
        order_by_case = {}
        lock = threading.Lock()

        def _capture(config, runs_dir="runs"):
            run_id = config["run"]["run_id"]
            case_index = int(run_id[1:3])
            trial_index = int(run_id[5:7])
            time.sleep(0.01)
            with lock:
                order_by_case.setdefault(case_index, []).append(trial_index)

        _install_parallel_mocks(monkeypatch, tmp_path, n_cases=3, trials=4,
                                 max_workers=3, run_campaign_fn=_capture)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))

        assert len(order_by_case) == 3
        for case_index, trial_order in order_by_case.items():
            assert trial_order == list(range(4)), (
                "case {} trials ran out of order: {}".format(case_index, trial_order)
            )

    def test_concurrency_never_exceeds_max_workers(self, tmp_path, monkeypatch):
        active = [0]
        peak = [0]
        lock = threading.Lock()

        def _capture(config, runs_dir="runs"):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.05)
            with lock:
                active[0] -= 1

        _install_parallel_mocks(monkeypatch, tmp_path, n_cases=6, trials=1,
                                 max_workers=2, run_campaign_fn=_capture)

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))

        assert 1 < peak[0] <= 2

    def test_sequential_destination_naming_blocked(self, tmp_path, monkeypatch):
        """The default 'sequential' naming would let concurrently-running
        cases plan colliding destinations, so parallel_cases mode must
        refuse to start."""
        called = []

        def _capture(config, runs_dir="runs"):
            called.append(config)

        _install_parallel_mocks(monkeypatch, tmp_path, n_cases=2, trials=1,
                                 max_workers=2, run_campaign_fn=_capture,
                                 destination_naming="sequential")

        from fts_framework.sequence.runner import run_sequence
        from fts_framework.exceptions import ConfigError
        with pytest.raises(ConfigError, match="destination_naming"):
            run_sequence("params.yaml", runs_dir=str(tmp_path))

        assert called == []

    def test_default_destination_naming_absent_key_blocked(self, tmp_path, monkeypatch):
        """A baseline config with no destination_naming key at all resolves
        to 'sequential' (the config-loader default) and must also be
        blocked, not just an explicit 'sequential' value."""
        import fts_framework.sequence.runner as runner_mod
        import fts_framework.config.loader as loader_mod

        # _base_config() doesn't set transfer.destination_naming at all,
        # matching a real baseline config that predates this feature.
        monkeypatch.setattr(loader_mod, "load", lambda path, **kwargs: _base_config())
        monkeypatch.setattr(
            runner_mod.seq_loader, "load",
            lambda path: {
                "baseline_config_path": "config/x.yaml",
                "label": "test",
                "output_base_dir": str(tmp_path),
                "sweep_mode": "cartesian",
                "trials": 1,
                "cases": _make_cases(2),
                "execution_mode": "parallel_cases",
                "max_workers": 2,
            },
        )
        monkeypatch.setattr(runner_mod, "run_campaign",
                            lambda config, runs_dir="runs": None)
        monkeypatch.setattr(runner_mod.seq_reporter, "generate_summary",
                            lambda seq_dir, state, runs_dir="runs": None)
        monkeypatch.setattr(runner_mod, "_write_params_copy",
                            lambda seq_dir, params_file: None)

        from fts_framework.sequence.runner import run_sequence
        from fts_framework.exceptions import ConfigError
        with pytest.raises(ConfigError, match="destination_naming"):
            run_sequence("params.yaml", runs_dir=str(tmp_path))

    def test_uuid_destination_naming_allowed(self, tmp_path, monkeypatch):
        seen = []

        def _capture(config, runs_dir="runs"):
            seen.append(config["run"]["run_id"])

        _install_parallel_mocks(monkeypatch, tmp_path, n_cases=2, trials=1,
                                 max_workers=2, run_campaign_fn=_capture,
                                 destination_naming="uuid")

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))
        assert len(seen) == 2

    def test_date_destination_naming_allowed(self, tmp_path, monkeypatch):
        seen = []

        def _capture(config, runs_dir="runs"):
            seen.append(config["run"]["run_id"])

        _install_parallel_mocks(monkeypatch, tmp_path, n_cases=2, trials=1,
                                 max_workers=2, run_campaign_fn=_capture,
                                 destination_naming="date")

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))
        assert len(seen) == 2

    def test_sequential_mode_still_default_and_unaffected(self, tmp_path, monkeypatch):
        """Sanity check: omitting sequence.execution entirely (as every
        other test in this file does) still dispatches to the serial path,
        even when multiple cases are present."""
        seen = []

        def _capture(config, runs_dir="runs"):
            seen.append(config["run"]["run_id"])

        _install_parallel_mocks(monkeypatch, tmp_path, n_cases=3, trials=1,
                                 max_workers=4, run_campaign_fn=_capture,
                                 destination_naming="sequential",
                                 execution_mode="sequential")

        from fts_framework.sequence.runner import run_sequence
        run_sequence("params.yaml", runs_dir=str(tmp_path))

        assert [run_id[:8] for run_id in seen] == ["c00_t00_", "c01_t00_", "c02_t00_"]


class _FakeInterruptibleThread:
    """Stand-in for threading.Thread whose join() raises KeyboardInterrupt
    immediately, simulating Ctrl-C while the main thread waits on workers."""

    def __init__(self, target=None, daemon=None):
        self._target = target

    def start(self):
        pass

    def is_alive(self):
        return True

    def join(self, timeout=None):
        raise KeyboardInterrupt()


class TestParallelCasesInterrupt:
    def test_keyboard_interrupt_prints_hint_and_propagates(self, tmp_path, monkeypatch, capsys):
        import fts_framework.sequence.runner as runner_mod

        monkeypatch.setattr(runner_mod.threading, "Thread", _FakeInterruptibleThread)

        state = {
            "cases": [{
                "case_index": 0,
                "params": {},
                "trials": [{"trial_index": 0, "status": "pending", "run_id": None}],
            }],
        }
        pending = [(0, 0)]

        with pytest.raises(KeyboardInterrupt):
            runner_mod._run_parallel_cases(
                pending, state, str(tmp_path), str(tmp_path), _base_config(),
                "params.yaml", max_workers=1,
            )

        captured = capsys.readouterr()
        assert "Interrupted" in captured.err
        assert "--resume" in captured.err
