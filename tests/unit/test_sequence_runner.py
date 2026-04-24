"""Unit tests for fts_framework.sequence.runner.

run_campaign is replaced with a lightweight fake so no real HTTP calls are
made.  Tests focus on resume logic: run_id reuse for RUNNING trials, skipping
COMPLETED/FAILED trials, and correct sequencing.
"""

import os
import tempfile

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

        monkeypatch.setenv("DST_CLIENT_ID",    "cid")
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

    def test_token_not_refreshed_when_no_scope_template(
        self, tmp_path, monkeypatch
    ):
        """If the dest_write scope has no {dst_prefix_path}, no re-fetch on
        dst_prefix override."""
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

        monkeypatch.setattr(
            runner_mod.seq_loader, "load",
            lambda path: {
                "baseline_config_path": "config/x.yaml",
                "label": "test",
                "output_base_dir": str(tmp_path),
                "sweep_mode": "cartesian",
                "trials": 1,
                "cases": [{"transfer.dst_prefix": "https://dst.example.org/other"}],
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

        assert len(fetch_calls) == 0
