"""
Unit tests for fts_framework.runner.

All external I/O is replaced with lightweight fakes.  No real HTTP calls,
no real filesystem writes (runs_dir is a tmp_path fixture).
"""

import os
import json
import pytest

from collections import OrderedDict

from fts_framework.exceptions import TokenExpiredError


# ---------------------------------------------------------------------------
# Minimal stubs injected via monkeypatch where needed
# ---------------------------------------------------------------------------

class _FakeClient(object):
    """Returns pre-configured responses in order."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.get_calls = []

    def get(self, path, **kwargs):
        self.get_calls.append(path)
        if not self._responses:
            raise AssertionError("Unexpected get() for {}".format(path))
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


# ---------------------------------------------------------------------------
# generate_run_id
# ---------------------------------------------------------------------------

class TestGenerateRunId:
    def test_format(self):
        from fts_framework.runner import generate_run_id
        run_id = generate_run_id()
        parts = run_id.split("_")
        assert len(parts) == 3
        assert len(parts[0]) == 8   # YYYYMMDD
        assert len(parts[1]) == 6   # HHMMSS
        assert len(parts[2]) == 8   # 8 hex chars

    def test_uniqueness(self):
        from fts_framework.runner import generate_run_id
        ids = {generate_run_id() for _ in range(20)}
        assert len(ids) == 20

    def test_date_part_is_digits(self):
        from fts_framework.runner import generate_run_id
        run_id = generate_run_id()
        date_part = run_id.split("_")[0]
        assert date_part.isdigit()

    def test_time_part_is_digits(self):
        from fts_framework.runner import generate_run_id
        run_id = generate_run_id()
        time_part = run_id.split("_")[1]
        assert time_part.isdigit()

    def test_uuid_part_is_hex(self):
        from fts_framework.runner import generate_run_id
        run_id = generate_run_id()
        hex_part = run_id.split("_")[2]
        int(hex_part, 16)  # raises ValueError if not hex


# ---------------------------------------------------------------------------
# _fts_monitor_base
# ---------------------------------------------------------------------------

class TestFtsMonitorBase:
    def test_standard_port_substitution(self):
        from fts_framework.runner import _fts_monitor_base
        result = _fts_monitor_base("https://fts.example.org:8446")
        assert result == "https://fts.example.org:8449/fts3/ftsmon/#/job/"

    def test_no_port_returns_empty(self):
        from fts_framework.runner import _fts_monitor_base
        result = _fts_monitor_base("https://fts.example.org")
        assert result == ""

    def test_empty_endpoint_returns_empty(self):
        from fts_framework.runner import _fts_monitor_base
        assert _fts_monitor_base("") == ""

    def test_monitor_url_has_job_fragment(self):
        from fts_framework.runner import _fts_monitor_base
        base = _fts_monitor_base("https://fts.example.org:8446")
        assert "/fts3/ftsmon/#/job/" in base

    def test_different_host(self):
        from fts_framework.runner import _fts_monitor_base
        result = _fts_monitor_base("https://transfer.cern.ch:8446")
        assert "transfer.cern.ch:8449" in result


# ---------------------------------------------------------------------------
# _merge_file_records
# ---------------------------------------------------------------------------

class TestMergeFileRecords:
    def _rec(self, src, state):
        return {"source_surl": src, "file_state": state}

    def test_new_record_replaces_old_for_same_source(self):
        from fts_framework.runner import _merge_file_records
        existing = [self._rec("https://src/a", "FAILED")]
        new = [self._rec("https://src/a", "FINISHED")]
        result = _merge_file_records(existing, new)
        assert result[0]["file_state"] == "FINISHED"

    def test_unchanged_record_preserved(self):
        from fts_framework.runner import _merge_file_records
        existing = [
            self._rec("https://src/a", "FINISHED"),
            self._rec("https://src/b", "FAILED"),
        ]
        new = [self._rec("https://src/b", "FINISHED")]
        result = _merge_file_records(existing, new)
        assert result[0]["file_state"] == "FINISHED"  # a unchanged
        assert result[1]["file_state"] == "FINISHED"  # b replaced

    def test_returns_same_length_as_existing(self):
        from fts_framework.runner import _merge_file_records
        existing = [self._rec("s1", "FAILED"), self._rec("s2", "FAILED")]
        new = [self._rec("s1", "FINISHED")]
        result = _merge_file_records(existing, new)
        assert len(result) == 2

    def test_empty_new_preserves_all(self):
        from fts_framework.runner import _merge_file_records
        existing = [self._rec("s1", "FAILED"), self._rec("s2", "FAILED")]
        result = _merge_file_records(existing, [])
        assert all(r["file_state"] == "FAILED" for r in result)

    def test_empty_existing_returns_empty(self):
        from fts_framework.runner import _merge_file_records
        result = _merge_file_records([], [self._rec("s1", "FINISHED")])
        assert result == []

    def test_multiple_new_records_applied(self):
        from fts_framework.runner import _merge_file_records
        existing = [self._rec("s1", "FAILED"), self._rec("s2", "FAILED")]
        new = [self._rec("s1", "FINISHED"), self._rec("s2", "FINISHED")]
        result = _merge_file_records(existing, new)
        assert all(r["file_state"] == "FINISHED" for r in result)

    def test_new_record_with_unknown_source_does_not_appear(self):
        """W4: new_records entry not in existing is silently dropped (intended)."""
        from fts_framework.runner import _merge_file_records
        existing = [self._rec("s1", "FAILED")]
        new = [self._rec("s1", "FINISHED"), self._rec("s-extra", "FINISHED")]
        result = _merge_file_records(existing, new)
        # Only s1 should appear; s-extra is not in existing so is dropped
        assert len(result) == 1
        assert result[0]["source_surl"] == "s1"
        assert result[0]["file_state"] == "FINISHED"


# ---------------------------------------------------------------------------
# _submit_chunks — isolated with monkeypatched internals
# ---------------------------------------------------------------------------

def _patch_submit_internals(monkeypatch, chunks=None, build_payload_fn=None,
                             write_payload_fn=None, submit_fn=None):
    """Patch all three submission internals in runner's namespace."""
    import fts_framework.runner as runner_mod

    if chunks is None:
        chunks = lambda m, size=200: [OrderedDict(list(m.items()))]  # noqa: E731
    if build_payload_fn is None:
        build_payload_fn = lambda cm, cs, cfg, run_id, ci, rr: {"files": []}  # noqa: E731
    if write_payload_fn is None:
        write_payload_fn = lambda run_id, ci, rr, payload, runs_dir="runs": "/p"  # noqa: E731
    if submit_fn is None:
        submit_fn = lambda client, payload, cfg, run_id, ci, rr: "job-001"  # noqa: E731

    monkeypatch.setattr(runner_mod, "chunk_mapping", chunks)
    monkeypatch.setattr(runner_mod, "build_payload", build_payload_fn)
    monkeypatch.setattr(runner_mod.store, "write_payload", write_payload_fn)
    monkeypatch.setattr(runner_mod, "submit_with_500_recovery", submit_fn)


class TestSubmitChunks:
    def _config(self, chunk_size=200):
        return {
            "fts": {"endpoint": "https://fts.example.org:8446"},
            "transfer": {"chunk_size": chunk_size},
        }

    def test_returns_subjob_list(self, tmp_path, monkeypatch):
        _patch_submit_internals(monkeypatch, submit_fn=lambda c, p, cfg, r, ci, rr: "job-001")
        from fts_framework.runner import _submit_chunks
        mapping = OrderedDict([("https://src/f1", "https://dst/f1")])
        subjobs = _submit_chunks(mapping, {}, self._config(), "run-1", 0, _FakeClient([]), str(tmp_path))
        assert len(subjobs) == 1
        assert subjobs[0]["job_id"] == "job-001"

    def test_subjob_fields_present(self, tmp_path, monkeypatch):
        _patch_submit_internals(monkeypatch, submit_fn=lambda c, p, cfg, r, ci, rr: "job-xyz")
        from fts_framework.runner import _submit_chunks
        mapping = OrderedDict([("https://src/a", "https://dst/a")])
        result = _submit_chunks(mapping, {}, self._config(), "run-1", 0, _FakeClient([]), str(tmp_path))
        sj = result[0]
        for key in ("job_id", "chunk_index", "run_id", "retry_round",
                    "submitted_at", "file_count", "status", "terminal",
                    "payload_path", "fts_monitor_url"):
            assert key in sj, "Missing key: {}".format(key)

    def test_status_is_submitted(self, tmp_path, monkeypatch):
        _patch_submit_internals(monkeypatch)
        from fts_framework.runner import _submit_chunks
        result = _submit_chunks(
            OrderedDict([("s", "d")]), {}, self._config(), "run-1", 0, _FakeClient([]), str(tmp_path)
        )
        assert result[0]["status"] == "SUBMITTED"
        assert result[0]["terminal"] is False

    def test_monitor_url_populated_when_endpoint_has_8446(self, tmp_path, monkeypatch):
        _patch_submit_internals(monkeypatch, submit_fn=lambda c, p, cfg, r, ci, rr: "job-abc")
        from fts_framework.runner import _submit_chunks
        cfg = {"fts": {"endpoint": "https://fts.example.org:8446"}, "transfer": {"chunk_size": 200}}
        result = _submit_chunks(
            OrderedDict([("s", "d")]), {}, cfg, "run-1", 0, _FakeClient([]), str(tmp_path)
        )
        assert "job-abc" in result[0]["fts_monitor_url"]
        assert "8449" in result[0]["fts_monitor_url"]

    def test_monitor_url_empty_when_no_8446(self, tmp_path, monkeypatch):
        _patch_submit_internals(monkeypatch)
        from fts_framework.runner import _submit_chunks
        cfg = {"fts": {"endpoint": "https://fts.example.org"}, "transfer": {"chunk_size": 200}}
        result = _submit_chunks(
            OrderedDict([("s", "d")]), {}, cfg, "run-1", 0, _FakeClient([]), str(tmp_path)
        )
        assert result[0]["fts_monitor_url"] == ""

    def test_multiple_chunks_produce_multiple_subjobs(self, tmp_path, monkeypatch):
        call_count = [0]
        def _fake_submit(client, payload, cfg, run_id, ci, rr):
            call_count[0] += 1
            return "job-{}".format(call_count[0])
        _patch_submit_internals(
            monkeypatch,
            chunks=lambda m, size=200: [OrderedDict([("s1", "d1")]), OrderedDict([("s2", "d2")])],
            submit_fn=_fake_submit,
        )
        from fts_framework.runner import _submit_chunks
        mapping = OrderedDict([("s1", "d1"), ("s2", "d2")])
        result = _submit_chunks(
            mapping, {}, self._config(chunk_size=1), "run-1", 0, _FakeClient([]), str(tmp_path)
        )
        assert len(result) == 2
        assert result[0]["chunk_index"] == 0
        assert result[1]["chunk_index"] == 1

    def test_payload_written_before_submit(self, tmp_path, monkeypatch):
        """Raw-data-first invariant: write_payload must be called before submit."""
        call_order = []
        def _fake_write(run_id, ci, rr, payload, runs_dir="runs"):
            call_order.append("write")
            return "/p"
        def _fake_submit(client, payload, cfg, run_id, ci, rr):
            call_order.append("submit")
            return "job-1"
        _patch_submit_internals(monkeypatch, write_payload_fn=_fake_write, submit_fn=_fake_submit)
        from fts_framework.runner import _submit_chunks
        _submit_chunks(OrderedDict([("s", "d")]), {}, self._config(), "r", 0, _FakeClient([]), str(tmp_path))
        assert call_order == ["write", "submit"]


# ---------------------------------------------------------------------------
# _persist_terminal_job_states
# ---------------------------------------------------------------------------

class TestPersistTerminalJobStates:
    def _subjob(self, job_id, terminal=True):
        return {"job_id": job_id, "terminal": terminal}

    def test_terminal_job_state_persisted(self, tmp_path, monkeypatch):
        written = []
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_raw",
            lambda run_id, category, filename, data, runs_dir="runs": written.append(filename),
        )
        client = _FakeClient([{"job_state": "FINISHED"}])
        from fts_framework.runner import _persist_terminal_job_states
        _persist_terminal_job_states([self._subjob("job-1")], client, "run-1", str(tmp_path))
        assert any("job-1" in f for f in written)

    def test_non_terminal_job_skipped(self, tmp_path, monkeypatch):
        written = []
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_raw",
            lambda run_id, category, filename, data, runs_dir="runs": written.append(filename),
        )
        client = _FakeClient([])
        from fts_framework.runner import _persist_terminal_job_states
        _persist_terminal_job_states([self._subjob("job-1", terminal=False)], client, "run-1", str(tmp_path))
        assert written == []
        assert len(client.get_calls) == 0

    def test_fetch_error_does_not_raise(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_raw",
            lambda run_id, category, filename, data, runs_dir="runs": None,
        )
        client = _FakeClient([RuntimeError("network error")])
        from fts_framework.runner import _persist_terminal_job_states
        # Must not raise
        _persist_terminal_job_states([self._subjob("job-1")], client, "run-1", str(tmp_path))

    def test_correct_endpoint_called(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_raw",
            lambda run_id, category, filename, data, runs_dir="runs": None,
        )
        client = _FakeClient([{"job_state": "FAILED"}])
        from fts_framework.runner import _persist_terminal_job_states
        _persist_terminal_job_states([self._subjob("job-abc-999")], client, "run-1", str(tmp_path))
        assert client.get_calls[0] == "/jobs/job-abc-999"

    def test_multiple_terminal_jobs_all_persisted(self, tmp_path, monkeypatch):
        written = []
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_raw",
            lambda run_id, category, filename, data, runs_dir="runs": written.append(filename),
        )
        client = _FakeClient([{"job_state": "FINISHED"}, {"job_state": "FAILED"}])
        from fts_framework.runner import _persist_terminal_job_states
        subjobs = [self._subjob("job-1"), self._subjob("job-2")]
        _persist_terminal_job_states(subjobs, client, "run-1", str(tmp_path))
        assert len(written) == 2

    def test_empty_job_id_skipped(self, tmp_path, monkeypatch):
        written = []
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_raw",
            lambda run_id, category, filename, data, runs_dir="runs": written.append(filename),
        )
        client = _FakeClient([])
        from fts_framework.runner import _persist_terminal_job_states
        _persist_terminal_job_states([{"job_id": "", "terminal": True}], client, "run-1", str(tmp_path))
        assert written == []


# ---------------------------------------------------------------------------
# run_campaign — full integration with all externals mocked
# ---------------------------------------------------------------------------

def _base_config():
    return {
        "fts": {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
        "tokens": {
            "fts_submit": "tok-fts",
            "source_read": "tok-src",
            "dest_write": "tok-dst",
        },
        "transfer": {
            "source_pfns_file": "/fake/pfns.txt",
            "chunk_size": 200,
        },
        "polling": {
            "initial_interval_s": 0,
            "backoff_multiplier": 1.0,
            "max_interval_s": 0,
            "campaign_timeout_s": 3600,
        },
        "retry": {"framework_retry_max": 0},
        "cleanup": {"before": False, "after": False},
        "reporting": {},
        "thresholds": {"min_success_rate": 0.0},
    }


def _install_run_campaign_mocks(monkeypatch, tmp_path,
                                 file_records=None,
                                 snapshot=None,
                                 run_exists=False):
    """Install full set of mocks needed for run_campaign()."""
    import fts_framework.runner as runner_mod

    if file_records is None:
        file_records = [{"source_surl": "s1", "file_state": "FINISHED"}]
    if snapshot is None:
        snapshot = {"threshold_passed": True, "run_id": "run-fake"}

    # fts client
    fake_session = object()
    monkeypatch.setattr(
        "fts_framework.fts.client.build_session",
        lambda token, ssl_verify: fake_session,
    )
    monkeypatch.setattr(
        "fts_framework.fts.client.FTSClient",
        lambda endpoint, session: _FakeClient([
            {"identity": "test"},    # /whoami
            {"pairs": []},           # /optimizer/current
        ]),
    )

    # resume
    monkeypatch.setattr(
        "fts_framework.resume.controller.run_exists",
        lambda run_id, runs_dir="runs": run_exists,
    )

    if run_exists:
        monkeypatch.setattr(
            "fts_framework.resume.controller.load",
            lambda run_id, client, config, runs_dir="runs": [
                {"job_id": "job-resumed", "chunk_index": 0, "retry_round": 0,
                 "terminal": False, "status": "SUBMITTED"}
            ],
        )
        monkeypatch.setattr(
            "fts_framework.persistence.store.load_manifest",
            lambda run_id, runs_dir="runs": {
                "destination_mapping": {"s1": "d1"},
            },
        )
    else:
        # Fresh run
        monkeypatch.setattr(
            "fts_framework.persistence.store.init_run_directory",
            lambda run_id, config, runs_dir="runs": None,
        )
        monkeypatch.setattr(
            "fts_framework.inventory.loader.load",
            lambda path: (["https://src/f1"], {}),
        )
        monkeypatch.setattr(
            "fts_framework.destination.planner.plan",
            lambda pfns, config: OrderedDict([("https://src/f1", "https://dst/f1")]),
        )
        monkeypatch.setattr(
            "fts_framework.checksum.fetcher.fetch_all",
            lambda pfns, session, config: {"https://src/f1": "adler32:aabbccdd"},
        )
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_manifest",
            lambda run_id, mapping, config, fts_monitor_base="", runs_dir="runs": None,
        )
        monkeypatch.setattr(
            "fts_framework.persistence.store.load_manifest",
            lambda run_id, runs_dir="runs": {"destination_mapping": {"https://src/f1": "https://dst/f1"}},
        )

    # _submit_chunks
    monkeypatch.setattr(
        runner_mod, "_submit_chunks",
        lambda mapping, checksums, config, run_id, retry_round, client, runs_dir: [
            {"job_id": "job-001", "chunk_index": 0, "retry_round": retry_round,
             "terminal": False, "status": "SUBMITTED", "file_count": 1,
             "payload_path": "/p", "fts_monitor_url": "", "run_id": run_id,
             "submitted_at": "2026-01-01T00:00:00Z"}
        ],
    )

    # store.update_manifest
    monkeypatch.setattr(
        "fts_framework.persistence.store.update_manifest",
        lambda run_id, subjobs, runs_dir="runs": None,
    )

    # poller
    monkeypatch.setattr(
        "fts_framework.fts.poller.poll_to_completion",
        lambda subjobs, client, config: [
            dict(sj, terminal=True, status="FINISHED") for sj in subjobs
        ],
    )

    # _persist_terminal_job_states
    monkeypatch.setattr(
        runner_mod, "_persist_terminal_job_states",
        lambda subjobs, client, run_id, runs_dir: None,
    )

    # collector
    monkeypatch.setattr(
        "fts_framework.fts.collector.harvest_all",
        lambda subjobs, client, run_id=None, runs_dir=None: (file_records, [], []),
    )

    # store.write_normalized
    monkeypatch.setattr(
        "fts_framework.persistence.store.write_normalized",
        lambda run_id, fr, rr, dr, runs_dir="runs": None,
    )

    # metrics
    monkeypatch.setattr(
        "fts_framework.metrics.engine.compute",
        lambda file_records, retry_records, config, run_id: snapshot,
    )

    # renderer
    monkeypatch.setattr(
        "fts_framework.reporting.renderer.render_all",
        lambda snap, config, subjobs=None, file_records=None, runs_dir="runs": None,
    )

    # store.mark_completed
    monkeypatch.setattr(
        "fts_framework.persistence.store.mark_completed",
        lambda run_id, runs_dir="runs": None,
    )


class TestRunCampaign:
    def test_returns_snapshot(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        from fts_framework.runner import run_campaign
        result = run_campaign(_base_config(), runs_dir=str(tmp_path))
        assert result["threshold_passed"] is True

    def test_uses_run_id_from_config_if_provided(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        config = _base_config()
        config["run"] = {"run_id": "custom-run-id"}
        from fts_framework.runner import run_campaign
        result = run_campaign(config, runs_dir=str(tmp_path))
        assert result is not None

    def test_generates_run_id_if_not_in_config(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        config = _base_config()
        # No "run" key at all
        from fts_framework.runner import run_campaign
        result = run_campaign(config, runs_dir=str(tmp_path))
        assert result is not None

    def test_resume_path_used_when_run_exists(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path, run_exists=True)
        from fts_framework.runner import run_campaign
        result = run_campaign(_base_config(), runs_dir=str(tmp_path))
        assert result is not None

    def test_no_framework_retry_when_all_finished(self, tmp_path, monkeypatch):
        file_records = [{"source_surl": "s1", "file_state": "FINISHED"}]
        _install_run_campaign_mocks(monkeypatch, tmp_path, file_records=file_records)
        import fts_framework.runner as runner_mod
        submit_calls = [0]
        original = runner_mod._submit_chunks
        def counting_submit(mapping, checksums, config, run_id, retry_round, client, runs_dir):
            submit_calls[0] += 1
            return original(mapping, checksums, config, run_id, retry_round, client, runs_dir)
        monkeypatch.setattr(runner_mod, "_submit_chunks", counting_submit)
        config = dict(_base_config())
        config["retry"] = {"framework_retry_max": 2}
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        # Only 1 call (initial submission); no retries because no failed files
        assert submit_calls[0] == 1

    def test_framework_retry_invoked_on_failed_files(self, tmp_path, monkeypatch):
        """With failed files and framework_retry_max=1, _submit_chunks is called twice."""
        file_records = [{"source_surl": "s1", "file_state": "FAILED"}]
        # After retry, all finished
        retry_file_records = [{"source_surl": "s1", "file_state": "FINISHED"}]

        _install_run_campaign_mocks(monkeypatch, tmp_path, file_records=file_records)

        import fts_framework.runner as runner_mod
        import fts_framework.fts.collector as collector_mod

        harvest_calls = [0]
        def _fake_harvest(subjobs, client, run_id=None, runs_dir=None):
            harvest_calls[0] += 1
            if harvest_calls[0] == 1:
                return (file_records, [], [])
            return (retry_file_records, [], [])
        monkeypatch.setattr(collector_mod, "harvest_all", _fake_harvest)

        submit_calls = [0]
        orig_submit = runner_mod._submit_chunks
        def counting_submit(mapping, checksums, config, run_id, retry_round, client, runs_dir):
            submit_calls[0] += 1
            return orig_submit(mapping, checksums, config, run_id, retry_round, client, runs_dir)
        monkeypatch.setattr(runner_mod, "_submit_chunks", counting_submit)

        config = dict(_base_config())
        config["retry"] = {"framework_retry_max": 1}
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert submit_calls[0] == 2  # initial + 1 retry

    def test_pre_cleanup_called_when_configured(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        cleanup_calls = []
        monkeypatch.setattr(
            "fts_framework.cleanup.manager.cleanup_pre",
            lambda mapping, session, config: cleanup_calls.append("pre") or [],
        )
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_cleanup_audit",
            lambda run_id, phase, audit, runs_dir="runs": None,
        )
        config = _base_config()
        config["cleanup"] = {"before": True, "after": False}
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert "pre" in cleanup_calls

    def test_post_cleanup_called_when_configured(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        cleanup_calls = []
        monkeypatch.setattr(
            "fts_framework.cleanup.manager.cleanup_post",
            lambda file_records, session, config: cleanup_calls.append("post") or [],
        )
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_cleanup_audit",
            lambda run_id, phase, audit, runs_dir="runs": None,
        )
        config = _base_config()
        config["cleanup"] = {"before": False, "after": True}
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert "post" in cleanup_calls

    def test_pre_cleanup_not_called_when_disabled(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        cleanup_calls = []
        monkeypatch.setattr(
            "fts_framework.cleanup.manager.cleanup_pre",
            lambda mapping, session, config: cleanup_calls.append("pre") or [],
        )
        monkeypatch.setattr(
            "fts_framework.persistence.store.write_cleanup_audit",
            lambda run_id, phase, audit, runs_dir="runs": None,
        )
        from fts_framework.runner import run_campaign
        run_campaign(_base_config(), runs_dir=str(tmp_path))
        assert "pre" not in cleanup_calls

    def test_whoami_failure_does_not_abort(self, tmp_path, monkeypatch):
        """GET /whoami failure must not raise — campaign continues."""
        import fts_framework.runner as runner_mod
        import fts_framework.fts.client as client_mod

        _install_run_campaign_mocks(monkeypatch, tmp_path)

        # Override client to raise on whoami
        import requests
        monkeypatch.setattr(
            client_mod, "FTSClient",
            lambda endpoint, session: _FakeClient([
                requests.ConnectionError("timeout"),  # /whoami raises
                {"pairs": []},                         # /optimizer/current
            ]),
        )
        from fts_framework.runner import run_campaign
        result = run_campaign(_base_config(), runs_dir=str(tmp_path))
        assert result is not None

    def test_mark_completed_called(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        completed = []
        monkeypatch.setattr(
            "fts_framework.persistence.store.mark_completed",
            lambda run_id, runs_dir="runs": completed.append(run_id),
        )
        from fts_framework.runner import run_campaign
        run_campaign(_base_config(), runs_dir=str(tmp_path))
        assert len(completed) == 1

    def test_token_expired_on_whoami_propagates(self, tmp_path, monkeypatch):
        """W1/B1: TokenExpiredError from /whoami must not be swallowed."""
        import fts_framework.fts.client as client_mod
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        monkeypatch.setattr(
            client_mod, "FTSClient",
            lambda endpoint, session: _FakeClient([
                TokenExpiredError("token expired"),  # /whoami
                {"pairs": []},                        # /optimizer/current
            ]),
        )
        from fts_framework.runner import run_campaign
        with pytest.raises(TokenExpiredError):
            run_campaign(_base_config(), runs_dir=str(tmp_path))

    def test_token_expired_on_optimizer_propagates(self, tmp_path, monkeypatch):
        """W1/B1: TokenExpiredError from /optimizer/current must propagate."""
        import fts_framework.fts.client as client_mod
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        monkeypatch.setattr(
            client_mod, "FTSClient",
            lambda endpoint, session: _FakeClient([
                {"identity": "ok"},          # /whoami succeeds
                TokenExpiredError("expired"), # /optimizer/current
            ]),
        )
        from fts_framework.runner import run_campaign
        with pytest.raises(TokenExpiredError):
            run_campaign(_base_config(), runs_dir=str(tmp_path))

    def test_checksum_fetch_skipped_when_verify_checksum_none(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        fetch_calls = []
        monkeypatch.setattr(
            "fts_framework.checksum.fetcher.fetch_all",
            lambda pfns, session, config: fetch_calls.append(pfns) or {},
        )
        config = _base_config()
        config["transfer"]["verify_checksum"] = "none"
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert fetch_calls == []

    def test_checksum_fetch_skipped_when_verify_checksum_target(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        fetch_calls = []
        monkeypatch.setattr(
            "fts_framework.checksum.fetcher.fetch_all",
            lambda pfns, session, config: fetch_calls.append(pfns) or {},
        )
        config = _base_config()
        config["transfer"]["verify_checksum"] = "target"
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert fetch_calls == []

    def test_supplied_checksums_skip_fetch(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        monkeypatch.setattr(
            "fts_framework.inventory.loader.load",
            lambda path: (["https://src/f1"], {"https://src/f1": "adler32:a1b2c3d4"}),
        )
        fetch_calls = []
        monkeypatch.setattr(
            "fts_framework.checksum.fetcher.fetch_all",
            lambda pfns, session, config: fetch_calls.append(pfns) or {},
        )
        config = _base_config()
        config["transfer"]["verify_checksum"] = "both"
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert fetch_calls == []

    def test_checksum_fetch_called_when_verify_checksum_both(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        fetch_calls = []
        monkeypatch.setattr(
            "fts_framework.checksum.fetcher.fetch_all",
            lambda pfns, session, config: fetch_calls.append(pfns) or {"https://src/f1": "adler32:aabbccdd"},
        )
        config = _base_config()
        config["transfer"]["verify_checksum"] = "both"
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert len(fetch_calls) == 1

    def test_max_files_truncates_pfn_list(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        planned = []
        monkeypatch.setattr(
            "fts_framework.inventory.loader.load",
            lambda path: (["https://src/f1", "https://src/f2", "https://src/f3"], {}),
        )
        monkeypatch.setattr(
            "fts_framework.destination.planner.plan",
            lambda pfns, config: planned.append(pfns) or OrderedDict(
                [("https://src/f1", "https://dst/f1")]
            ),
        )
        config = _base_config()
        config["transfer"]["max_files"] = 1
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert planned[0] == ["https://src/f1"]

    def test_max_files_none_uses_all_pfns(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        planned = []
        monkeypatch.setattr(
            "fts_framework.inventory.loader.load",
            lambda path: (["https://src/f1", "https://src/f2"], {}),
        )
        monkeypatch.setattr(
            "fts_framework.destination.planner.plan",
            lambda pfns, config: planned.append(pfns) or OrderedDict(
                [("https://src/f1", "https://dst/f1")]
            ),
        )
        config = _base_config()
        config["transfer"]["max_files"] = None
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert planned[0] == ["https://src/f1", "https://src/f2"]

    def test_max_files_larger_than_inventory_uses_all(self, tmp_path, monkeypatch):
        _install_run_campaign_mocks(monkeypatch, tmp_path)
        planned = []
        monkeypatch.setattr(
            "fts_framework.inventory.loader.load",
            lambda path: (["https://src/f1", "https://src/f2"], {}),
        )
        monkeypatch.setattr(
            "fts_framework.destination.planner.plan",
            lambda pfns, config: planned.append(pfns) or OrderedDict(
                [("https://src/f1", "https://dst/f1")]
            ),
        )
        config = _base_config()
        config["transfer"]["max_files"] = 999
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        assert planned[0] == ["https://src/f1", "https://src/f2"]

    def test_empty_file_records_with_retry_max_set(self, tmp_path, monkeypatch):
        """W2: empty file_records with framework_retry_max>0 exits loop without submitting retry."""
        _install_run_campaign_mocks(monkeypatch, tmp_path, file_records=[])
        import fts_framework.runner as runner_mod
        submit_calls = [0]
        orig = runner_mod._submit_chunks
        def counting(mapping, checksums, config, run_id, retry_round, client, runs_dir):
            submit_calls[0] += 1
            return orig(mapping, checksums, config, run_id, retry_round, client, runs_dir)
        monkeypatch.setattr(runner_mod, "_submit_chunks", counting)
        config = dict(_base_config())
        config["retry"] = {"framework_retry_max": 3}
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))
        # Only the initial submission — loop exits immediately on empty failed list
        assert submit_calls[0] == 1

    def test_retry_round_increments_across_multiple_retries(self, tmp_path, monkeypatch):
        """W3: retry_round argument to _submit_chunks must be 0, 1, 2 for initial+two retries."""
        import fts_framework.runner as runner_mod
        import fts_framework.fts.collector as collector_mod

        # Install base mocks first, then wrap _submit_chunks to track calls
        _install_run_campaign_mocks(monkeypatch, tmp_path)

        rounds_submitted = []
        mocked_submit = runner_mod._submit_chunks  # the stub set by _install

        def tracking_submit(mapping, checksums, config, run_id, retry_round, client, runs_dir):
            rounds_submitted.append(retry_round)
            return mocked_submit(mapping, checksums, config, run_id, retry_round, client, runs_dir)
        monkeypatch.setattr(runner_mod, "_submit_chunks", tracking_submit)

        # Round 0 + round 1: FAILED; round 2: FINISHED
        harvest_calls = [0]
        def fake_harvest(subjobs, client, run_id=None, runs_dir=None):
            harvest_calls[0] += 1
            if harvest_calls[0] <= 2:
                return ([{"source_surl": "s1", "file_state": "FAILED"}], [], [])
            return ([{"source_surl": "s1", "file_state": "FINISHED"}], [], [])
        monkeypatch.setattr(collector_mod, "harvest_all", fake_harvest)

        config = dict(_base_config())
        config["retry"] = {"framework_retry_max": 2}
        from fts_framework.runner import run_campaign
        run_campaign(config, runs_dir=str(tmp_path))

        assert rounds_submitted == [0, 1, 2]


# ---------------------------------------------------------------------------
# main() — CLI entry point
# ---------------------------------------------------------------------------

class TestMain:
    def _install_main_mocks(self, monkeypatch, tmp_path, threshold_passed=True):
        monkeypatch.setattr(
            "fts_framework.config.loader.load",
            lambda path, token=None, fts_submit_token=None, source_read_token=None, dest_write_token=None: _base_config(),
        )
        _install_run_campaign_mocks(
            monkeypatch, tmp_path,
            snapshot={"threshold_passed": threshold_passed, "run_id": "run-1"},
        )

    def test_exits_zero_on_success(self, tmp_path, monkeypatch):
        self._install_main_mocks(monkeypatch, tmp_path, threshold_passed=True)
        monkeypatch.setattr("sys.argv", ["fts-run", "/fake/config.yaml"])
        from fts_framework.runner import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0

    def test_exits_one_on_threshold_failure(self, tmp_path, monkeypatch):
        self._install_main_mocks(monkeypatch, tmp_path, threshold_passed=False)
        monkeypatch.setattr("sys.argv", ["fts-run", "/fake/config.yaml"])
        from fts_framework.runner import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_exits_one_on_campaign_exception(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "fts_framework.config.loader.load",
            lambda path, token=None, fts_submit_token=None, source_read_token=None, dest_write_token=None: _base_config(),
        )
        monkeypatch.setattr("sys.argv", ["fts-run", "/fake/config.yaml"])
        import fts_framework.runner as runner_mod
        monkeypatch.setattr(
            runner_mod, "run_campaign",
            lambda config, runs_dir="runs": (_ for _ in ()).throw(RuntimeError("boom")),
        )
        from fts_framework.runner import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_accepts_runs_dir_argument(self, tmp_path, monkeypatch):
        self._install_main_mocks(monkeypatch, tmp_path)
        monkeypatch.setattr(
            "sys.argv", ["fts-run", "/fake/config.yaml", "--runs-dir", str(tmp_path)],
        )
        from fts_framework.runner import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0

    def test_accepts_log_level_argument(self, tmp_path, monkeypatch):
        self._install_main_mocks(monkeypatch, tmp_path)
        monkeypatch.setattr(
            "sys.argv", ["fts-run", "/fake/config.yaml", "--log-level", "DEBUG"],
        )
        from fts_framework.runner import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
