"""
Unit tests for offline metric recompute:
``store.load_run_config`` / ``store.load_normalized``,
``runner.recompute_run``, ``fts-run --recompute`` and
``fts-sequence --recompute``.

Run directories are built with the real store writers under ``tmp_path``.
"""

import json
import os

import pytest

from fts_framework.persistence import store
from fts_framework.runner import recompute_run
from fts_framework.sequence.state import COMPLETED, FAILED


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(test_label="recompute-test", ssl_verify=True):
    return {
        "run": {"test_label": test_label},
        "fts": {"endpoint": "https://fts.example.org:8446",
                "ssl_verify": ssl_verify},
        "tokens": {"fts_submit": "secret", "source_read": "secret",
                   "dest_write": "secret"},
        "retry": {"min_success_threshold": 0.95},
        "output": {"timeseries_bucket_s": 60},
    }


def _file(file_id, filesize, start, finish, throughput):
    return {
        "file_id": file_id,
        "job_id": "job-1",
        "file_state": "FINISHED",
        "source_surl": "https://src/f{}".format(file_id),
        "dest_surl": "https://dst/f{}".format(file_id),
        "filesize": filesize,
        "throughput": throughput,
        "tx_duration": 1.0,
        "start_time": start,
        "finish_time": finish,
        "reason": "",
        "throughput_wire": 0.0,
        "throughput_wall": 0.0,
        "wall_duration_s": 0.0,
    }


def _make_run(runs_dir, run_id, ssl_verify=True, stale_snapshot=True):
    """Write a completed run as a pre-fix framework would have left it."""
    config = _config(ssl_verify=ssl_verify)
    store.init_run_directory(run_id, config, runs_dir=runs_dir)
    store.write_manifest(run_id, [], config, runs_dir=runs_dir)
    # Single 42 s file, 4200 B → aggregate 100 B/s (the peak-bug shape)
    files = [_file(1, 4200, "2026-01-01T00:00:00", "2026-01-01T00:00:42", 120.0)]
    store.write_normalized(run_id, files, [], [], runs_dir=runs_dir)
    if stale_snapshot:
        store.write_metrics(run_id, {
            "run_id": run_id,
            "peak_aggregate_throughput_bytes_per_s": 70.0,
        }, runs_dir=runs_dir)
    store.mark_completed(run_id, runs_dir=runs_dir)


def _read_snapshot(runs_dir, run_id):
    path = os.path.join(runs_dir, run_id, "metrics", "snapshot.json")
    with open(path) as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# store loaders
# ---------------------------------------------------------------------------

class TestStoreLoaders:
    def test_load_run_config_is_redacted(self, tmp_path):
        _make_run(str(tmp_path), "run-1")
        cfg = store.load_run_config("run-1", runs_dir=str(tmp_path))
        assert cfg["run"]["test_label"] == "recompute-test"
        assert cfg["tokens"]["fts_submit"] == "<REDACTED>"

    def test_load_normalized_round_trip(self, tmp_path):
        _make_run(str(tmp_path), "run-1")
        files, retries = store.load_normalized("run-1", runs_dir=str(tmp_path))
        assert len(files) == 1
        assert files[0]["filesize"] == 4200
        assert retries == []

    def test_load_normalized_missing_raises(self, tmp_path):
        with pytest.raises((IOError, OSError)):
            store.load_normalized("nope", runs_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# recompute_run
# ---------------------------------------------------------------------------

class TestRecomputeRun:
    def test_rewrites_stale_snapshot(self, tmp_path):
        runs_dir = str(tmp_path)
        _make_run(runs_dir, "run-1")
        recompute_run("run-1", runs_dir=runs_dir, console=False)
        snap = _read_snapshot(runs_dir, "run-1")
        assert snap["aggregate_throughput_bytes_per_s"] == pytest.approx(100.0)
        assert snap["peak_aggregate_throughput_bytes_per_s"] == pytest.approx(100.0)
        assert snap["throughput_p10"] == pytest.approx(120.0)

    def test_works_without_existing_snapshot(self, tmp_path):
        runs_dir = str(tmp_path)
        _make_run(runs_dir, "run-1", stale_snapshot=False)
        snap = recompute_run("run-1", runs_dir=runs_dir, console=False)
        assert snap["run_id"] == "run-1"
        assert os.path.isfile(
            os.path.join(runs_dir, "run-1", "metrics", "snapshot.json"))

    def test_ssl_verify_flag_taken_from_manifest(self, tmp_path):
        runs_dir = str(tmp_path)
        _make_run(runs_dir, "run-1", ssl_verify=False)
        snap = recompute_run("run-1", runs_dir=runs_dir, console=False)
        assert snap["ssl_verify_disabled"] is True

    def test_normalized_records_untouched(self, tmp_path):
        runs_dir = str(tmp_path)
        _make_run(runs_dir, "run-1")
        path = os.path.join(runs_dir, "run-1", "normalized", "file_records.json")
        with open(path) as fh:
            before = fh.read()
        recompute_run("run-1", runs_dir=runs_dir, console=False)
        with open(path) as fh:
            assert fh.read() == before

    def test_console_suppressed(self, tmp_path, capsys):
        _make_run(str(tmp_path), "run-1")
        recompute_run("run-1", runs_dir=str(tmp_path), console=False)
        assert "Run Summary" not in capsys.readouterr().out

    def test_console_printed_by_default(self, tmp_path, capsys):
        _make_run(str(tmp_path), "run-1")
        recompute_run("run-1", runs_dir=str(tmp_path))
        assert "Run Summary" in capsys.readouterr().out

    def test_no_token_written(self, tmp_path):
        runs_dir = str(tmp_path)
        _make_run(runs_dir, "run-1")
        recompute_run("run-1", runs_dir=runs_dir, console=False)
        for root, _, names in os.walk(os.path.join(runs_dir, "run-1")):
            for name in names:
                with open(os.path.join(root, name)) as fh:
                    assert "secret" not in fh.read()


# ---------------------------------------------------------------------------
# fts-run --recompute
# ---------------------------------------------------------------------------

class TestRunCliRecompute:
    def test_exits_zero_without_config(self, tmp_path, monkeypatch):
        _make_run(str(tmp_path), "run-1")
        monkeypatch.setattr("sys.argv", [
            "fts-run", "--recompute", "run-1", "--runs-dir", str(tmp_path)])
        from fts_framework.runner import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
        snap = _read_snapshot(str(tmp_path), "run-1")
        assert snap["peak_aggregate_throughput_bytes_per_s"] == pytest.approx(100.0)

    def test_exits_one_for_missing_run(self, tmp_path, monkeypatch):
        monkeypatch.setattr("sys.argv", [
            "fts-run", "--recompute", "nope", "--runs-dir", str(tmp_path)])
        from fts_framework.runner import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_config_required_without_recompute(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["fts-run"])
        from fts_framework.runner import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# fts-sequence --recompute
# ---------------------------------------------------------------------------

def _write_state(seq_dir, runs_dir, trials):
    """trials: list of (status, run_id) for a single case."""
    state = {
        "sequence_id": "seq-1",
        "sequence_label": None,
        "sweep_mode": "cartesian",
        "trials": len(trials),
        "runs_dir": runs_dir,
        "cases": [{
            "case_index": 0,
            "params": {"transfer.max_files": 1},
            "trials": [
                {"trial_index": i, "run_id": run_id, "status": status,
                 "error": None, "completed_at": None}
                for i, (status, run_id) in enumerate(trials)
            ],
        }],
    }
    with open(os.path.join(seq_dir, "state.json"), "w") as fh:
        json.dump(state, fh)


class TestSequenceCliRecompute:
    def _run(self, monkeypatch, seq_dir):
        monkeypatch.setattr("sys.argv", ["fts-sequence", "--recompute", seq_dir])
        from fts_framework.sequence.__main__ import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        return exc_info.value.code

    def test_recomputes_completed_and_regenerates_summary(self, tmp_path, monkeypatch):
        seq_dir = str(tmp_path)
        runs_dir = os.path.join(seq_dir, "runs")
        _make_run(runs_dir, "run-a")
        _make_run(runs_dir, "run-b")
        _write_state(seq_dir, runs_dir, [(COMPLETED, "run-a"), (COMPLETED, "run-b")])

        assert self._run(monkeypatch, seq_dir) == 0
        for run_id in ("run-a", "run-b"):
            snap = _read_snapshot(runs_dir, run_id)
            assert snap["peak_aggregate_throughput_bytes_per_s"] == pytest.approx(100.0)
        with open(os.path.join(seq_dir, "reports", "summary.md")) as fh:
            content = fh.read()
        assert "TP p10 (MB/s)" in content

    def test_skips_non_completed_trials(self, tmp_path, monkeypatch):
        seq_dir = str(tmp_path)
        runs_dir = os.path.join(seq_dir, "runs")
        _make_run(runs_dir, "run-a")
        _make_run(runs_dir, "run-f")
        _write_state(seq_dir, runs_dir, [(COMPLETED, "run-a"), (FAILED, "run-f")])

        assert self._run(monkeypatch, seq_dir) == 0
        stale = _read_snapshot(runs_dir, "run-f")
        assert stale["peak_aggregate_throughput_bytes_per_s"] == 70.0

    def test_exits_one_when_a_run_fails_but_still_reports(self, tmp_path, monkeypatch):
        seq_dir = str(tmp_path)
        runs_dir = os.path.join(seq_dir, "runs")
        _make_run(runs_dir, "run-a")
        _write_state(seq_dir, runs_dir, [(COMPLETED, "run-a"), (COMPLETED, "gone")])

        assert self._run(monkeypatch, seq_dir) == 1
        assert os.path.isfile(os.path.join(seq_dir, "reports", "summary.md"))
        snap = _read_snapshot(runs_dir, "run-a")
        assert snap["peak_aggregate_throughput_bytes_per_s"] == pytest.approx(100.0)
