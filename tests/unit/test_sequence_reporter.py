"""Unit tests for fts_framework.sequence.reporter."""

import csv
import json
import os
import tempfile

import pytest

from fts_framework.sequence import reporter as seq_reporter
from fts_framework.sequence import state as seq_state
from fts_framework.sequence.state import COMPLETED, FAILED, PENDING


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_state(n_cases=2, trials=2, sweep_mode="cartesian", label=None):
    """Build a minimal in-memory state dict (no disk I/O)."""
    cases = []
    for i in range(n_cases):
        trial_list = []
        for j in range(trials):
            trial_list.append({
                "trial_index": j,
                "run_id":      None,
                "status":      PENDING,
                "error":       None,
                "completed_at": None,
            })
        cases.append({
            "case_index": i,
            "params":     {"transfer.max_files": (i + 1) * 100},
            "trials":     trial_list,
        })
    return {
        "sequence_id":    "test_seq_001",
        "sequence_label": label,
        "sweep_mode":     sweep_mode,
        "trials":         trials,
        "cases":          cases,
    }


def _mark_completed_inmem(state, case_index, trial_index, run_id):
    state["cases"][case_index]["trials"][trial_index]["status"] = COMPLETED
    state["cases"][case_index]["trials"][trial_index]["run_id"] = run_id


def _mark_failed_inmem(state, case_index, trial_index, error="boom"):
    state["cases"][case_index]["trials"][trial_index]["status"] = FAILED
    state["cases"][case_index]["trials"][trial_index]["error"] = error


def _write_snapshot(runs_dir, run_id, snapshot):
    """Write a snapshot.json for a fake run."""
    metrics_dir = os.path.join(runs_dir, run_id, "metrics")
    os.makedirs(metrics_dir, exist_ok=True)
    with open(os.path.join(metrics_dir, "snapshot.json"), "w") as fh:
        json.dump(snapshot, fh)


def _default_snapshot(**overrides):
    snap = {
        "run_id":           "run_000",
        "files_total":      100,
        "files_succeeded":  95,
        "files_failed":     5,
        "success_rate":     0.95,
        "throughput_mean":  50e6,   # 50 MB/s in bytes/s
        "throughput_p50":   48e6,
        "throughput_p90":   60e6,
        "throughput_stddev": 5e6,
        "campaign_wall_s":  120.0,
        "threshold_passed": True,
    }
    snap.update(overrides)
    return snap


# ---------------------------------------------------------------------------
# generate_summary — file creation
# ---------------------------------------------------------------------------

class TestGenerateSummaryFiles:
    def test_creates_summary_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            assert os.path.isfile(os.path.join(tmp, "reports", "summary.json"))

    def test_creates_summary_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            assert os.path.isfile(os.path.join(tmp, "reports", "summary.csv"))

    def test_creates_summary_md(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            assert os.path.isfile(os.path.join(tmp, "reports", "summary.md"))

    def test_creates_reports_dir_if_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            # Do NOT pre-create reports/
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            assert os.path.isdir(os.path.join(tmp, "reports"))


# ---------------------------------------------------------------------------
# JSON report
# ---------------------------------------------------------------------------

class TestJsonReport:
    def test_structure_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.json")) as fh:
                data = json.load(fh)
        assert "sequence_id" in data
        assert "runs"        in data
        assert "cases"       in data

    def test_run_count_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state(n_cases=2, trials=3)
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.json")) as fh:
                data = json.load(fh)
        assert len(data["runs"]) == 6   # 2 cases × 3 trials

    def test_snapshot_metrics_included(self):
        with tempfile.TemporaryDirectory() as tmp:
            runs_dir = os.path.join(tmp, "runs")
            os.makedirs(runs_dir)
            state = _make_state(n_cases=1, trials=1)
            _mark_completed_inmem(state, 0, 0, "run_abc")
            _write_snapshot(runs_dir, "run_abc", _default_snapshot(
                run_id="run_abc", throughput_mean=50e6,
            ))
            seq_reporter.generate_summary(tmp, state, runs_dir=runs_dir)
            with open(os.path.join(tmp, "reports", "summary.json")) as fh:
                data = json.load(fh)
        run = data["runs"][0]
        assert run["run_id"]          == "run_abc"
        assert run["status"]          == COMPLETED
        assert run["throughput_mean"] == pytest.approx(50e6)

    def test_missing_snapshot_does_not_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state(n_cases=1, trials=1)
            _mark_completed_inmem(state, 0, 0, "run_missing")
            # Do NOT write snapshot
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.json")) as fh:
                data = json.load(fh)
        assert data["runs"][0]["throughput_mean"] is None


# ---------------------------------------------------------------------------
# CSV report
# ---------------------------------------------------------------------------

class TestCsvReport:
    def test_header_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.csv"), newline="") as fh:
                reader = csv.DictReader(fh)
                headers = reader.fieldnames
        assert "case_index"   in headers
        assert "trial_index"  in headers
        assert "run_id"       in headers
        assert "status"       in headers
        assert "success_rate" in headers

    def test_param_columns_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.csv"), newline="") as fh:
                reader = csv.DictReader(fh)
                headers = reader.fieldnames
        assert "param_transfer.max_files" in headers

    def test_row_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state(n_cases=3, trials=2)
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.csv"), newline="") as fh:
                rows = list(csv.DictReader(fh))
        assert len(rows) == 6


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------

class TestMarkdownReport:
    def test_sequence_id_in_heading(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.md")) as fh:
                content = fh.read()
        assert "test_seq_001" in content

    def test_results_by_case_section_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.md")) as fh:
                content = fh.read()
        assert "Results by Case" in content

    def test_individual_runs_section_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state()
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.md")) as fh:
                content = fh.read()
        assert "Individual Runs" in content

    def test_label_shown_when_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state(label="my_label")
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.md")) as fh:
                content = fh.read()
        assert "my_label" in content


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

class TestAggregation:
    def test_mean_computed_across_trials(self):
        with tempfile.TemporaryDirectory() as tmp:
            runs_dir = os.path.join(tmp, "runs")
            os.makedirs(runs_dir)
            state = _make_state(n_cases=1, trials=2)
            _mark_completed_inmem(state, 0, 0, "run_a")
            _mark_completed_inmem(state, 0, 1, "run_b")
            _write_snapshot(runs_dir, "run_a",
                            _default_snapshot(throughput_mean=40e6))
            _write_snapshot(runs_dir, "run_b",
                            _default_snapshot(throughput_mean=60e6))
            seq_reporter.generate_summary(tmp, state, runs_dir=runs_dir)
            with open(os.path.join(tmp, "reports", "summary.json")) as fh:
                data = json.load(fh)
        agg = data["cases"][0]
        assert agg["throughput_mean_mean"] == pytest.approx(50e6)

    def test_stddev_none_for_single_trial(self):
        with tempfile.TemporaryDirectory() as tmp:
            runs_dir = os.path.join(tmp, "runs")
            os.makedirs(runs_dir)
            state = _make_state(n_cases=1, trials=1)
            _mark_completed_inmem(state, 0, 0, "run_a")
            _write_snapshot(runs_dir, "run_a", _default_snapshot())
            seq_reporter.generate_summary(tmp, state, runs_dir=runs_dir)
            with open(os.path.join(tmp, "reports", "summary.json")) as fh:
                data = json.load(fh)
        agg = data["cases"][0]
        assert agg["throughput_mean_stddev"] is None

    def test_failed_trial_excluded_from_aggregate(self):
        with tempfile.TemporaryDirectory() as tmp:
            runs_dir = os.path.join(tmp, "runs")
            os.makedirs(runs_dir)
            state = _make_state(n_cases=1, trials=2)
            _mark_completed_inmem(state, 0, 0, "run_ok")
            _mark_failed_inmem(state, 0, 1)
            _write_snapshot(runs_dir, "run_ok", _default_snapshot())
            seq_reporter.generate_summary(tmp, state, runs_dir=runs_dir)
            with open(os.path.join(tmp, "reports", "summary.json")) as fh:
                data = json.load(fh)
        agg = data["cases"][0]
        assert agg["n_completed"] == 1
        assert agg["n_failed"]    == 1

    def test_all_pending_produces_none_aggregates(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = _make_state(n_cases=1, trials=2)
            seq_reporter.generate_summary(tmp, state, runs_dir=tmp)
            with open(os.path.join(tmp, "reports", "summary.json")) as fh:
                data = json.load(fh)
        agg = data["cases"][0]
        assert agg["throughput_mean_mean"] is None
