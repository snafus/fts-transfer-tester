"""
fts_framework.sequence.reporter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Summary report generation for sequence runs.

Reads ``metrics/snapshot.json`` from each completed trial's run directory
and generates aggregate reports across cases and trials:

- ``reports/summary.json`` — full data, machine-readable
- ``reports/summary.csv``  — one row per trial (params + key metrics)
- ``reports/summary.md``   — human-readable table with per-case aggregates

Throughput values from snapshot.json are in bytes/s; they are formatted as
MB/s (SI, 1e6) in all reports.
"""

import csv
import json
import os
import statistics
import sys

from fts_framework.sequence.state import COMPLETED, FAILED


# Snapshot keys to include in summary reports, with display labels.
_SUMMARY_METRICS = [
    ("files_total",                       "Files Total"),
    ("files_succeeded",                   "Files Succeeded"),
    ("files_failed",                      "Files Failed"),
    ("success_rate",                      "Success Rate"),
    ("peak_concurrency",                  "Peak Concurrency"),
    ("aggregate_throughput_bytes_per_s",  "Agg TP (MB/s)"),
    ("throughput_mean",                   "TP Mean (MB/s)"),
    ("throughput_p50",                    "TP p50 (MB/s)"),
    ("throughput_p90",                    "TP p90 (MB/s)"),
    ("throughput_stddev",                 "TP StdDev (MB/s)"),
    ("campaign_wall_s",                   "Wall Time (s)"),
    ("threshold_passed",                  "Threshold Passed"),
]

# Keys whose values are in bytes/s and must be formatted as MB/s.
_THROUGHPUT_KEYS = frozenset([
    "aggregate_throughput_bytes_per_s",
    "throughput_mean",
    "throughput_p50",
    "throughput_p90",
    "throughput_p95",
    "throughput_stddev",
])


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_val(key, val):
    # type: (str, object) -> str
    """Format a snapshot value for display."""
    if val is None:
        return "-"
    if key in _THROUGHPUT_KEYS:
        return "{:.2f}".format(val / 1.0e6)
    if key == "success_rate":
        return "{:.1%}".format(val)
    if key == "campaign_wall_s":
        return "{:.1f}".format(val)
    return str(val)


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def _load_snapshot(run_id, runs_dir):
    # type: (str, str) -> object
    """Load ``metrics/snapshot.json`` for *run_id*.  Returns None if absent."""
    path = os.path.join(runs_dir, run_id, "metrics", "snapshot.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r") as fh:
        return json.load(fh)


def _collect_rows(state, runs_dir):
    # type: (dict, str) -> list
    """Build one row dict per trial."""
    rows = []
    for case in state["cases"]:
        for trial in case["trials"]:
            run_id = trial.get("run_id") or ""
            snapshot = None
            if run_id and trial["status"] == COMPLETED:
                snapshot = _load_snapshot(run_id, runs_dir)

            row = {
                "sequence_id":  state["sequence_id"],
                "case_index":   case["case_index"],
                "trial_index":  trial["trial_index"],
                "run_id":       run_id,
                "status":       trial["status"],
                "error":        trial.get("error") or "",
            }
            for k, v in case["params"].items():
                row["param_" + k] = v
            for key, _ in _SUMMARY_METRICS:
                row[key] = snapshot.get(key) if snapshot else None
            rows.append(row)
    return rows


def _aggregate_cases(state, rows):
    # type: (dict, list) -> list
    """Aggregate rows per case; compute mean ± stdev of each metric."""
    aggregates = []
    for case in state["cases"]:
        ci = case["case_index"]
        case_rows = [r for r in rows if r["case_index"] == ci]
        completed = [
            r for r in case_rows
            if r["status"] == COMPLETED and r.get("files_total") is not None
        ]
        failed_count = sum(1 for r in case_rows if r["status"] == FAILED)

        agg = {
            "case_index":  ci,
            "params":      case["params"],
            "n_total":     len(case_rows),
            "n_completed": len(completed),
            "n_failed":    failed_count,
        }
        for key, _ in _SUMMARY_METRICS:
            vals = [r[key] for r in completed if r.get(key) is not None]
            if not vals:
                agg[key + "_mean"]   = None
                agg[key + "_stddev"] = None
            else:
                try:
                    agg[key + "_mean"] = statistics.mean(vals)
                    agg[key + "_stddev"] = (
                        statistics.stdev(vals) if len(vals) >= 2 else None
                    )
                except Exception:
                    agg[key + "_mean"]   = None
                    agg[key + "_stddev"] = None
        aggregates.append(agg)
    return aggregates


# ---------------------------------------------------------------------------
# Report writers
# ---------------------------------------------------------------------------

def _write_json(sequence_dir, rows, aggregates, state):
    # type: (str, list, list, dict) -> None
    data = {
        "sequence_id":    state["sequence_id"],
        "sequence_label": state.get("sequence_label"),
        "sweep_mode":     state.get("sweep_mode"),
        "trials":         state["trials"],
        "runs":           rows,
        "cases":          aggregates,
    }
    path = os.path.join(sequence_dir, "reports", "summary.json")
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2)


def _write_csv(sequence_dir, rows):
    # type: (str, list) -> None
    if not rows:
        return
    param_cols  = sorted(k for k in rows[0] if k.startswith("param_"))
    metric_cols = [key for key, _ in _SUMMARY_METRICS]
    fixed_cols  = ["sequence_id", "case_index", "trial_index",
                   "run_id", "status", "error"]
    all_cols    = fixed_cols + param_cols + metric_cols

    path = os.path.join(sequence_dir, "reports", "summary.csv")
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=all_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(sequence_dir, rows, aggregates, state):
    # type: (str, list, list, dict) -> None
    lines = []

    seq_id   = state["sequence_id"]
    label    = state.get("sequence_label") or ""
    n_cases  = len(state["cases"])
    n_trials = state["trials"]
    total    = n_cases * n_trials
    done     = sum(1 for r in rows if r["status"] == COMPLETED)

    lines.append("# Sequence Summary: {}".format(seq_id))
    if label:
        lines.append("")
        lines.append("**Label:** {}".format(label))
    lines.append("")
    lines.append("| | |")
    lines.append("|---|---|")
    lines.append("| Cases | {} |".format(n_cases))
    lines.append("| Trials per case | {} |".format(n_trials))
    lines.append("| Total runs | {} |".format(total))
    lines.append("| Completed | {} |".format(done))
    lines.append("| Sweep mode | {} |".format(state.get("sweep_mode", "cartesian")))
    lines.append("")

    # Per-case aggregate table
    if aggregates:
        param_keys = list(aggregates[0]["params"].keys())

        lines.append("## Results by Case")
        lines.append("")

        headers = (
            ["Case"]
            + param_keys
            + ["Trials", "Success Rate", "Agg TP (MB/s)",
               "TP Mean (MB/s)", "TP p50 (MB/s)", "TP p90 (MB/s)",
               "Wall (s)"]
        )
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("|" + "---|" * len(headers))

        for agg in aggregates:
            def _mv(key, _agg=agg):
                # type: (str, dict) -> str
                v  = _agg.get(key + "_mean")
                sd = _agg.get(key + "_stddev")
                if v is None:
                    return "-"
                s = _fmt_val(key, v)
                if sd is not None:
                    s += " ± " + _fmt_val(key, sd)
                return s

            param_vals  = [str(agg["params"].get(k, "-")) for k in param_keys]
            trials_str  = "{}/{}".format(agg["n_completed"], agg["n_total"])
            row_vals = (
                [str(agg["case_index"])]
                + param_vals
                + [
                    trials_str,
                    _mv("success_rate"),
                    _mv("aggregate_throughput_bytes_per_s"),
                    _mv("throughput_mean"),
                    _mv("throughput_p50"),
                    _mv("throughput_p90"),
                    _mv("campaign_wall_s"),
                ]
            )
            lines.append("| " + " | ".join(row_vals) + " |")

        lines.append("")

    # Individual runs table
    lines.append("## Individual Runs")
    lines.append("")
    lines.append("| Case | Trial | Run ID | Status | Files | Max Conc | "
                 "Success Rate | Agg TP (MB/s) | TP Mean (MB/s) | Wall (s) |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        lines.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            r["case_index"],
            r["trial_index"],
            r["run_id"] or "-",
            r["status"],
            _fmt_val("files_total",                      r.get("files_total")),
            _fmt_val("peak_concurrency",                 r.get("peak_concurrency")),
            _fmt_val("success_rate",                     r.get("success_rate")),
            _fmt_val("aggregate_throughput_bytes_per_s", r.get("aggregate_throughput_bytes_per_s")),
            _fmt_val("throughput_mean",                  r.get("throughput_mean")),
            _fmt_val("campaign_wall_s",                  r.get("campaign_wall_s")),
        ))
    lines.append("")

    path = os.path.join(sequence_dir, "reports", "summary.md")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------

def _mv_str(agg, key):
    # type: (dict, str) -> str
    """Format mean ± stdev for an aggregate key, or '-' if absent."""
    v  = agg.get(key + "_mean")
    sd = agg.get(key + "_stddev")
    if v is None:
        return "-"
    s = _fmt_val(key, v)
    if sd is not None:
        s += u" \u00b1 " + _fmt_val(key, sd)
    return s


def print_console_summary(sequence_dir, state, rows, aggregates):
    # type: (str, dict, list, list) -> None
    """Print a human-readable sequence summary table to stdout."""
    n_cases   = len(state["cases"])
    n_trials  = state["trials"]
    total     = n_cases * n_trials
    completed = sum(1 for r in rows if r["status"] == COMPLETED)
    failed    = sum(1 for r in rows if r["status"] == FAILED)

    out = sys.stdout

    out.write("\n")
    out.write("=" * 72 + "\n")
    out.write("Sequence Summary\n")
    out.write("=" * 72 + "\n")
    seq_id = state["sequence_id"]
    label  = state.get("sequence_label") or ""
    out.write("ID:        {}{}\n".format(
        seq_id, "  ({})".format(label) if label else "",
    ))
    out.write("Cases:     {}   Trials/case: {}   Total: {}   "
              "Completed: {}   Failed: {}\n".format(
                  n_cases, n_trials, total, completed, failed,
              ))
    out.write("\n")

    if not aggregates:
        out.write("(no data)\n")
        out.write("\n")
        return

    param_keys = list(aggregates[0]["params"].keys()) if aggregates else []

    # Column definitions: (header, value_fn)
    columns = []
    columns.append(("Case", lambda agg: str(agg["case_index"])))
    for pk in param_keys:
        columns.append((pk, lambda agg, _pk=pk: str(agg["params"].get(_pk, "-"))))
    columns.append(("Trials",
                    lambda agg: "{}/{}".format(agg["n_completed"], agg["n_total"])))
    columns.append(("Succ Rate",
                    lambda agg: _mv_str(agg, "success_rate")))
    columns.append(("Agg TP MB/s",
                    lambda agg: _mv_str(agg, "aggregate_throughput_bytes_per_s")))
    columns.append(("Mean MB/s",
                    lambda agg: _mv_str(agg, "throughput_mean")))
    columns.append(("p50 MB/s",
                    lambda agg: _mv_str(agg, "throughput_p50")))
    columns.append(("p90 MB/s",
                    lambda agg: _mv_str(agg, "throughput_p90")))
    columns.append(("Wall (s)",
                    lambda agg: _mv_str(agg, "campaign_wall_s")))

    # Compute column widths
    widths = [len(hdr) for hdr, _ in columns]
    cell_rows = []
    for agg in aggregates:
        cells = [fn(agg) for _, fn in columns]
        cell_rows.append(cells)
        for i, cell in enumerate(cells):
            widths[i] = max(widths[i], len(cell))

    sep   = "  "
    hdr   = sep.join(h.ljust(widths[i]) for i, (h, _) in enumerate(columns))
    ruler = sep.join("-" * widths[i] for i in range(len(columns)))

    out.write(hdr + "\n")
    out.write(ruler + "\n")
    for cells in cell_rows:
        out.write(sep.join(c.ljust(widths[i]) for i, c in enumerate(cells)) + "\n")

    out.write("\n")
    out.write("Reports: {}\n".format(os.path.join(sequence_dir, "reports")))
    out.write("\n")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_summary(sequence_dir, state, runs_dir="runs"):
    # type: (str, dict, str) -> None
    """Generate ``summary.json``, ``summary.csv``, and ``summary.md``.

    Safe to call on a partial sequence (some trials still pending/failed).

    Args:
        sequence_dir (str): Sequence output directory.
        state (dict): Current state dict (from ``sequence.state.load()``).
        runs_dir (str): Base directory for individual run outputs.
    """
    reports_dir = os.path.join(sequence_dir, "reports")
    if not os.path.isdir(reports_dir):
        os.makedirs(reports_dir)

    rows       = _collect_rows(state, runs_dir)
    aggregates = _aggregate_cases(state, rows)

    _write_json(sequence_dir, rows, aggregates, state)
    _write_csv(sequence_dir, rows)
    _write_markdown(sequence_dir, rows, aggregates, state)
    print_console_summary(sequence_dir, state, rows, aggregates)
