"""
fts_framework.reporting.renderer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Render campaign summary reports from a ``MetricsSnapshot`` dict.

Four output formats
--------------------
- **Console**: human-readable table printed to ``sys.stdout``.
- **JSON**: ``reports/summary.json`` — full ``MetricsSnapshot`` for CI use.
- **Markdown**: ``reports/report.md`` — structured sections.
- **HTML**: ``reports/report.html`` — minimal inline-CSS HTML (optional).

All file writes are delegated to ``persistence.store.write_report`` and
``persistence.store.write_metrics``.  This module only generates content.

SSL warning
-----------
When ``snapshot["ssl_verify_disabled"]`` is ``True``, a prominent warning is
prepended to the console output and appended to every report.

Usage::

    from fts_framework.reporting.renderer import render_all
    render_all(snapshot, config, subjobs=subjobs)
"""

import calendar
import csv
import io
import json
import logging
import sys
from datetime import datetime

from fts_framework.persistence import store

logger = logging.getLogger(__name__)

# Separator character for console output
_SEP = "=" * 51
_SSL_WARNING = (
    "WARNING: SSL certificate verification was DISABLED for this run.\n"
    "Results should not be used as security-validated production benchmarks."
)


def render_all(snapshot, config, subjobs=None, file_records=None,
               runs_dir=store._DEFAULT_RUNS_DIR):
    # type: (dict, dict, object, object, str) -> None
    """Render all enabled report formats and write them to disk.

    Args:
        snapshot (dict): ``MetricsSnapshot`` dict from ``metrics.engine.compute``.
        config (dict): Validated framework config dict.
        subjobs (list[dict] or None): ``SubjobRecord`` dicts for the
            per-subjob table in the Markdown report.  Optional.
        file_records (list[dict] or None): Normalised ``FileRecord`` dicts
            (with computed metrics) for the per-file CSV report.  Optional.
        runs_dir (str): Base directory for run outputs.
    """
    run_id = snapshot["run_id"]
    reports_cfg = config.get("output", {}).get("reports", {})

    if reports_cfg.get("console", True):
        text = render_console(snapshot, config)
        sys.stdout.write(text + "\n")
        sys.stdout.flush()

    if reports_cfg.get("json", True):
        store.write_metrics(run_id, snapshot, runs_dir=runs_dir)
        # Also write summary.json as an alias for CI consumers
        store.write_report(
            run_id, "summary.json",
            json.dumps(snapshot, indent=2, default=str),
            runs_dir=runs_dir,
        )

    if reports_cfg.get("markdown", True):
        md = render_markdown(snapshot, config, subjobs=subjobs)
        store.write_report(run_id, "report.md", md, runs_dir=runs_dir)

    if reports_cfg.get("html", False):
        html = render_html(snapshot, config, subjobs=subjobs)
        store.write_report(run_id, "report.html", html, runs_dir=runs_dir)

    if reports_cfg.get("csv", True) and file_records is not None:
        csv_content = render_csv(file_records)
        store.write_report(run_id, "files.csv", csv_content, runs_dir=runs_dir)

    if reports_cfg.get("timeseries_csv", True):
        timeseries = snapshot.get("timeseries") or []
        ts_content = render_timeseries_csv(timeseries)
        store.write_report(run_id, "timeseries.csv", ts_content, runs_dir=runs_dir)

    logger.info("Reports written for run %s", run_id)


# ---------------------------------------------------------------------------
# Console
# ---------------------------------------------------------------------------

def render_console(snapshot, config):
    # type: (dict, dict) -> str
    """Return the console summary as a single string.

    Args:
        snapshot (dict): ``MetricsSnapshot`` dict.
        config (dict): Validated framework config dict.

    Returns:
        str: Formatted text suitable for printing to stdout.
    """
    lines = []
    _add = lines.append

    if snapshot.get("ssl_verify_disabled"):
        _add("")
        _add("  *** " + _SSL_WARNING.replace("\n", "\n  *** ") + " ***")
        _add("")

    _add(_SEP)
    _add(" FTS3 Test Framework — Run Summary")
    _add(" Run ID   : {}".format(snapshot.get("run_id", "")))
    _add(" Label    : {}".format(snapshot.get("test_label", "")))
    _add(" Endpoint : {}".format(
        config.get("fts", {}).get("endpoint", "<unknown>")
    ))
    _add(_SEP)

    _add(" Files total       : {}".format(snapshot.get("total_files", 0)))
    _add(" Finished          : {}".format(snapshot.get("finished", 0)))
    _add(" Failed            : {}".format(snapshot.get("failed", 0)))
    _add(" Canceled          : {}".format(snapshot.get("canceled", 0)))
    _add(" Not used          : {}".format(snapshot.get("not_used", 0)))
    _add(" Staging           : {}".format(snapshot.get("staging_unsupported", 0)))

    sr = snapshot.get("success_rate", 0.0)
    threshold = config.get("retry", {}).get("min_success_threshold", 0.95)
    pass_fail = "PASS" if snapshot.get("threshold_passed") else "FAIL"
    _add(" Success rate      : {:.2%}  [{} >= {:.2%}]".format(
        sr, pass_fail, threshold
    ))
    _add("")

    _add(" Throughput (agent-reported)")
    tp_mean = snapshot.get("throughput_mean")
    tp_p50 = snapshot.get("throughput_p50")
    tp_p90 = snapshot.get("throughput_p90")
    tp_p95 = snapshot.get("throughput_p95")
    if tp_mean is not None:
        _add("   Mean    : {}".format(_fmt_bytes_per_sec(tp_mean)))
        _add("   p50     : {}".format(_fmt_bytes_per_sec(tp_p50)))
        _add("   p90     : {}".format(_fmt_bytes_per_sec(tp_p90)))
        _add("   p95     : {}".format(_fmt_bytes_per_sec(tp_p95)))
    else:
        _add("   (no throughput data)")
    _add("")

    _add(" Duration (wall, seconds)")
    d_mean = snapshot.get("duration_mean_s")
    d_p50 = snapshot.get("duration_p50_s")
    d_p90 = snapshot.get("duration_p90_s")
    if d_mean is not None:
        _add("   Mean    : {:.1f}s    p50: {:.1f}s    p90: {:.1f}s".format(
            d_mean,
            d_p50 if d_p50 is not None else 0.0,
            d_p90 if d_p90 is not None else 0.0,
        ))
    else:
        _add("   (no duration data)")
    _add("")

    agg = snapshot.get("aggregate_throughput_bytes_per_s")
    _add(" Peak concurrency  : {} files".format(snapshot.get("peak_concurrency", 0)))
    _add(" Total retries     : {} ({} files)".format(
        snapshot.get("total_retries", 0),
        snapshot.get("files_with_retries", 0),
    ))
    if agg is not None:
        _add(" Aggregate throughput: {}".format(_fmt_bytes_per_sec(agg)))

    ssl_state = "DISABLED" if snapshot.get("ssl_verify_disabled") else "ENABLED"
    _add("")
    _add(" SSL verify        : {}".format(ssl_state))
    _add(_SEP)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Markdown
# ---------------------------------------------------------------------------

def render_markdown(snapshot, config, subjobs=None):
    # type: (dict, dict, object) -> str
    """Return the full Markdown report as a string.

    Args:
        snapshot (dict): ``MetricsSnapshot`` dict.
        config (dict): Validated framework config dict.
        subjobs (list[dict] or None): Optional subjob records for the
            per-subjob table.

    Returns:
        str: Markdown-formatted report.
    """
    lines = []
    _add = lines.append

    _add("# FTS3 Test Framework — Run Report")
    _add("")

    if snapshot.get("ssl_verify_disabled"):
        _add("> **WARNING:** SSL certificate verification was DISABLED for this run.")
        _add("> Results should not be used as security-validated production benchmarks.")
        _add("")

    # 1. Run metadata
    _add("## 1. Run Metadata")
    _add("")
    _add("| Field | Value |")
    _add("|-------|-------|")
    _add("| Run ID | `{}` |".format(snapshot.get("run_id", "")))
    _add("| Test label | {} |".format(snapshot.get("test_label", "")))
    _add("| Generated at | {} |".format(snapshot.get("generated_at", "")))
    _add("| FTS endpoint | {} |".format(
        config.get("fts", {}).get("endpoint", "<unknown>")
    ))
    _add("| SSL verify | {} |".format(
        "DISABLED" if snapshot.get("ssl_verify_disabled") else "ENABLED"
    ))
    _add("")

    # 2. Transfer outcomes
    sr = snapshot.get("success_rate", 0.0)
    threshold = config.get("retry", {}).get("min_success_threshold", 0.95)
    pass_fail = "PASS" if snapshot.get("threshold_passed") else "FAIL"
    _add("## 2. Transfer Outcomes")
    _add("")
    _add("| Metric | Value |")
    _add("|--------|-------|")
    _add("| Total files | {} |".format(snapshot.get("total_files", 0)))
    _add("| Finished | {} |".format(snapshot.get("finished", 0)))
    _add("| Failed | {} |".format(snapshot.get("failed", 0)))
    _add("| Canceled | {} |".format(snapshot.get("canceled", 0)))
    _add("| Not used | {} |".format(snapshot.get("not_used", 0)))
    _add("| Staging unsupported | {} |".format(snapshot.get("staging_unsupported", 0)))
    _add("| Success rate | {:.2%} ({} >= {:.2%}) |".format(sr, pass_fail, threshold))
    _add("| Failure rate | {:.2%} |".format(snapshot.get("failure_rate", 0.0)))
    _add("")

    # 3. Throughput statistics
    _add("## 3. Throughput Statistics")
    _add("")
    tp_mean = snapshot.get("throughput_mean")
    if tp_mean is not None:
        _add("| Statistic | Value |")
        _add("|-----------|-------|")
        _add("| Mean | {} |".format(_fmt_bytes_per_sec(tp_mean)))
        _add("| p50 | {} |".format(_fmt_bytes_per_sec(snapshot.get("throughput_p50"))))
        _add("| p90 | {} |".format(_fmt_bytes_per_sec(snapshot.get("throughput_p90"))))
        _add("| p95 | {} |".format(_fmt_bytes_per_sec(snapshot.get("throughput_p95"))))
        _add("| p99 | {} |".format(_fmt_bytes_per_sec(snapshot.get("throughput_p99"))))
        _add("| Max | {} |".format(_fmt_bytes_per_sec(snapshot.get("throughput_max"))))
        agg = snapshot.get("aggregate_throughput_bytes_per_s")
        if agg is not None:
            _add("| Aggregate | {} |".format(_fmt_bytes_per_sec(agg)))
    else:
        _add("*No throughput data available.*")
    _add("")

    # 4. Duration statistics
    _add("## 4. Duration Statistics")
    _add("")
    d_mean = snapshot.get("duration_mean_s")
    if d_mean is not None:
        _add("| Statistic | Value (s) |")
        _add("|-----------|-----------|")
        _add("| Mean | {:.2f} |".format(d_mean))
        _add("| p50 | {:.2f} |".format(snapshot.get("duration_p50_s") if snapshot.get("duration_p50_s") is not None else 0.0))
        _add("| p90 | {:.2f} |".format(snapshot.get("duration_p90_s") if snapshot.get("duration_p90_s") is not None else 0.0))
        _add("| p95 | {:.2f} |".format(snapshot.get("duration_p95_s") if snapshot.get("duration_p95_s") is not None else 0.0))
    else:
        _add("*No duration data available.*")
    _add("")

    # 5. Concurrency
    _add("## 5. Concurrency")
    _add("")
    _add("| Metric | Value |")
    _add("|--------|-------|")
    _add("| Peak concurrency | {} |".format(snapshot.get("peak_concurrency", 0)))
    _add("| Mean concurrency | {:.1f} |".format(snapshot.get("mean_concurrency", 0.0)))
    _add("")

    # 6. Retry distribution
    _add("## 6. Retry Distribution")
    _add("")
    retry_dist = snapshot.get("retry_distribution") or {}
    if retry_dist:
        _add("| Retries | Files |")
        _add("|---------|-------|")
        for k in sorted(retry_dist.keys(), key=lambda x: int(x)):
            _add("| {} | {} |".format(k, retry_dist[k]))
    else:
        _add("*No retries recorded.*")
    _add("")
    _add("Total retries: {}  Files with retries: {}  Retry rate: {:.2%}".format(
        snapshot.get("total_retries", 0),
        snapshot.get("files_with_retries", 0),
        snapshot.get("retry_rate", 0.0),
    ))
    _add("")

    # 7. Failure reasons
    _add("## 7. Failure Reasons")
    _add("")
    failure_reasons = snapshot.get("failure_reasons") or {}
    if failure_reasons:
        _add("| Reason | Count |")
        _add("|--------|-------|")
        for reason, count in sorted(
            failure_reasons.items(), key=lambda kv: kv[1], reverse=True
        ):
            _add("| {} | {} |".format(_md_escape(reason), count))
    else:
        _add("*No failures.*")
    _add("")

    # 8. Per-subjob summary
    _add("## 8. Per-Subjob Summary")
    _add("")
    if subjobs:
        _add("| Job ID | Chunk | Retry | Files | Status |")
        _add("|--------|-------|-------|-------|--------|")
        for s in subjobs:
            _add("| `{}` | {} | {} | {} | {} |".format(
                s.get("job_id", ""),
                s.get("chunk_index", ""),
                s.get("retry_round", ""),
                s.get("file_count", ""),
                s.get("status", ""),
            ))
    else:
        _add("*Subjob data not available.*")
    _add("")

    # 9. SSL warning (repeated at end)
    if snapshot.get("ssl_verify_disabled"):
        _add("## SSL Warning")
        _add("")
        _add("> **WARNING:** SSL certificate verification was DISABLED for this run.")
        _add("> Results should not be used as security-validated production benchmarks.")
        _add("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML (minimal, stdlib only)
# ---------------------------------------------------------------------------

def render_html(snapshot, config, subjobs=None):
    # type: (dict, dict, object) -> str
    """Return a minimal HTML report wrapping the Markdown content.

    Uses stdlib ``html.escape`` for escaping.  No external dependencies.

    Args:
        snapshot (dict): ``MetricsSnapshot`` dict.
        config (dict): Validated framework config dict.
        subjobs (list[dict] or None): Optional subjob records.

    Returns:
        str: HTML string.
    """
    import html as html_mod

    md_content = render_markdown(snapshot, config, subjobs=subjobs)

    # Convert Markdown headings and tables to minimal HTML
    html_lines = []
    for line in md_content.split("\n"):
        if line.startswith("## "):
            html_lines.append("<h2>{}</h2>".format(
                html_mod.escape(line[3:])
            ))
        elif line.startswith("# "):
            html_lines.append("<h1>{}</h1>".format(
                html_mod.escape(line[2:])
            ))
        elif line.startswith("> "):
            html_lines.append("<blockquote>{}</blockquote>".format(
                html_mod.escape(line[2:])
            ))
        elif line.startswith("| "):
            html_lines.append("<tr>" + "".join(
                "<td>{}</td>".format(html_mod.escape(cell.strip()))
                for cell in line.strip("|").split("|")
            ) + "</tr>")
        elif line.startswith("|---"):
            pass  # skip separator rows
        elif line.strip() == "":
            html_lines.append("<br>")
        else:
            html_lines.append("<p>{}</p>".format(html_mod.escape(line)))

    body = "\n".join(html_lines)
    title = html_mod.escape("FTS3 Run {} — {}".format(
        snapshot.get("run_id", ""), snapshot.get("test_label", "")
    ))

    return (
        "<!DOCTYPE html>\n"
        "<html><head><meta charset=\"utf-8\">\n"
        "<title>{title}</title>\n"
        "<style>body{{font-family:monospace;max-width:900px;margin:2em auto;}}"
        "table{{border-collapse:collapse;width:100%;}}"
        "td,th{{border:1px solid #ccc;padding:4px 8px;}}"
        "blockquote{{background:#fff3cd;border-left:4px solid #ffc107;"
        "padding:8px 12px;}}</style>\n"
        "</head><body>\n{body}\n</body></html>"
    ).format(title=title, body=body)


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

_ISO_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
]


def _iso_to_epoch(ts):
    # type: (object) -> object
    """Convert an ISO 8601 timestamp string to a Unix epoch integer.

    Returns an integer (seconds since 1970-01-01 UTC) or empty string
    if the input is absent or cannot be parsed.
    """
    if not ts:
        return ""
    for fmt in _ISO_FORMATS:
        try:
            dt = datetime.strptime(str(ts), fmt)
            return calendar.timegm(dt.timetuple())
        except ValueError:
            continue
    return ""


# ---------------------------------------------------------------------------
# Per-file CSV
# ---------------------------------------------------------------------------

# Ordered column definitions: (csv_header, file_record_key)
_CSV_COLUMNS = [
    ("file_id",          "file_id"),
    ("job_id",           "job_id"),
    ("file_state",       "file_state"),
    ("source_surl",      "source_surl"),
    ("dest_surl",        "dest_surl"),
    ("filesize",         "filesize"),
    ("throughput",       "throughput"),
    ("throughput_wire",  "throughput_wire"),
    ("throughput_wall",  "throughput_wall"),
    ("wall_duration_s",  "wall_duration_s"),
    ("tx_duration",      "tx_duration"),
    ("start_time",       "start_time"),
    ("start_time_ts",    None),   # derived: Unix epoch of start_time
    ("finish_time",      "finish_time"),
    ("finish_time_ts",   None),   # derived: Unix epoch of finish_time
    ("checksum",         "checksum"),
    ("reason",           "reason"),
]


def render_csv(file_records):
    # type: (list) -> str
    """Return a CSV string with one row per file record.

    Columns: file_id, job_id, file_state, source_surl, dest_surl, filesize,
    throughput, throughput_wire, throughput_wall, wall_duration_s,
    tx_duration, start_time, start_time_ts, finish_time, finish_time_ts,
    checksum, reason.

    Args:
        file_records (list[dict]): Normalised ``FileRecord`` dicts, updated
            in-place by ``metrics.engine`` with computed throughput and
            duration fields.

    Returns:
        str: CSV content including header row.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow([col for col, _ in _CSV_COLUMNS])
    for f in file_records:
        row = []
        for col, key in _CSV_COLUMNS:
            if col == "start_time_ts":
                row.append(_iso_to_epoch(f.get("start_time")))
            elif col == "finish_time_ts":
                row.append(_iso_to_epoch(f.get("finish_time")))
            else:
                row.append(f.get(key, ""))
        writer.writerow(row)
    return buf.getvalue()


_TIMESERIES_COLUMNS = [
    "bucket_start",
    "bucket_start_ts",
    "bucket_end",
    "bucket_end_ts",
    "active_transfers",
    "aggregate_throughput_bytes_s",
    "aggregate_throughput_mb_s",
]


def render_timeseries_csv(timeseries):
    # type: (list) -> str
    """Return a CSV string with one row per timeseries bucket.

    Columns: ``bucket_start``, ``bucket_start_ts``, ``bucket_end``,
    ``bucket_end_ts``, ``active_transfers``,
    ``aggregate_throughput_bytes_s``, ``aggregate_throughput_mb_s``.

    Args:
        timeseries (list[dict]): Bucket dicts from
            ``metrics.engine._compute_timeseries``.

    Returns:
        str: CSV content including header row.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(_TIMESERIES_COLUMNS)
    for b in timeseries:
        tp_bytes = b.get("aggregate_throughput_bytes_s", 0.0)
        writer.writerow([
            b.get("bucket_start", ""),
            _iso_to_epoch(b.get("bucket_start")),
            b.get("bucket_end", ""),
            _iso_to_epoch(b.get("bucket_end")),
            b.get("active_transfers", 0),
            tp_bytes,
            round(tp_bytes / 1e6, 4) if tp_bytes else 0.0,
        ])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_bytes_per_sec(val):
    # type: (object) -> str
    """Format a bytes/second value with appropriate SI prefix."""
    if val is None:
        return "N/A"
    val = float(val)
    if val >= 1e9:
        return "{:.1f} GB/s".format(val / 1e9)
    if val >= 1e6:
        return "{:.1f} MB/s".format(val / 1e6)
    if val >= 1e3:
        return "{:.1f} KB/s".format(val / 1e3)
    return "{:.1f} B/s".format(val)


def _md_escape(text):
    # type: (str) -> str
    """Minimal Markdown escaping for cell content."""
    return str(text).replace("|", "\\|")
