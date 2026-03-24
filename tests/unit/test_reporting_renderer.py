"""
Unit tests for fts_framework.reporting.renderer
"""

import json
import sys

import pytest

from fts_framework.reporting import renderer
from fts_framework.reporting.renderer import (
    _fmt_bytes_per_sec,
    _md_escape,
    render_console,
    render_csv,
    render_html,
    render_markdown,
    render_timeseries_csv,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _base_snapshot(**overrides):
    snap = {
        "run_id": "20240101_abcd1234",
        "test_label": "test_label_value",
        "generated_at": "2024-01-01T00:00:00Z",
        "total_files": 100,
        "finished": 95,
        "failed": 3,
        "canceled": 1,
        "not_used": 1,
        "staging_unsupported": 0,
        "success_rate": 0.9574,
        "failure_rate": 0.0303,
        "threshold_passed": True,
        "throughput_mean": 50e6,
        "throughput_p50": 48e6,
        "throughput_p90": 70e6,
        "throughput_p95": 80e6,
        "throughput_p99": 90e6,
        "throughput_max": 100e6,
        "aggregate_throughput_bytes_per_s": 500e6,
        "duration_mean_s": 12.5,
        "duration_p50_s": 11.0,
        "duration_p90_s": 18.0,
        "duration_p95_s": 22.0,
        "peak_concurrency": 20,
        "mean_concurrency": 15.3,
        "total_retries": 5,
        "files_with_retries": 3,
        "retry_rate": 0.03,
        "retry_distribution": {"1": 2, "2": 1},
        "failure_reasons": {"TRANSFER_ERROR": 3},
        "ssl_verify_disabled": False,
    }
    snap.update(overrides)
    return snap


def _base_config(**overrides):
    cfg = {
        "fts": {"endpoint": "https://fts.example.org:8446"},
        "retry": {"min_success_threshold": 0.95},
        "output": {"reports": {
            "console": True,
            "json": True,
            "markdown": True,
            "html": False,
            "csv": False,
            "timeseries_csv": False,
        }},
        "run": {"test_label": "test_label_value"},
    }
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# _fmt_bytes_per_sec
# ---------------------------------------------------------------------------

class TestFmtBytesPerSec:
    def test_none_returns_na(self):
        assert _fmt_bytes_per_sec(None) == "N/A"

    def test_gb_per_sec(self):
        assert _fmt_bytes_per_sec(1.5e9) == "1.5 GB/s"

    def test_mb_per_sec(self):
        assert _fmt_bytes_per_sec(2.3e6) == "2.3 MB/s"

    def test_kb_per_sec(self):
        assert _fmt_bytes_per_sec(500e3) == "500.0 KB/s"

    def test_bytes_per_sec(self):
        assert _fmt_bytes_per_sec(999.0) == "999.0 B/s"

    def test_exactly_1_gb(self):
        assert _fmt_bytes_per_sec(1e9) == "1.0 GB/s"

    def test_exactly_1_mb(self):
        assert _fmt_bytes_per_sec(1e6) == "1.0 MB/s"

    def test_exactly_1_kb(self):
        assert _fmt_bytes_per_sec(1e3) == "1.0 KB/s"

    def test_zero(self):
        assert _fmt_bytes_per_sec(0.0) == "0.0 B/s"

    def test_int_input(self):
        result = _fmt_bytes_per_sec(1000000000)
        assert result == "1.0 GB/s"


# ---------------------------------------------------------------------------
# _md_escape
# ---------------------------------------------------------------------------

class TestMdEscape:
    def test_pipe_escaped(self):
        assert _md_escape("foo|bar") == "foo\\|bar"

    def test_no_pipe_unchanged(self):
        assert _md_escape("no special chars") == "no special chars"

    def test_multiple_pipes(self):
        assert _md_escape("a|b|c") == "a\\|b\\|c"

    def test_empty_string(self):
        assert _md_escape("") == ""

    def test_non_string_converted(self):
        assert _md_escape(42) == "42"


# ---------------------------------------------------------------------------
# render_console
# ---------------------------------------------------------------------------

class TestRenderConsole:
    def test_returns_string(self):
        out = render_console(_base_snapshot(), _base_config())
        assert isinstance(out, str)

    def test_contains_run_id(self):
        snap = _base_snapshot()
        out = render_console(snap, _base_config())
        assert snap["run_id"] in out

    def test_contains_test_label(self):
        snap = _base_snapshot()
        out = render_console(snap, _base_config())
        assert snap["test_label"] in out

    def test_contains_endpoint(self):
        out = render_console(_base_snapshot(), _base_config())
        assert "fts.example.org" in out

    def test_pass_when_threshold_passed(self):
        snap = _base_snapshot(threshold_passed=True)
        out = render_console(snap, _base_config())
        assert "PASS" in out

    def test_fail_when_threshold_not_passed(self):
        snap = _base_snapshot(threshold_passed=False)
        out = render_console(snap, _base_config())
        assert "FAIL" in out

    def test_throughput_present_when_data_available(self):
        out = render_console(_base_snapshot(), _base_config())
        assert "MB/s" in out or "GB/s" in out

    def test_no_throughput_message_when_none(self):
        snap = _base_snapshot(
            throughput_mean=None, throughput_p50=None,
            throughput_p90=None, throughput_p95=None,
        )
        out = render_console(snap, _base_config())
        assert "no throughput data" in out

    def test_duration_present_when_data_available(self):
        out = render_console(_base_snapshot(), _base_config())
        assert "Mean" in out

    def test_no_duration_message_when_none(self):
        snap = _base_snapshot(duration_mean_s=None)
        out = render_console(snap, _base_config())
        assert "no duration data" in out

    def test_ssl_enabled_shown(self):
        snap = _base_snapshot(ssl_verify_disabled=False)
        out = render_console(snap, _base_config())
        assert "ENABLED" in out

    def test_ssl_disabled_warning_shown_at_top(self):
        snap = _base_snapshot(ssl_verify_disabled=True)
        out = render_console(snap, _base_config())
        assert "WARNING" in out
        assert "DISABLED" in out
        # Warning should appear near the top (before the separator)
        warning_pos = out.index("WARNING")
        sep_pos = out.index("===")
        assert warning_pos < sep_pos

    def test_ssl_disabled_state_shown(self):
        snap = _base_snapshot(ssl_verify_disabled=True)
        out = render_console(snap, _base_config())
        assert "DISABLED" in out

    def test_aggregate_throughput_shown(self):
        snap = _base_snapshot(aggregate_throughput_bytes_per_s=1e9)
        out = render_console(snap, _base_config())
        assert "Aggregate" in out

    def test_aggregate_throughput_absent_when_none(self):
        snap = _base_snapshot(aggregate_throughput_bytes_per_s=None)
        out = render_console(snap, _base_config())
        assert "Aggregate throughput" not in out

    def test_file_counts_present(self):
        snap = _base_snapshot()
        out = render_console(snap, _base_config())
        assert "100" in out   # total_files
        assert "95" in out    # finished

    def test_separator_present(self):
        out = render_console(_base_snapshot(), _base_config())
        assert "===" in out

    def test_duration_p50_defaults_to_zero_if_none(self):
        snap = _base_snapshot(duration_p50_s=None, duration_p90_s=None)
        # Should not raise
        out = render_console(snap, _base_config())
        assert "0.0s" in out


# ---------------------------------------------------------------------------
# render_markdown
# ---------------------------------------------------------------------------

class TestRenderMarkdown:
    def test_returns_string(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert isinstance(out, str)

    def test_has_h1_title(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert out.startswith("# FTS3 Test Framework")

    def test_section_run_metadata(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert "## 1. Run Metadata" in out

    def test_section_transfer_outcomes(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert "## 2. Transfer Outcomes" in out

    def test_section_throughput(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert "## 3. Throughput Statistics" in out

    def test_section_duration(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert "## 4. Duration Statistics" in out

    def test_section_concurrency(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert "## 5. Concurrency" in out

    def test_section_retry(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert "## 6. Retry Distribution" in out

    def test_section_failure_reasons(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert "## 7. Failure Reasons" in out

    def test_section_subjob_summary(self):
        out = render_markdown(_base_snapshot(), _base_config())
        assert "## 8. Per-Subjob Summary" in out

    def test_run_id_in_metadata_table(self):
        snap = _base_snapshot()
        out = render_markdown(snap, _base_config())
        assert snap["run_id"] in out

    def test_pass_fail_in_outcomes(self):
        snap = _base_snapshot(threshold_passed=True)
        out = render_markdown(snap, _base_config())
        assert "PASS" in out

    def test_no_throughput_message_when_none(self):
        snap = _base_snapshot(
            throughput_mean=None, throughput_p50=None,
            throughput_p90=None, throughput_p95=None,
        )
        out = render_markdown(snap, _base_config())
        assert "No throughput data" in out

    def test_no_duration_message_when_none(self):
        snap = _base_snapshot(duration_mean_s=None)
        out = render_markdown(snap, _base_config())
        assert "No duration data" in out

    def test_retry_distribution_table_shown(self):
        snap = _base_snapshot(retry_distribution={"1": 5, "2": 2})
        out = render_markdown(snap, _base_config())
        assert "| 1 | 5 |" in out
        assert "| 2 | 2 |" in out

    def test_no_retries_message_when_empty(self):
        snap = _base_snapshot(retry_distribution={})
        out = render_markdown(snap, _base_config())
        assert "No retries" in out

    def test_failure_reasons_table_shown(self):
        snap = _base_snapshot(failure_reasons={"TRANSFER_ERROR": 3})
        out = render_markdown(snap, _base_config())
        assert "TRANSFER_ERROR" in out

    def test_no_failures_message_when_empty(self):
        snap = _base_snapshot(failure_reasons={})
        out = render_markdown(snap, _base_config())
        assert "No failures" in out

    def test_pipe_in_failure_reason_escaped(self):
        snap = _base_snapshot(failure_reasons={"ERR|CODE": 1})
        out = render_markdown(snap, _base_config())
        assert "ERR\\|CODE" in out

    def test_subjob_table_shown_when_provided(self):
        subjobs = [
            {"job_id": "job1", "chunk_index": 0, "retry_round": 0,
             "file_count": 50, "status": "FINISHED"},
        ]
        out = render_markdown(_base_snapshot(), _base_config(), subjobs=subjobs)
        assert "job1" in out
        assert "FINISHED" in out

    def test_subjob_not_available_when_none(self):
        out = render_markdown(_base_snapshot(), _base_config(), subjobs=None)
        assert "Subjob data not available" in out

    def test_ssl_warning_shown_when_disabled(self):
        snap = _base_snapshot(ssl_verify_disabled=True)
        out = render_markdown(snap, _base_config())
        assert "WARNING" in out
        assert "DISABLED" in out
        assert "## SSL Warning" in out
        # W1: top warning must appear before section 1
        pos_top = out.index("> **WARNING:**")
        pos_s1 = out.index("## 1. Run Metadata")
        assert pos_top < pos_s1

    def test_ssl_warning_not_shown_when_enabled(self):
        snap = _base_snapshot(ssl_verify_disabled=False)
        out = render_markdown(snap, _base_config())
        assert "## SSL Warning" not in out

    def test_ssl_enabled_shown_in_metadata(self):
        snap = _base_snapshot(ssl_verify_disabled=False)
        out = render_markdown(snap, _base_config())
        assert "ENABLED" in out

    def test_ssl_disabled_shown_in_metadata(self):
        snap = _base_snapshot(ssl_verify_disabled=True)
        out = render_markdown(snap, _base_config())
        assert "DISABLED" in out

    def test_retry_distribution_sorted_numerically(self):
        # Keys as strings "1", "10", "2" — must sort as int not lexicographic
        snap = _base_snapshot(retry_distribution={"10": 1, "2": 3, "1": 5})
        out = render_markdown(snap, _base_config())
        pos_1 = out.index("| 1 | 5 |")
        pos_2 = out.index("| 2 | 3 |")
        pos_10 = out.index("| 10 | 1 |")
        assert pos_1 < pos_2 < pos_10

    def test_aggregate_throughput_shown_when_present(self):
        snap = _base_snapshot(aggregate_throughput_bytes_per_s=2e9)
        out = render_markdown(snap, _base_config())
        assert "Aggregate" in out

    def test_aggregate_throughput_absent_when_none(self):
        snap = _base_snapshot(aggregate_throughput_bytes_per_s=None)
        out = render_markdown(snap, _base_config())
        # Aggregate row should not appear in throughput table
        assert "| Aggregate" not in out

    def test_duration_percentile_none_with_mean_present(self):
        # W2: duration percentiles None while mean is set → fallback to 0.00, no crash
        snap = _base_snapshot(duration_p50_s=None, duration_p90_s=None, duration_p95_s=None)
        out = render_markdown(snap, _base_config())
        assert "| p50 | 0.00 |" in out
        assert "| p90 | 0.00 |" in out
        assert "| p95 | 0.00 |" in out

    def test_subjob_not_available_when_empty_list(self):
        # W3: empty list is falsy — same fallback as None
        out = render_markdown(_base_snapshot(), _base_config(), subjobs=[])
        assert "Subjob data not available" in out


# ---------------------------------------------------------------------------
# render_html
# ---------------------------------------------------------------------------

class TestRenderHtml:
    def test_returns_string(self):
        out = render_html(_base_snapshot(), _base_config())
        assert isinstance(out, str)

    def test_doctype_present(self):
        out = render_html(_base_snapshot(), _base_config())
        assert "<!DOCTYPE html>" in out

    def test_h1_tag_present(self):
        out = render_html(_base_snapshot(), _base_config())
        assert "<h1>" in out

    def test_h2_tags_present(self):
        out = render_html(_base_snapshot(), _base_config())
        assert "<h2>" in out

    def test_table_rows_present(self):
        out = render_html(_base_snapshot(), _base_config())
        assert "<tr>" in out and "<td>" in out

    def test_title_contains_run_id(self):
        snap = _base_snapshot()
        out = render_html(snap, _base_config())
        assert snap["run_id"] in out

    def test_charset_utf8(self):
        out = render_html(_base_snapshot(), _base_config())
        assert "utf-8" in out

    def test_ssl_warning_blockquote_when_disabled(self):
        snap = _base_snapshot(ssl_verify_disabled=True)
        out = render_html(snap, _base_config())
        assert "<blockquote>" in out

    def test_subjobs_rendered_via_markdown_path(self):
        subjobs = [
            {"job_id": "jobABC", "chunk_index": 0, "retry_round": 0,
             "file_count": 10, "status": "FINISHED"},
        ]
        out = render_html(_base_snapshot(), _base_config(), subjobs=subjobs)
        assert "jobABC" in out

    def test_special_chars_escaped_in_title(self):
        snap = _base_snapshot(test_label="<script>alert(1)</script>")
        out = render_html(snap, _base_config())
        assert "<script>" not in out.split("<title>")[1].split("</title>")[0]

    def test_inline_style_present(self):
        out = render_html(_base_snapshot(), _base_config())
        assert "<style>" in out


# ---------------------------------------------------------------------------
# render_all
# ---------------------------------------------------------------------------

class TestRenderCsv:
    def _rec(self, **kwargs):
        base = {
            "file_id": 1, "job_id": "job-1", "file_state": "FINISHED",
            "source_surl": "https://src/f.dat", "dest_surl": "https://dst/f.dat",
            "filesize": 1000, "throughput": 500.0,
            "throughput_wire": 490.0, "throughput_wall": 480.0,
            "wall_duration_s": 2.0, "tx_duration": 2.0,
            "start_time": "2024-01-01T00:00:00Z",
            "finish_time": "2024-01-01T00:00:02Z",
            "checksum": "adler32:a1b2c3d4", "reason": "",
        }
        base.update(kwargs)
        return base

    def test_has_header_row(self):
        result = render_csv([self._rec()])
        lines = result.strip().split("\n")
        assert lines[0].startswith("file_id,")

    def test_header_contains_expected_columns(self):
        result = render_csv([self._rec()])
        header = result.split("\n")[0]
        for col in ("file_id", "job_id", "file_state", "source_surl",
                    "dest_surl", "filesize", "throughput", "wall_duration_s",
                    "start_time", "finish_time", "checksum", "reason"):
            assert col in header

    def test_one_row_per_record(self):
        records = [self._rec(file_id=i) for i in range(5)]
        result = render_csv(records)
        lines = [l for l in result.strip().split("\n") if l]
        assert len(lines) == 6  # header + 5 data rows

    def test_empty_records_returns_header_only(self):
        result = render_csv([])
        lines = [l for l in result.strip().split("\n") if l]
        assert len(lines) == 1
        assert "file_id" in lines[0]

    def test_values_present_in_row(self):
        rec = self._rec(source_surl="https://src/myfile.dat",
                        file_state="FAILED", reason="Timeout")
        result = render_csv([rec])
        assert "https://src/myfile.dat" in result
        assert "FAILED" in result
        assert "Timeout" in result

    def test_commas_in_reason_quoted(self):
        rec = self._rec(reason="Transfer failed, host unreachable")
        result = render_csv([rec])
        assert '"Transfer failed, host unreachable"' in result

    def test_missing_field_defaults_to_empty(self):
        rec = {"file_id": 99, "file_state": "FAILED"}
        result = render_csv([rec])
        lines = result.strip().split("\n")
        assert len(lines) == 2
        assert "99" in lines[1]


class TestRenderAll:
    def test_console_written_to_stdout(self, capsys):
        snap = _base_snapshot()
        cfg = _base_config()
        cfg["output"]["reports"]["console"] = True
        cfg["output"]["reports"]["json"] = False
        cfg["output"]["reports"]["markdown"] = False
        renderer.render_all(snap, cfg, runs_dir="/nonexistent_should_not_reach")
        captured = capsys.readouterr()
        assert snap["run_id"] in captured.out

    def test_json_writes_metrics_and_summary(self, tmp_path):
        import os
        snap = _base_snapshot()
        cfg = _base_config()
        cfg["output"]["reports"] = {"console": False, "json": True, "markdown": False, "html": False}
        runs_dir = str(tmp_path)
        run_id = snap["run_id"]
        # Create directory structure
        os.makedirs(os.path.join(runs_dir, run_id, "metrics"))
        os.makedirs(os.path.join(runs_dir, run_id, "reports"))
        renderer.render_all(snap, cfg, runs_dir=runs_dir)
        assert os.path.isfile(os.path.join(runs_dir, run_id, "metrics", "snapshot.json"))
        assert os.path.isfile(os.path.join(runs_dir, run_id, "reports", "summary.json"))

    def test_markdown_writes_report_md(self, tmp_path):
        import os
        snap = _base_snapshot()
        cfg = _base_config()
        cfg["output"]["reports"] = {"console": False, "json": False, "markdown": True, "html": False}
        runs_dir = str(tmp_path)
        run_id = snap["run_id"]
        os.makedirs(os.path.join(runs_dir, run_id, "reports"))
        os.makedirs(os.path.join(runs_dir, run_id, "metrics"))
        renderer.render_all(snap, cfg, runs_dir=runs_dir)
        assert os.path.isfile(os.path.join(runs_dir, run_id, "reports", "report.md"))

    def test_html_writes_report_html(self, tmp_path):
        import os
        snap = _base_snapshot()
        cfg = _base_config()
        cfg["output"]["reports"] = {"console": False, "json": False, "markdown": False, "html": True}
        runs_dir = str(tmp_path)
        run_id = snap["run_id"]
        os.makedirs(os.path.join(runs_dir, run_id, "reports"))
        os.makedirs(os.path.join(runs_dir, run_id, "metrics"))
        renderer.render_all(snap, cfg, runs_dir=runs_dir)
        assert os.path.isfile(os.path.join(runs_dir, run_id, "reports", "report.html"))

    def test_html_not_written_when_disabled(self, tmp_path):
        import os
        snap = _base_snapshot()
        cfg = _base_config()
        cfg["output"]["reports"] = {"console": False, "json": False, "markdown": False, "html": False}
        runs_dir = str(tmp_path)
        run_id = snap["run_id"]
        os.makedirs(os.path.join(runs_dir, run_id, "reports"))
        os.makedirs(os.path.join(runs_dir, run_id, "metrics"))
        renderer.render_all(snap, cfg, runs_dir=runs_dir)
        assert not os.path.isfile(os.path.join(runs_dir, run_id, "reports", "report.html"))

    def test_console_not_written_when_disabled(self, capsys):
        snap = _base_snapshot()
        cfg = _base_config()
        cfg["output"]["reports"] = {"console": False, "json": False, "markdown": False,
                                     "html": False, "csv": False, "timeseries_csv": False}
        renderer.render_all(snap, cfg, runs_dir="/nonexistent_should_not_reach")
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_summary_json_is_valid_json(self, tmp_path):
        import os
        snap = _base_snapshot()
        cfg = _base_config()
        cfg["output"]["reports"] = {"console": False, "json": True, "markdown": False, "html": False}
        runs_dir = str(tmp_path)
        run_id = snap["run_id"]
        os.makedirs(os.path.join(runs_dir, run_id, "metrics"))
        os.makedirs(os.path.join(runs_dir, run_id, "reports"))
        renderer.render_all(snap, cfg, runs_dir=runs_dir)
        with open(os.path.join(runs_dir, run_id, "reports", "summary.json")) as f:
            loaded = json.load(f)
        assert loaded["run_id"] == snap["run_id"]

    def test_csv_written_when_file_records_provided(self, tmp_path):
        import os
        snap = _base_snapshot()
        cfg = {
            "fts": {"endpoint": "https://fts.example.org:8446"},
            "retry": {"min_success_threshold": 0.95},
            "run": {"test_label": "test_label_value"},
            "output": {"reports": {"console": False, "json": False,
                                   "markdown": False, "csv": True}},
        }
        run_id = snap["run_id"]
        os.makedirs(os.path.join(str(tmp_path), run_id, "reports"))
        file_records = [{"file_id": 1, "job_id": "job-1",
                         "file_state": "FINISHED",
                         "source_surl": "https://src/f.dat",
                         "dest_surl": "https://dst/f.dat",
                         "filesize": 1000, "throughput": 500.0,
                         "throughput_wire": 490.0, "throughput_wall": 480.0,
                         "wall_duration_s": 2.0, "tx_duration": 2.0,
                         "start_time": "2024-01-01T00:00:00Z",
                         "finish_time": "2024-01-01T00:00:02Z",
                         "checksum": "adler32:a1b2c3d4", "reason": ""}]
        renderer.render_all(snap, cfg, file_records=file_records,
                            runs_dir=str(tmp_path))
        assert os.path.isfile(
            os.path.join(str(tmp_path), run_id, "reports", "files.csv")
        )

    def test_csv_not_written_when_file_records_none(self, tmp_path):
        import os
        snap = _base_snapshot()
        cfg = {
            "fts": {"endpoint": "https://fts.example.org:8446"},
            "retry": {"min_success_threshold": 0.95},
            "run": {"test_label": "test_label_value"},
            "output": {"reports": {"console": False, "json": False,
                                   "markdown": False, "csv": True}},
        }
        run_id = snap["run_id"]
        os.makedirs(os.path.join(str(tmp_path), run_id, "reports"))
        renderer.render_all(snap, cfg, file_records=None, runs_dir=str(tmp_path))
        assert not os.path.isfile(
            os.path.join(str(tmp_path), run_id, "reports", "files.csv")
        )

    def test_defaults_console_json_markdown_on(self, tmp_path, capsys):
        import os
        snap = _base_snapshot()
        # No "output" key — all defaults should fire except html
        cfg = {
            "fts": {"endpoint": "https://fts.example.org:8446"},
            "retry": {"min_success_threshold": 0.95},
            "run": {"test_label": "test_label_value"},
        }
        runs_dir = str(tmp_path)
        run_id = snap["run_id"]
        os.makedirs(os.path.join(runs_dir, run_id, "metrics"))
        os.makedirs(os.path.join(runs_dir, run_id, "reports"))
        renderer.render_all(snap, cfg, runs_dir=runs_dir)
        # Console written
        captured = capsys.readouterr()
        assert snap["run_id"] in captured.out
        # JSON written — both files (W4)
        assert os.path.isfile(os.path.join(runs_dir, run_id, "metrics", "snapshot.json"))
        assert os.path.isfile(os.path.join(runs_dir, run_id, "reports", "summary.json"))
        # Markdown written
        assert os.path.isfile(os.path.join(runs_dir, run_id, "reports", "report.md"))
        # HTML not written (default False)
        assert not os.path.isfile(os.path.join(runs_dir, run_id, "reports", "report.html"))


# ---------------------------------------------------------------------------
# render_timeseries_csv
# ---------------------------------------------------------------------------

class TestRenderTimeseriesCsv:
    def _bucket(self, start="2026-01-01T00:00:00Z", end="2026-01-01T00:01:00Z",
                active=2, throughput_bytes_s=1000000.0):
        return {
            "bucket_start": start,
            "bucket_end": end,
            "active_transfers": active,
            "aggregate_throughput_bytes_s": throughput_bytes_s,
        }

    def test_empty_timeseries_returns_header_only(self):
        result = render_timeseries_csv([])
        lines = result.strip().splitlines()
        assert len(lines) == 1
        assert "bucket_start" in lines[0]
        assert "bucket_end" in lines[0]
        assert "active_transfers" in lines[0]

    def test_single_bucket_row(self):
        b = self._bucket(throughput_bytes_s=2000000.0)
        result = render_timeseries_csv([b])
        lines = result.strip().splitlines()
        assert len(lines) == 2  # header + 1 row
        row = lines[1]
        assert "2026-01-01T00:00:00Z" in row
        assert "2026-01-01T00:01:00Z" in row
        assert "2" in row  # active_transfers

    def test_throughput_columns_present(self):
        b = self._bucket(throughput_bytes_s=1000000.0)
        result = render_timeseries_csv([b])
        header = result.splitlines()[0]
        assert "aggregate_throughput_bytes_s" in header
        assert "aggregate_throughput_mb_s" in header

    def test_mb_s_computed_correctly(self):
        # 1 MB/s = 1,000,000 B/s
        b = self._bucket(throughput_bytes_s=1000000.0)
        result = render_timeseries_csv([b])
        data_line = result.strip().splitlines()[1]
        cols = data_line.split(",")
        # Last column is MB/s
        mb_s = float(cols[-1])
        assert mb_s == pytest.approx(1.0, rel=1e-3)

    def test_zero_throughput_row(self):
        b = self._bucket(throughput_bytes_s=0.0)
        result = render_timeseries_csv([b])
        lines = result.strip().splitlines()
        assert len(lines) == 2
        data_line = lines[1]
        assert "0" in data_line

    def test_multiple_buckets(self):
        buckets = [
            self._bucket(start="2026-01-01T00:00:00Z", end="2026-01-01T00:01:00Z",
                         active=1, throughput_bytes_s=500000.0),
            self._bucket(start="2026-01-01T00:01:00Z", end="2026-01-01T00:02:00Z",
                         active=3, throughput_bytes_s=1500000.0),
        ]
        result = render_timeseries_csv(buckets)
        lines = result.strip().splitlines()
        assert len(lines) == 3  # header + 2 rows

    def test_output_is_valid_csv(self):
        import csv
        import io
        buckets = [self._bucket(), self._bucket(
            start="2026-01-01T00:01:00Z", end="2026-01-01T00:02:00Z",
            active=5, throughput_bytes_s=9999999.0
        )]
        result = render_timeseries_csv(buckets)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 2
        assert "bucket_start" in rows[0]
        assert "active_transfers" in rows[0]
