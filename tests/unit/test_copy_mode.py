"""
Unit tests for HTTP-TPC copy-mode selection (``transfer.copy_mode``).

Covers the URL helpers in fts_framework.fts.copy_mode, config validation,
payload construction (destination query + job_metadata), stripping the
echoed query on harvest, resume reminting, cleanup URLs, the metrics echo
check, and report rendering.
"""

import logging

import pytest
import yaml

from fts_framework.cleanup.manager import cleanup_post
from fts_framework.config.loader import load
from fts_framework.exceptions import ConfigError
from fts_framework.fts import copy_mode
from fts_framework.fts.collector import _normalise_file_record
from fts_framework.fts.submission import build_payload
from fts_framework.metrics.engine import compute
from fts_framework.reporting.renderer import (
    render_console,
    render_csv,
    render_markdown,
)
from fts_framework.resume.controller import _remint_payload
from tests.unit.test_fts_collector import _fts_file
from tests.unit.test_fts_submission import _config as _submission_config
from tests.unit.test_metrics_engine import _config as _engine_config
from tests.unit.test_metrics_engine import _file as _engine_file
from tests.unit.test_reporting_renderer import _base_config as _report_config
from tests.unit.test_reporting_renderer import _base_snapshot

DST = "davs://dst.example.org:1094/data/test/testfile_000001"


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_add_without_query(self):
        assert copy_mode.add(DST, "push") == DST + "?copy_mode=push"

    def test_add_with_existing_query(self):
        assert copy_mode.add(DST + "?a=1", "pull") == DST + "?a=1&copy_mode=pull"

    def test_split_no_query(self):
        assert copy_mode.split(DST) == (DST, None)

    def test_split_only_copy_mode(self):
        assert copy_mode.split(DST + "?copy_mode=push") == (DST, "push")

    def test_split_preserves_other_params_in_order(self):
        url = DST + "?a=1&copy_mode=pull&b=2"
        assert copy_mode.split(url) == (DST + "?a=1&b=2", "pull")

    def test_split_other_params_only(self):
        assert copy_mode.split(DST + "?a=1") == (DST + "?a=1", None)

    @pytest.mark.parametrize("mode", ["pull", "push"])
    def test_round_trip(self, mode):
        assert copy_mode.split(copy_mode.add(DST, mode)) == (DST, mode)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _config_file(tmp_path, **transfer):
    data = {
        "run": {"test_label": "t"},
        "fts": {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
        "tokens": {"fts_submit": "a", "source_read": "b", "dest_write": "c"},
        "transfer": dict({
            "source_pfns_file": "sources.txt",
            "dst_prefix": "https://storage.example.org/data",
        }, **transfer),
        "output": {"base_dir": "runs"},
    }
    path = str(tmp_path / "c.yaml")
    with open(path, "w") as fh:
        yaml.dump(data, fh)
    return path


class TestConfig:
    def test_default_is_none(self, tmp_path):
        assert load(_config_file(tmp_path))["transfer"]["copy_mode"] is None

    @pytest.mark.parametrize("mode", ["pull", "push"])
    def test_valid_modes(self, tmp_path, mode):
        assert load(_config_file(tmp_path, copy_mode=mode))["transfer"]["copy_mode"] == mode

    @pytest.mark.parametrize("bad", ["streamed", "PUSH", "puhs", 1])
    def test_invalid_mode_rejected(self, tmp_path, bad):
        with pytest.raises(ConfigError, match="transfer.copy_mode"):
            load(_config_file(tmp_path, copy_mode=bad))


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------

def _payload(mode, dst=DST):
    config = _submission_config()
    config["transfer"]["copy_mode"] = mode
    return build_payload([("https://src/f1", dst)], {}, config, "run-1", 0, 0)


class TestPayload:
    @pytest.mark.parametrize("mode", ["pull", "push"])
    def test_destination_carries_query(self, mode):
        entry = _payload(mode)["files"][0]
        assert entry["destinations"] == [DST + "?copy_mode=" + mode]
        assert entry["sources"] == ["https://src/f1"]

    def test_job_metadata_records_mode(self):
        assert _payload("push")["params"]["job_metadata"]["copy_mode"] == "push"

    def test_none_leaves_payload_unchanged(self):
        payload = _payload(None)
        assert payload["files"][0]["destinations"] == [DST]
        assert "copy_mode" not in payload["params"]["job_metadata"]

    def test_user_metadata_cannot_override_mode(self):
        config = _submission_config(job_metadata={"copy_mode": "pull"})
        config["transfer"]["copy_mode"] = "push"
        payload = build_payload([("https://src/f1", DST)], {}, config, "run-1", 0, 0)
        assert payload["params"]["job_metadata"]["copy_mode"] == "push"

    def test_invalid_swept_value_rejected(self):
        # Sequence overrides bypass the loader; build_payload re-checks.
        with pytest.raises(ConfigError, match="transfer.copy_mode"):
            _payload("puhs")


# ---------------------------------------------------------------------------
# Harvest, cleanup, resume
# ---------------------------------------------------------------------------

class TestHarvest:
    def test_echoed_query_stripped_and_recorded(self):
        rec = _normalise_file_record(
            _fts_file(dest_surl=DST + "?copy_mode=push"), "job-1", 0, 0,
        )
        assert rec["dest_surl"] == DST
        assert rec["copy_mode"] == "push"

    def test_no_query_records_none(self):
        rec = _normalise_file_record(_fts_file(dest_surl=DST), "job-1", 0, 0)
        assert rec["dest_surl"] == DST
        assert rec["copy_mode"] is None

    def test_cleanup_deletes_clean_url(self):
        rec = _normalise_file_record(
            _fts_file(dest_surl=DST + "?copy_mode=pull"), "job-1", 0, 0,
        )
        deleted = []

        class _Session(object):
            def delete(self, url, **kwargs):
                deleted.append(url)

                class _R(object):
                    status_code = 204
                return _R()

        cleanup_post([rec], _Session(), {"cleanup": {"after": True}})
        assert deleted == [DST.replace("davs://", "https://", 1)]


class TestResumeRemint:
    def test_remint_keeps_copy_mode(self):
        payload = {"files": [{"sources": ["s"], "destinations": [DST + "?copy_mode=push"]}]}
        new_dst = _remint_payload(payload)["files"][0]["destinations"][0]
        clean, mode = copy_mode.split(new_dst)
        assert mode == "push"
        assert clean != DST
        assert clean.startswith("davs://dst.example.org:1094/data/test/testfile_")
        assert "?" not in clean

    def test_remint_without_copy_mode_unchanged_behaviour(self):
        payload = {"files": [{"sources": ["s"], "destinations": [DST]}]}
        new_dst = _remint_payload(payload)["files"][0]["destinations"][0]
        assert "?" not in new_dst and new_dst != DST


# ---------------------------------------------------------------------------
# Metrics echo check
# ---------------------------------------------------------------------------

def _records(*modes):
    recs = []
    for i, mode in enumerate(modes):
        f = _engine_file(file_id=i + 1)
        f["copy_mode"] = mode
        recs.append(f)
    return recs


def _snapshot(requested, *modes):
    config = _engine_config()
    config["transfer"] = {"copy_mode": requested}
    return compute(_records(*modes), [], config, "run-1")


class TestMetrics:
    def test_all_echoed(self):
        snap = _snapshot("push", "push", "push")
        assert snap["copy_mode_requested"] == "push"
        assert snap["copy_mode_echoed_files"] == 2
        assert snap["copy_mode_mismatched_files"] == 0

    def test_missing_echo_is_mismatch(self):
        snap = _snapshot("push", "push", None)
        assert snap["copy_mode_echoed_files"] == 1
        assert snap["copy_mode_mismatched_files"] == 1

    def test_not_requested(self):
        snap = _snapshot(None, None, None)
        assert snap["copy_mode_requested"] is None
        assert snap["copy_mode_echoed_files"] == 0
        assert snap["copy_mode_mismatched_files"] == 0

    def test_unrequested_mode_present_is_mismatch(self):
        assert _snapshot(None, "pull")["copy_mode_mismatched_files"] == 1

    def test_records_without_field_tolerated(self):
        config = _engine_config()
        snap = compute([_engine_file()], [], config, "run-1")
        assert snap["copy_mode_requested"] is None
        assert snap["copy_mode_mismatched_files"] == 0


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

class TestReports:
    def test_console_requested_with_echo_count(self):
        snap = _base_snapshot(copy_mode_requested="push", copy_mode_echoed_files=100,
                              copy_mode_mismatched_files=0)
        out = render_console(snap, _report_config())
        assert "Copy mode: push (requested; FTS3 echoed on 100/100 files)" in out
        assert "WARNING" not in out

    def test_console_default(self):
        out = render_console(_base_snapshot(), _report_config())
        assert "Copy mode: server default" in out

    def test_console_mismatch_warning(self):
        snap = _base_snapshot(copy_mode_requested="pull", copy_mode_echoed_files=97,
                              copy_mode_mismatched_files=3)
        assert "3 file(s) have a dest_surl copy_mode" in render_console(snap, _report_config())

    def test_markdown_row_and_note(self):
        snap = _base_snapshot(copy_mode_requested="pull", copy_mode_echoed_files=100,
                              copy_mode_mismatched_files=0)
        md = render_markdown(snap, _report_config())
        assert "| Copy mode | pull (requested; FTS3 echoed on 100/100 files) |" in md
        assert "does not report the mode actually used" in md

    def test_markdown_no_note_when_not_requested(self):
        md = render_markdown(_base_snapshot(), _report_config())
        assert "| Copy mode | server default" in md
        assert "does not report the mode actually used" not in md

    def test_csv_has_copy_mode_column(self):
        rec = _normalise_file_record(
            _fts_file(dest_surl=DST + "?copy_mode=push"), "job-1", 0, 0,
        )
        lines = render_csv([rec]).splitlines()
        header = lines[0].split(",")
        row = lines[1].split(",")
        assert row[header.index("copy_mode")] == "push"
        assert row[header.index("dest_surl")] == DST


# ---------------------------------------------------------------------------
# Campaign start log line
# ---------------------------------------------------------------------------

class TestCampaignLog:
    @pytest.mark.parametrize("mode,expected", [
        ("push", "HTTP-TPC copy_mode: push"),
        ("pull", "HTTP-TPC copy_mode: pull"),
        (None, "HTTP-TPC copy_mode: server default (pull, fallback to push)"),
    ])
    def test_mode_logged_at_campaign_start(self, monkeypatch, tmp_path, caplog,
                                           mode, expected):
        from fts_framework.runner import run_campaign
        from tests.unit import test_runner
        test_runner._install_run_campaign_mocks(monkeypatch, tmp_path)
        config = test_runner._base_config()
        config["transfer"]["copy_mode"] = mode
        with caplog.at_level(logging.INFO, logger="fts_framework.runner"):
            run_campaign(config, runs_dir=str(tmp_path))
        assert expected in caplog.text
