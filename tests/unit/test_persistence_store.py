"""
Unit tests for fts_framework.persistence.store.

Uses a temporary directory (``tmp_path`` pytest fixture) for all file I/O.
No real FTS3 interaction; no monkeypatching of os calls.
"""

import json
import os

import pytest
import yaml

from fts_framework.persistence.store import (
    init_run_directory,
    write_manifest,
    update_manifest,
    mark_completed,
    load_manifest,
    write_raw,
    write_payload,
    write_normalized,
    write_metrics,
    write_cleanup_audit,
    redact_config,
    _redact_payload,
)
from fts_framework.exceptions import ResumeError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(test_label="test-run", endpoint="https://fts.example.org:8446",
            ssl_verify=True):
    return {
        "run": {"test_label": test_label},
        "fts": {"endpoint": endpoint, "ssl_verify": ssl_verify},
        "tokens": {
            "fts_submit": "tok_submit_secret",
            "source_read": "tok_src_secret",
            "dest_write": "tok_dst_secret",
        },
        "retry": {"min_success_threshold": 0.95},
    }


def _mapping():
    return {
        "https://src/file1": "https://dst/run/file1",
        "https://src/file2": "https://dst/run/file2",
    }


def _subjob(job_id="job-1", chunk_index=0, retry_round=0,
            status="FINISHED", terminal=True):
    return {
        "job_id": job_id,
        "chunk_index": chunk_index,
        "retry_round": retry_round,
        "status": status,
        "terminal": terminal,
        "submitted_at": "2026-01-01T00:00:00Z",
        "file_count": 2,
        "payload_path": "submitted_payloads/chunk_0000_r0.json",
    }


# ---------------------------------------------------------------------------
# redact_config
# ---------------------------------------------------------------------------

class TestRedactConfig:
    def test_tokens_replaced_with_redacted(self):
        cfg = _config()
        redacted = redact_config(cfg)
        for key in ("fts_submit", "source_read", "dest_write"):
            assert redacted["tokens"][key] == "<REDACTED>"

    def test_original_config_not_mutated(self):
        cfg = _config()
        _ = redact_config(cfg)
        assert cfg["tokens"]["fts_submit"] == "tok_submit_secret"

    def test_non_token_fields_preserved(self):
        cfg = _config(test_label="my-run")
        redacted = redact_config(cfg)
        assert redacted["run"]["test_label"] == "my-run"
        assert redacted["fts"]["endpoint"] == "https://fts.example.org:8446"

    def test_missing_tokens_section_no_error(self):
        cfg = {"run": {"test_label": "t"}}
        redacted = redact_config(cfg)
        assert "tokens" not in redacted

    def test_empty_tokens_section_no_error(self):
        cfg = {"run": {"test_label": "t"}, "tokens": {}}
        redacted = redact_config(cfg)
        assert redacted["tokens"] == {}

    def test_returns_new_dict_not_same_object(self):
        cfg = _config()
        redacted = redact_config(cfg)
        assert redacted is not cfg
        assert redacted["tokens"] is not cfg["tokens"]


# ---------------------------------------------------------------------------
# init_run_directory
# ---------------------------------------------------------------------------

class TestInitRunDirectory:
    def test_creates_run_directory(self, tmp_path):
        run_dir = init_run_directory("run-001", _config(),
                                     runs_dir=str(tmp_path))
        assert os.path.isdir(run_dir)

    def test_creates_required_subdirs(self, tmp_path):
        init_run_directory("run-001", _config(), runs_dir=str(tmp_path))
        base = os.path.join(str(tmp_path), "run-001")
        for sub in ["submitted_payloads", "normalized", "metrics", "reports",
                    os.path.join("raw", "jobs"), os.path.join("raw", "files"),
                    os.path.join("raw", "retries"), os.path.join("raw", "dm")]:
            assert os.path.isdir(os.path.join(base, sub)), \
                "Missing subdir: {}".format(sub)

    def test_writes_config_yaml(self, tmp_path):
        init_run_directory("run-001", _config(), runs_dir=str(tmp_path))
        yaml_path = os.path.join(str(tmp_path), "run-001", "config.yaml")
        assert os.path.isfile(yaml_path)

    def test_config_yaml_tokens_redacted(self, tmp_path):
        init_run_directory("run-001", _config(), runs_dir=str(tmp_path))
        yaml_path = os.path.join(str(tmp_path), "run-001", "config.yaml")
        with open(yaml_path) as fh:
            data = yaml.safe_load(fh)
        for key in ("fts_submit", "source_read", "dest_write"):
            assert data["tokens"][key] == "<REDACTED>"

    def test_config_yaml_contains_no_real_token(self, tmp_path):
        init_run_directory("run-001", _config(), runs_dir=str(tmp_path))
        yaml_path = os.path.join(str(tmp_path), "run-001", "config.yaml")
        with open(yaml_path) as fh:
            raw_text = fh.read()
        assert "tok_submit_secret" not in raw_text
        assert "tok_src_secret" not in raw_text
        assert "tok_dst_secret" not in raw_text

    def test_idempotent_second_call(self, tmp_path):
        # Must not raise if directory already exists
        init_run_directory("run-001", _config(), runs_dir=str(tmp_path))
        init_run_directory("run-001", _config(), runs_dir=str(tmp_path))

    def test_returns_run_dir_path(self, tmp_path):
        path = init_run_directory("run-001", _config(), runs_dir=str(tmp_path))
        assert path == os.path.join(str(tmp_path), "run-001")


# ---------------------------------------------------------------------------
# write_manifest / load_manifest
# ---------------------------------------------------------------------------

class TestWriteManifest:
    def test_creates_manifest_json(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "manifest.json")
        assert os.path.isfile(path)

    def test_manifest_has_required_keys(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        for key in ["run_id", "test_label", "created_at", "config_hash",
                    "fts_endpoint", "destination_mapping", "subjobs", "completed"]:
            assert key in m, "Missing key: {}".format(key)

    def test_manifest_run_id_correct(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["run_id"] == "r1"

    def test_manifest_test_label_from_config(self, tmp_path):
        init_run_directory("r1", _config(test_label="bench"), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(test_label="bench"),
                       runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["test_label"] == "bench"

    def test_manifest_destination_mapping_stored(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["destination_mapping"] == _mapping()

    def test_manifest_subjobs_initially_empty(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["subjobs"] == []

    def test_manifest_completed_initially_false(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["completed"] is False

    def test_manifest_fts_endpoint_stored(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["fts_endpoint"] == "https://fts.example.org:8446"

    def test_manifest_ssl_verify_disabled_true_when_false(self, tmp_path):
        init_run_directory("r1", _config(ssl_verify=False), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(ssl_verify=False),
                       runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["ssl_verify_disabled"] is True

    def test_manifest_ssl_verify_disabled_false_when_true(self, tmp_path):
        init_run_directory("r1", _config(ssl_verify=True), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(ssl_verify=True),
                       runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["ssl_verify_disabled"] is False

    def test_manifest_config_hash_is_sha256_string(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["config_hash"].startswith("sha256:")
        assert len(m["config_hash"]) == len("sha256:") + 64

    def test_manifest_is_valid_json(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "manifest.json")
        with open(path) as fh:
            data = json.load(fh)
        assert isinstance(data, dict)

    def test_manifest_contains_no_token_values(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "manifest.json")
        with open(path) as fh:
            raw = fh.read()
        assert "tok_submit_secret" not in raw
        assert "tok_src_secret" not in raw
        assert "tok_dst_secret" not in raw


# ---------------------------------------------------------------------------
# load_manifest error cases
# ---------------------------------------------------------------------------

class TestLoadManifest:
    def test_missing_manifest_raises_resume_error(self, tmp_path):
        with pytest.raises(ResumeError):
            load_manifest("nonexistent-run", runs_dir=str(tmp_path))

    def test_corrupt_manifest_raises_resume_error(self, tmp_path):
        run_dir = os.path.join(str(tmp_path), "run-bad")
        os.makedirs(run_dir)
        path = os.path.join(run_dir, "manifest.json")
        with open(path, "w") as fh:
            fh.write("not json {{{{")
        with pytest.raises(ResumeError):
            load_manifest("run-bad", runs_dir=str(tmp_path))

    def test_empty_manifest_file_raises_resume_error(self, tmp_path):
        run_dir = os.path.join(str(tmp_path), "run-empty")
        os.makedirs(run_dir)
        path = os.path.join(run_dir, "manifest.json")
        open(path, "w").close()  # create empty file
        with pytest.raises(ResumeError):
            load_manifest("run-empty", runs_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# update_manifest
# ---------------------------------------------------------------------------

class TestUpdateManifest:
    def _setup(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))

    def test_adds_subjob_to_empty_list(self, tmp_path):
        self._setup(tmp_path)
        update_manifest("r1", [_subjob()], runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert len(m["subjobs"]) == 1
        assert m["subjobs"][0]["job_id"] == "job-1"

    def test_multiple_subjobs_added(self, tmp_path):
        self._setup(tmp_path)
        update_manifest("r1",
                        [_subjob("job-1"), _subjob("job-2")],
                        runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert len(m["subjobs"]) == 2

    def test_existing_subjob_replaced_by_job_id(self, tmp_path):
        self._setup(tmp_path)
        update_manifest("r1", [_subjob("job-1", status="ACTIVE", terminal=False)],
                        runs_dir=str(tmp_path))
        # Now update with terminal version
        update_manifest("r1", [_subjob("job-1", status="FINISHED", terminal=True)],
                        runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert len(m["subjobs"]) == 1
        assert m["subjobs"][0]["status"] == "FINISHED"

    def test_completed_flag_preserved_after_update(self, tmp_path):
        self._setup(tmp_path)
        update_manifest("r1", [_subjob()], runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["completed"] is False

    def test_no_job_id_subjob_preserved_not_discarded(self, tmp_path):
        self._setup(tmp_path)
        pending = {"job_id": None, "chunk_index": 0, "status": "PENDING"}
        update_manifest("r1", [pending], runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        # Pending subjob with no job_id must be preserved
        assert len(m["subjobs"]) == 1
        assert m["subjobs"][0]["status"] == "PENDING"

    def test_no_job_id_subjob_not_lost_on_second_update(self, tmp_path):
        self._setup(tmp_path)
        pending = {"job_id": None, "chunk_index": 0, "status": "PENDING"}
        update_manifest("r1", [pending], runs_dir=str(tmp_path))
        # Second update adds a real subjob; pending must still be present
        update_manifest("r1", [_subjob("job-1")], runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        statuses = [s["status"] for s in m["subjobs"]]
        assert "PENDING" in statuses
        assert "FINISHED" in statuses


# ---------------------------------------------------------------------------
# mark_completed
# ---------------------------------------------------------------------------

class TestMarkCompleted:
    def test_sets_completed_true(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        mark_completed("r1", runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["completed"] is True

    def test_other_manifest_fields_preserved(self, tmp_path):
        init_run_directory("r1", _config(test_label="bench"), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(test_label="bench"),
                       runs_dir=str(tmp_path))
        mark_completed("r1", runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert m["test_label"] == "bench"


# ---------------------------------------------------------------------------
# write_raw
# ---------------------------------------------------------------------------

class TestWriteRaw:
    def test_file_created_in_correct_location(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_raw("r1", "files", "job-abc.json", [{"a": 1}],
                  runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "raw", "files", "job-abc.json")
        assert os.path.isfile(path)

    def test_data_round_trips_correctly(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        data = [{"file_id": 1, "state": "FINISHED"}, {"file_id": 2}]
        write_raw("r1", "files", "job-1.json", data, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "raw", "files", "job-1.json")
        with open(path) as fh:
            result = json.load(fh)
        assert result == data

    def test_dm_category_works(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_raw("r1", "dm", "job-1.json", [], runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "raw", "dm", "job-1.json")
        assert os.path.isfile(path)

    def test_retries_category_works(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_raw("r1", "retries", "job-1_42.json", [], runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "raw", "retries", "job-1_42.json")
        assert os.path.isfile(path)

    def test_jobs_poll_category_works(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_raw("r1", "jobs", "job-1_poll_0.json", {"job_state": "FINISHED"},
                  runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "raw", "jobs", "job-1_poll_0.json")
        assert os.path.isfile(path)


# ---------------------------------------------------------------------------
# write_payload
# ---------------------------------------------------------------------------

class TestWritePayload:
    def test_file_created_in_submitted_payloads(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_payload("r1", 0, 0, {"files": []}, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "submitted_payloads",
                            "chunk_0000_r0.json")
        assert os.path.isfile(path)

    def test_filename_format_chunk_and_round(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_payload("r1", 3, 1, {}, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "submitted_payloads",
                            "chunk_0003_r1.json")
        assert os.path.isfile(path)

    def test_returns_relative_path(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        rel = write_payload("r1", 0, 0, {}, runs_dir=str(tmp_path))
        assert rel == os.path.join("submitted_payloads", "chunk_0000_r0.json")

    def test_payload_content_round_trips(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        payload = {"files": [{"source": "https://src/f"}], "priority": 3}
        write_payload("r1", 0, 0, payload, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "submitted_payloads",
                            "chunk_0000_r0.json")
        with open(path) as fh:
            result = json.load(fh)
        assert result == payload

    def test_payload_contains_no_tokens(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        payload = {"priority": 3}
        write_payload("r1", 0, 0, payload, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "submitted_payloads",
                            "chunk_0000_r0.json")
        with open(path) as fh:
            raw = fh.read()
        # Payloads never contain tokens by design; verify the write itself
        assert "tok_submit_secret" not in raw

    def test_storage_tokens_redacted_in_persisted_payload(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        payload = {
            "files": [],
            "params": {
                "priority": 3,
                "source_token": "real_source_token_value",
                "destination_token": "real_dest_token_value",
            },
        }
        write_payload("r1", 0, 0, payload, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "submitted_payloads",
                            "chunk_0000_r0.json")
        with open(path) as fh:
            on_disk = json.load(fh)
        assert on_disk["params"]["source_token"] == "<REDACTED>"
        assert on_disk["params"]["destination_token"] == "<REDACTED>"

    def test_storage_token_redaction_does_not_mutate_caller_payload(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        payload = {
            "params": {
                "source_token": "real_source_token_value",
                "destination_token": "real_dest_token_value",
            }
        }
        write_payload("r1", 0, 0, payload, runs_dir=str(tmp_path))
        assert payload["params"]["source_token"] == "real_source_token_value"
        assert payload["params"]["destination_token"] == "real_dest_token_value"

    def test_non_token_params_preserved_after_redaction(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        payload = {
            "params": {
                "priority": 4,
                "verify_checksum": "both",
                "source_token": "tok",
            }
        }
        write_payload("r1", 0, 0, payload, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "submitted_payloads",
                            "chunk_0000_r0.json")
        with open(path) as fh:
            on_disk = json.load(fh)
        assert on_disk["params"]["priority"] == 4
        assert on_disk["params"]["verify_checksum"] == "both"


# ---------------------------------------------------------------------------
# _redact_payload
# ---------------------------------------------------------------------------

class TestRedactPayload:
    def test_source_token_replaced(self):
        payload = {"params": {"source_token": "secret", "priority": 3}}
        result = _redact_payload(payload)
        assert result["params"]["source_token"] == "<REDACTED>"

    def test_destination_token_replaced(self):
        payload = {"params": {"destination_token": "secret"}}
        result = _redact_payload(payload)
        assert result["params"]["destination_token"] == "<REDACTED>"

    def test_both_tokens_replaced(self):
        payload = {"params": {"source_token": "s", "destination_token": "d"}}
        result = _redact_payload(payload)
        assert result["params"]["source_token"] == "<REDACTED>"
        assert result["params"]["destination_token"] == "<REDACTED>"

    def test_no_tokens_in_params_unchanged(self):
        payload = {"params": {"priority": 3, "verify_checksum": "both"}}
        result = _redact_payload(payload)
        assert result["params"] == {"priority": 3, "verify_checksum": "both"}

    def test_no_params_key_unchanged(self):
        payload = {"files": []}
        result = _redact_payload(payload)
        assert result == {"files": []}

    def test_original_not_mutated(self):
        payload = {"params": {"source_token": "secret"}}
        _ = _redact_payload(payload)
        assert payload["params"]["source_token"] == "secret"

    def test_returns_new_dict(self):
        payload = {"params": {"source_token": "s"}}
        result = _redact_payload(payload)
        assert result is not payload
        assert result["params"] is not payload["params"]


# ---------------------------------------------------------------------------
# write_normalized
# ---------------------------------------------------------------------------

class TestWriteNormalized:
    def test_three_files_created(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_normalized("r1", [{"file_id": 1}], [{"file_id": 1}],
                         [{"op": "delete"}], runs_dir=str(tmp_path))
        norm_dir = os.path.join(str(tmp_path), "r1", "normalized")
        for f in ["file_records.json", "retry_records.json", "dm_records.json"]:
            assert os.path.isfile(os.path.join(norm_dir, f))

    def test_file_records_round_trip(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        records = [{"file_id": 1, "file_state": "FINISHED"}]
        write_normalized("r1", records, [], [], runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "normalized", "file_records.json")
        with open(path) as fh:
            result = json.load(fh)
        assert result == records

    def test_empty_lists_written_as_empty_arrays(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_normalized("r1", [], [], [], runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "normalized", "file_records.json")
        with open(path) as fh:
            result = json.load(fh)
        assert result == []


# ---------------------------------------------------------------------------
# write_metrics
# ---------------------------------------------------------------------------

class TestWriteMetrics:
    def test_snapshot_file_created(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_metrics("r1", {"run_id": "r1", "success_rate": 1.0},
                      runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "metrics", "snapshot.json")
        assert os.path.isfile(path)

    def test_snapshot_round_trips(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        snapshot = {"run_id": "r1", "success_rate": 0.98, "total_files": 100}
        write_metrics("r1", snapshot, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "metrics", "snapshot.json")
        with open(path) as fh:
            result = json.load(fh)
        assert result["success_rate"] == 0.98
        assert result["total_files"] == 100


# ---------------------------------------------------------------------------
# write_cleanup_audit
# ---------------------------------------------------------------------------

class TestWriteCleanupAudit:
    def test_pre_audit_file_created(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_cleanup_audit("r1", "pre",
                            [{"url": "https://dst/f", "success": True}],
                            runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "cleanup_pre.json")
        assert os.path.isfile(path)

    def test_post_audit_file_created(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_cleanup_audit("r1", "post", [], runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "cleanup_post.json")
        assert os.path.isfile(path)

    def test_audit_records_round_trip(self, tmp_path):
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        audit = [{"url": "https://dst/f", "status_code": 204, "success": True}]
        write_cleanup_audit("r1", "pre", audit, runs_dir=str(tmp_path))
        path = os.path.join(str(tmp_path), "r1", "cleanup_pre.json")
        with open(path) as fh:
            result = json.load(fh)
        assert result == audit


# ---------------------------------------------------------------------------
# Atomic write — verify no partial state visible on replace
# ---------------------------------------------------------------------------

class TestAtomicManifestWrite:
    def test_manifest_never_empty_after_write(self, tmp_path):
        """Read manifest immediately after write must return valid JSON."""
        init_run_directory("r1", _config(), runs_dir=str(tmp_path))
        write_manifest("r1", _mapping(), _config(), runs_dir=str(tmp_path))
        # Overwrite via update to exercise rename path
        update_manifest("r1", [_subjob()], runs_dir=str(tmp_path))
        m = load_manifest("r1", runs_dir=str(tmp_path))
        assert isinstance(m, dict)
        assert "run_id" in m
