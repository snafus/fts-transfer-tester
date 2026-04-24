"""
Unit tests for fts_framework.config.loader.

Tests cover: default application, SSL verify variants, all required-field
validations, type/range constraints, enum validation, file-level error
handling, deep-merge correctness, and section-type guards.
"""

import pytest
import yaml

from fts_framework.config.loader import load, _deep_merge, _apply_defaults
from fts_framework.exceptions import ConfigError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_yaml(data, path):
    """Serialise *data* as YAML to *path*."""
    with open(path, "w") as fh:
        yaml.dump(data, fh, default_flow_style=False)


def _base(tmp_path):
    """Return (path_str, data_dict) for a minimal valid config."""
    data = {
        "run": {"test_label": "test_campaign"},
        "fts": {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
        "tokens": {
            "fts_submit": "tok_submit",
            "source_read": "tok_read",
            "dest_write": "tok_write",
        },
        "transfer": {
            "source_pfns_file": "sources.txt",
            "dst_prefix": "https://storage.example.org/data",
        },
        "output": {"base_dir": "runs"},
    }
    path = str(tmp_path / "config.yaml")
    write_yaml(data, path)
    return path, data


def _write(tmp_path, data, name="c.yaml"):
    path = str(tmp_path / name)
    write_yaml(data, path)
    return path


# ---------------------------------------------------------------------------
# _deep_merge
# ---------------------------------------------------------------------------

class TestDeepMerge:
    def test_override_takes_precedence(self):
        result = _deep_merge({"a": 1, "b": 2}, {"b": 99})
        assert result["b"] == 99
        assert result["a"] == 1

    def test_two_level_nested_merge(self):
        defaults = {"outer": {"a": 1, "b": 2}}
        override = {"outer": {"b": 99}}
        result = _deep_merge(defaults, override)
        assert result["outer"]["a"] == 1
        assert result["outer"]["b"] == 99

    def test_three_level_nested_merge(self):
        defaults = {"l1": {"l2": {"a": 1, "b": 2}}}
        override = {"l1": {"l2": {"b": 99}}}
        result = _deep_merge(defaults, override)
        assert result["l1"]["l2"]["a"] == 1
        assert result["l1"]["l2"]["b"] == 99

    def test_new_keys_in_override_added(self):
        result = _deep_merge({"a": 1}, {"b": 2})
        assert result["b"] == 2

    def test_inputs_not_mutated(self):
        defaults = {"a": {"x": 1}}
        override = {"a": {"y": 2}}
        _deep_merge(defaults, override)
        assert "y" not in defaults["a"]

    def test_scalar_override_replaces_dict_default(self):
        # If user explicitly sets a nested section to a scalar,
        # the scalar wins (section-type guard catches this before merge in production)
        result = _deep_merge({"a": {"x": 1}}, {"a": "flat"})
        assert result["a"] == "flat"

    def test_empty_override_returns_copy_of_defaults(self):
        defaults = {"a": 1, "b": {"c": 2}}
        result = _deep_merge(defaults, {})
        assert result == defaults
        assert result is not defaults


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

class TestDefaults:
    def test_loads_successfully(self, tmp_path):
        path, _ = _base(tmp_path)
        config = load(path)
        assert config["run"]["test_label"] == "test_campaign"

    def test_default_run_id_is_none(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["run"]["run_id"] is None

    def test_default_max_files_is_none(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["transfer"]["max_files"] is None

    def test_default_chunk_size(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["transfer"]["chunk_size"] == 200

    def test_default_priority(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["transfer"]["priority"] == 3

    def test_default_activity(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["transfer"]["activity"] == "default"

    def test_default_preserve_extension_false(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["transfer"]["preserve_extension"] is False

    def test_default_job_metadata_empty_dict(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["transfer"]["job_metadata"] == {}

    def test_default_checksum_algorithm(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["transfer"]["checksum_algorithm"] == "adler32"

    def test_default_verify_checksum(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["transfer"]["verify_checksum"] == "both"

    def test_default_scan_window(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["submission"]["scan_window_s"] == 300

    def test_default_want_digest_workers(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["concurrency"]["want_digest_workers"] == 8

    def test_default_framework_retry_off(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["retry"]["framework_retry_max"] == 0

    def test_default_fts_retry_max(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["retry"]["fts_retry_max"] == 2

    def test_default_min_success_threshold(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["retry"]["min_success_threshold"] == pytest.approx(0.95)

    def test_default_cleanup_before_false(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["cleanup"]["before"] is False

    def test_default_cleanup_after_false(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["cleanup"]["after"] is False

    def test_default_reports_console_true(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["output"]["reports"]["console"] is True

    def test_default_reports_html_false(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["output"]["reports"]["html"] is False

    def test_user_value_overrides_default(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["chunk_size"] = 50
        write_yaml(data, path)
        assert load(path)["transfer"]["chunk_size"] == 50

    def test_partial_section_override_preserves_other_defaults(self, tmp_path):
        """Supply only one field in a defaulted section; all others must survive."""
        path, data = _base(tmp_path)
        data["polling"] = {"initial_interval_s": 60}   # override only one field
        write_yaml(data, path)
        cfg = load(path)
        assert cfg["polling"]["initial_interval_s"] == 60
        assert cfg["polling"]["max_interval_s"] == 300       # default preserved
        assert cfg["polling"]["campaign_timeout_s"] == 86400  # default preserved

    def test_user_job_metadata_preserved(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["job_metadata"] = {"campaign": "q1", "owner": "alice"}
        write_yaml(data, path)
        meta = load(path)["transfer"]["job_metadata"]
        assert meta["campaign"] == "q1"
        assert meta["owner"] == "alice"

    def test_unknown_section_passes_through(self, tmp_path):
        """Extra sections not in _DEFAULTS must survive to the returned config."""
        path, data = _base(tmp_path)
        data["custom_extension"] = {"foo": "bar", "count": 42}
        write_yaml(data, path)
        cfg = load(path)
        assert cfg["custom_extension"]["foo"] == "bar"

    def test_reports_partial_override_preserves_other_report_defaults(self, tmp_path):
        """Override only one report flag; others must keep their defaults."""
        path, data = _base(tmp_path)
        data["output"] = {"base_dir": "runs", "reports": {"html": True}}
        write_yaml(data, path)
        cfg = load(path)
        assert cfg["output"]["reports"]["html"] is True
        assert cfg["output"]["reports"]["console"] is True   # default preserved
        assert cfg["output"]["reports"]["json"] is True      # default preserved


# ---------------------------------------------------------------------------
# SSL verify
# ---------------------------------------------------------------------------

class TestSSLVerify:
    def test_ssl_verify_true(self, tmp_path):
        path, _ = _base(tmp_path)
        assert load(path)["fts"]["ssl_verify"] is True

    def test_ssl_verify_false(self, tmp_path):
        path, data = _base(tmp_path)
        data["fts"]["ssl_verify"] = False
        write_yaml(data, path)
        assert load(path)["fts"]["ssl_verify"] is False

    def test_ssl_verify_ca_bundle_path(self, tmp_path):
        ca = tmp_path / "ca.pem"
        ca.write_text("fake-ca")
        path, data = _base(tmp_path)
        data["fts"]["ssl_verify"] = str(ca)
        write_yaml(data, path)
        assert load(path)["fts"]["ssl_verify"] == str(ca)

    def test_ssl_verify_missing_path_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["fts"]["ssl_verify"] = "/nonexistent/ca.pem"
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="CA bundle path does not exist"):
            load(path)

    def test_ssl_verify_bad_type_integer_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["fts"]["ssl_verify"] = 42
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="ssl_verify"):
            load(path)

    def test_ssl_verify_quoted_true_string_raises(self, tmp_path):
        """Quoted 'true' is a string, not a boolean — must be rejected as a non-existent path."""
        path, data = _base(tmp_path)
        # Write raw YAML to force quoted string (yaml.dump would serialise bool correctly)
        with open(path, "w") as fh:
            fh.write(
                "run:\n  test_label: test_campaign\n"
                "fts:\n  endpoint: https://fts.example.org:8446\n  ssl_verify: 'true'\n"
                "tokens:\n  fts_submit: t\n  source_read: t\n  dest_write: t\n"
                "transfer:\n  source_pfns_file: s.txt\n  dst_prefix: https://s.example.org/d\n"
                "output:\n  base_dir: runs\n"
            )
        with pytest.raises(ConfigError, match="CA bundle path does not exist"):
            load(path)


# ---------------------------------------------------------------------------
# Section type guard
# ---------------------------------------------------------------------------

class TestSectionTypeGuard:
    def test_transfer_null_treated_as_absent_fails_required_fields(self, tmp_path):
        """``transfer: null`` is treated as absent; required fields then fail validation.

        This is preferable to a type error — the user learns what is actually missing.
        """
        with open(str(tmp_path / "c.yaml"), "w") as fh:
            fh.write(
                "run:\n  test_label: t\n"
                "fts:\n  endpoint: https://fts.example.org:8446\n  ssl_verify: true\n"
                "tokens:\n  fts_submit: t\n  source_read: t\n  dest_write: t\n"
                "transfer: null\n"
                "output:\n  base_dir: runs\n"
            )
        with pytest.raises(ConfigError, match="transfer\\.source_pfns_file"):
            load(str(tmp_path / "c.yaml"))

    def test_cleanup_scalar_raises(self, tmp_path):
        path, data = _base(tmp_path)
        # Write raw to force non-dict value
        with open(path, "w") as fh:
            fh.write(
                "run:\n  test_label: t\n"
                "fts:\n  endpoint: https://fts.example.org:8446\n  ssl_verify: true\n"
                "tokens:\n  fts_submit: t\n  source_read: t\n  dest_write: t\n"
                "transfer:\n  source_pfns_file: s.txt\n  dst_prefix: https://s.example.org/d\n"
                "output:\n  base_dir: runs\n"
                "cleanup: false\n"
            )
        with pytest.raises(ConfigError, match="must be a YAML mapping"):
            load(path)


# ---------------------------------------------------------------------------
# Required field validation
# ---------------------------------------------------------------------------

class TestRequiredFields:
    def test_missing_test_label_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["run"] = {}
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="run.test_label"):
            load(path)

    def test_missing_endpoint_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["fts"] = {"ssl_verify": True}
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="fts.endpoint"):
            load(path)

    def test_endpoint_not_https_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["fts"]["endpoint"] = "http://fts.example.org:8446"
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="https://"):
            load(path)

    def test_missing_fts_section_raises(self, tmp_path):
        path, data = _base(tmp_path)
        del data["fts"]
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="'fts'"):
            load(path)

    def test_missing_tokens_section_raises(self, tmp_path):
        path, data = _base(tmp_path)
        del data["tokens"]
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="'tokens'"):
            load(path)

    def test_missing_fts_submit_token_raises(self, tmp_path):
        path, data = _base(tmp_path)
        del data["tokens"]["fts_submit"]
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="tokens.fts_submit"):
            load(path)

    def test_missing_source_read_token_raises(self, tmp_path):
        path, data = _base(tmp_path)
        del data["tokens"]["source_read"]
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="tokens.source_read"):
            load(path)

    def test_missing_dest_write_token_raises(self, tmp_path):
        path, data = _base(tmp_path)
        del data["tokens"]["dest_write"]
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="tokens.dest_write"):
            load(path)

    def test_missing_source_pfns_file_raises(self, tmp_path):
        path, data = _base(tmp_path)
        del data["transfer"]["source_pfns_file"]
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="transfer.source_pfns_file"):
            load(path)

    def test_missing_dst_prefix_raises(self, tmp_path):
        path, data = _base(tmp_path)
        del data["transfer"]["dst_prefix"]
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="transfer.dst_prefix"):
            load(path)

    def test_dst_prefix_not_https_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["dst_prefix"] = "http://storage.example.org/data"
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="dst_prefix"):
            load(path)

    def test_dst_prefix_davs_accepted(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["dst_prefix"] = "davs://xrootd01.example.org:1094/path/testarea"
        write_yaml(data, path)
        cfg = load(path)
        assert cfg["transfer"]["dst_prefix"] == "davs://xrootd01.example.org:1094/path/testarea"


# ---------------------------------------------------------------------------
# Value constraint validation
# ---------------------------------------------------------------------------

class TestValueConstraints:
    def test_chunk_size_201_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["chunk_size"] = 201
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="chunk_size"):
            load(path)

    def test_chunk_size_0_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["chunk_size"] = 0
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="chunk_size"):
            load(path)

    def test_chunk_size_200_valid(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["chunk_size"] = 200
        write_yaml(data, path)
        assert load(path)["transfer"]["chunk_size"] == 200

    def test_priority_6_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["priority"] = 6
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="priority"):
            load(path)

    def test_priority_0_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["priority"] = 0
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="priority"):
            load(path)

    def test_priority_1_and_5_valid(self, tmp_path):
        for p in (1, 5):
            path, data = _base(tmp_path)
            data["transfer"]["priority"] = p
            write_yaml(data, path)
            assert load(path)["transfer"]["priority"] == p

    def test_max_files_valid_positive_int(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["max_files"] = 50
        write_yaml(data, path)
        assert load(path)["transfer"]["max_files"] == 50

    def test_max_files_zero_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["max_files"] = 0
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="max_files"):
            load(path)

    def test_max_files_negative_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["max_files"] = -1
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="max_files"):
            load(path)

    def test_max_files_float_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["max_files"] = 10.0
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="max_files"):
            load(path)

    def test_max_files_null_is_valid(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["max_files"] = None
        write_yaml(data, path)
        assert load(path)["transfer"]["max_files"] is None

    def test_scan_window_below_60_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["submission"] = {"scan_window_s": 59}
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="scan_window_s"):
            load(path)

    def test_scan_window_60_valid(self, tmp_path):
        path, data = _base(tmp_path)
        data["submission"] = {"scan_window_s": 60}
        write_yaml(data, path)
        assert load(path)["submission"]["scan_window_s"] == 60

    def test_success_threshold_above_1_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["retry"] = {"min_success_threshold": 1.01}
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="min_success_threshold"):
            load(path)

    def test_success_threshold_zero_valid(self, tmp_path):
        path, data = _base(tmp_path)
        data["retry"] = {"min_success_threshold": 0.0}
        write_yaml(data, path)
        assert load(path)["retry"]["min_success_threshold"] == pytest.approx(0.0)

    def test_backoff_below_1_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["polling"] = {
            "backoff_multiplier": 0.9,
            "initial_interval_s": 30,
            "max_interval_s": 300,
            "campaign_timeout_s": 86400,
        }
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="backoff_multiplier"):
            load(path)

    def test_framework_retry_negative_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["retry"] = {"framework_retry_max": -1, "min_success_threshold": 0.95}
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="framework_retry_max"):
            load(path)

    def test_fts_retry_max_negative_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["retry"] = {"fts_retry_max": -1}
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="fts_retry_max"):
            load(path)

    def test_fts_retry_max_string_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["retry"] = {"fts_retry_max": "two"}
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="fts_retry_max"):
            load(path)

    def test_polling_interval_float_raises_with_helpful_message(self, tmp_path):
        """YAML float literal (30.0) must produce a clear actionable error."""
        path, data = _base(tmp_path)
        data["polling"] = {
            "initial_interval_s": 30.0,
            "max_interval_s": 300,
            "campaign_timeout_s": 86400,
            "backoff_multiplier": 1.5,
        }
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="decimal point"):
            load(path)

    def test_want_digest_workers_zero_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["concurrency"] = {"want_digest_workers": 0}
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="want_digest_workers"):
            load(path)


# ---------------------------------------------------------------------------
# Enum validation
# ---------------------------------------------------------------------------

class TestEnumValidation:
    def test_invalid_checksum_algorithm_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["checksum_algorithm"] = "sha256"
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="checksum_algorithm"):
            load(path)

    def test_valid_checksum_algorithm(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["checksum_algorithm"] = "adler32"
        write_yaml(data, path)
        assert load(path)["transfer"]["checksum_algorithm"] == "adler32"

    def test_invalid_verify_checksum_raises(self, tmp_path):
        path, data = _base(tmp_path)
        data["transfer"]["verify_checksum"] = "always"
        write_yaml(data, path)
        with pytest.raises(ConfigError, match="verify_checksum"):
            load(path)

    def test_all_valid_verify_checksum_values(self, tmp_path):
        for mode in ("both", "source", "target", "none"):
            path, data = _base(tmp_path)
            data["transfer"]["verify_checksum"] = mode
            write_yaml(data, path)
            assert load(path)["transfer"]["verify_checksum"] == mode


# ---------------------------------------------------------------------------
# File-level errors
# ---------------------------------------------------------------------------

class TestFileErrors:
    def test_file_not_found_raises(self):
        with pytest.raises(ConfigError, match="Cannot read"):
            load("/nonexistent/path/config.yaml")

    def test_invalid_yaml_raises(self, tmp_path):
        path = str(tmp_path / "bad.yaml")
        with open(path, "w") as fh:
            fh.write("key: [unclosed bracket\n")
        with pytest.raises(ConfigError, match="YAML parse error"):
            load(path)

    def test_non_mapping_yaml_raises(self, tmp_path):
        path = str(tmp_path / "list.yaml")
        with open(path, "w") as fh:
            fh.write("- item1\n- item2\n")
        with pytest.raises(ConfigError, match="YAML mapping"):
            load(path)


# ---------------------------------------------------------------------------
# Token resolution
# ---------------------------------------------------------------------------

def _base_no_tokens(tmp_path):
    """Return (path_str, data_dict) for a minimal valid config with no tokens section."""
    data = {
        "run": {"test_label": "test_campaign"},
        "fts": {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
        "transfer": {
            "source_pfns_file": "sources.txt",
            "dst_prefix": "https://storage.example.org/data",
        },
        "output": {"base_dir": "runs"},
    }
    path = str(tmp_path / "config_no_tokens.yaml")
    write_yaml(data, path)
    return path, data


class TestTokenResolution:
    def test_yaml_tokens_used_when_no_overrides(self, tmp_path):
        path, _ = _base(tmp_path)
        config = load(path)
        assert config["tokens"]["fts_submit"] == "tok_submit"
        assert config["tokens"]["source_read"] == "tok_read"
        assert config["tokens"]["dest_write"] == "tok_write"

    def test_shared_env_var_fills_all_roles(self, tmp_path, monkeypatch):
        path, _ = _base_no_tokens(tmp_path)
        monkeypatch.setenv("FTS_TOKEN", "shared_from_env")
        config = load(path)
        assert config["tokens"]["fts_submit"] == "shared_from_env"
        assert config["tokens"]["source_read"] == "shared_from_env"
        assert config["tokens"]["dest_write"] == "shared_from_env"

    def test_per_role_env_var_overrides_shared_env_var(self, tmp_path, monkeypatch):
        path, _ = _base_no_tokens(tmp_path)
        monkeypatch.setenv("FTS_TOKEN", "shared")
        monkeypatch.setenv("FTS_SUBMIT_TOKEN", "submit_specific")
        config = load(path)
        assert config["tokens"]["fts_submit"] == "submit_specific"
        assert config["tokens"]["source_read"] == "shared"
        assert config["tokens"]["dest_write"] == "shared"

    def test_all_per_role_env_vars(self, tmp_path, monkeypatch):
        path, _ = _base_no_tokens(tmp_path)
        monkeypatch.setenv("FTS_SUBMIT_TOKEN", "env_submit")
        monkeypatch.setenv("SOURCE_READ_TOKEN", "env_read")
        monkeypatch.setenv("DEST_WRITE_TOKEN", "env_write")
        config = load(path)
        assert config["tokens"]["fts_submit"] == "env_submit"
        assert config["tokens"]["source_read"] == "env_read"
        assert config["tokens"]["dest_write"] == "env_write"

    def test_shared_cli_token_overrides_env_var(self, tmp_path, monkeypatch):
        path, _ = _base_no_tokens(tmp_path)
        monkeypatch.setenv("FTS_TOKEN", "env_shared")
        config = load(path, token="cli_shared")
        assert config["tokens"]["fts_submit"] == "cli_shared"
        assert config["tokens"]["source_read"] == "cli_shared"
        assert config["tokens"]["dest_write"] == "cli_shared"

    def test_per_role_cli_overrides_shared_cli(self, tmp_path, monkeypatch):
        path, _ = _base_no_tokens(tmp_path)
        monkeypatch.setenv("FTS_TOKEN", "env_shared")
        config = load(path, token="cli_shared", fts_submit_token="cli_submit_only")
        assert config["tokens"]["fts_submit"] == "cli_submit_only"
        assert config["tokens"]["source_read"] == "cli_shared"
        assert config["tokens"]["dest_write"] == "cli_shared"

    def test_per_role_cli_overrides_yaml(self, tmp_path):
        path, _ = _base(tmp_path)
        config = load(path, fts_submit_token="new_submit")
        assert config["tokens"]["fts_submit"] == "new_submit"
        assert config["tokens"]["source_read"] == "tok_read"
        assert config["tokens"]["dest_write"] == "tok_write"

    def test_yaml_tokens_absent_with_cli_token_succeeds(self, tmp_path):
        path, _ = _base_no_tokens(tmp_path)
        config = load(path, token="all_in_one")
        assert config["tokens"]["fts_submit"] == "all_in_one"
        assert config["tokens"]["source_read"] == "all_in_one"
        assert config["tokens"]["dest_write"] == "all_in_one"

    def test_yaml_tokens_absent_with_all_per_role_cli_succeeds(self, tmp_path):
        path, _ = _base_no_tokens(tmp_path)
        config = load(
            path,
            fts_submit_token="s1",
            source_read_token="s2",
            dest_write_token="s3",
        )
        assert config["tokens"]["fts_submit"] == "s1"
        assert config["tokens"]["source_read"] == "s2"
        assert config["tokens"]["dest_write"] == "s3"

    def test_yaml_tokens_absent_with_no_override_raises(self, tmp_path):
        path, _ = _base_no_tokens(tmp_path)
        with pytest.raises(ConfigError, match="'tokens'"):
            load(path)

    def test_empty_env_var_ignored(self, tmp_path, monkeypatch):
        path, _ = _base(tmp_path)
        monkeypatch.setenv("FTS_TOKEN", "")
        monkeypatch.setenv("FTS_SUBMIT_TOKEN", "")
        config = load(path)
        assert config["tokens"]["fts_submit"] == "tok_submit"

    def test_env_var_overrides_yaml_token(self, tmp_path, monkeypatch):
        path, _ = _base(tmp_path)
        monkeypatch.setenv("FTS_SUBMIT_TOKEN", "env_wins")
        config = load(path)
        assert config["tokens"]["fts_submit"] == "env_wins"
        assert config["tokens"]["source_read"] == "tok_read"

    def test_full_priority_chain(self, tmp_path, monkeypatch):
        """Demonstrate all priority levels: per-role CLI > per-role env > shared env > YAML.

        fts_submit: per-role CLI wins
        source_read: per-role env wins over shared env (no CLI supplied)
        dest_write:  shared env wins over YAML (no per-role env or CLI supplied)
        """
        path, _ = _base(tmp_path)
        monkeypatch.setenv("FTS_TOKEN", "env_shared")
        monkeypatch.setenv("SOURCE_READ_TOKEN", "env_read")
        config = load(path, fts_submit_token="cli_submit")
        assert config["tokens"]["fts_submit"] == "cli_submit"
        assert config["tokens"]["source_read"] == "env_read"
        assert config["tokens"]["dest_write"] == "env_shared"


# ---------------------------------------------------------------------------
# OIDC token resolution
# ---------------------------------------------------------------------------

class TestOidcTokenResolution:
    """Tests for OIDC client-credentials token generation (6th priority)."""

    def _oidc_config(self, tmp_path, roles=None):
        """Return (path, data) with oidc section enabled."""
        data = {
            "run":   {"test_label": "oidc_test"},
            "fts":   {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
            "tokens": {},
            "transfer": {
                "source_pfns_file": "s.txt",
                "dst_prefix": "https://dst.example.org/data",
            },
            "oidc": {
                "enabled": True,
                "env_file": str(tmp_path / "nonexistent.env"),
                "roles": roles or {
                    "fts_submit":  {
                        "token_endpoint":   "https://iam.example.org/token",
                        "client_id_var":    "FTS_CLIENT_ID",
                        "client_secret_var": "FTS_CLIENT_SECRET",
                        "scope":            "openid profile",
                    },
                    "source_read": {
                        "token_endpoint":   "https://iam.example.org/token",
                        "client_id_var":    "SRC_CLIENT_ID",
                        "client_secret_var": "SRC_CLIENT_SECRET",
                        "scope":            "openid storage.read:/",
                    },
                    "dest_write": {
                        "token_endpoint":   "https://iam.example.org/token",
                        "client_id_var":    "DST_CLIENT_ID",
                        "client_secret_var": "DST_CLIENT_SECRET",
                        "scope":            "openid storage.modify:/",
                    },
                },
            },
        }
        import os
        path = str(tmp_path / "config.yaml")
        write_yaml(data, path)
        return path, data

    def test_oidc_tokens_injected_when_enabled(self, tmp_path, monkeypatch):
        import responses as rsps_lib
        monkeypatch.setenv("FTS_CLIENT_ID",    "cid_fts")
        monkeypatch.setenv("FTS_CLIENT_SECRET", "csec_fts")
        monkeypatch.setenv("SRC_CLIENT_ID",    "cid_src")
        monkeypatch.setenv("SRC_CLIENT_SECRET", "csec_src")
        monkeypatch.setenv("DST_CLIENT_ID",    "cid_dst")
        monkeypatch.setenv("DST_CLIENT_SECRET", "csec_dst")

        path, _ = self._oidc_config(tmp_path)
        with rsps_lib.RequestsMock() as rsps:
            rsps.add(rsps_lib.POST, "https://iam.example.org/token",
                     json={"access_token": "tok_fts"}, status=200)
            rsps.add(rsps_lib.POST, "https://iam.example.org/token",
                     json={"access_token": "tok_src"}, status=200)
            rsps.add(rsps_lib.POST, "https://iam.example.org/token",
                     json={"access_token": "tok_dst"}, status=200)
            config = load(path)

        assert config["tokens"]["fts_submit"]  == "tok_fts"
        assert config["tokens"]["source_read"] == "tok_src"
        assert config["tokens"]["dest_write"]  == "tok_dst"

    def test_existing_token_not_overwritten_by_oidc(self, tmp_path, monkeypatch):
        """If a role already has a token from CLI/env/YAML, OIDC is skipped for it."""
        import responses as rsps_lib
        monkeypatch.setenv("SRC_CLIENT_ID",    "cid_src")
        monkeypatch.setenv("SRC_CLIENT_SECRET", "csec_src")
        monkeypatch.setenv("DST_CLIENT_ID",    "cid_dst")
        monkeypatch.setenv("DST_CLIENT_SECRET", "csec_dst")

        path, _ = self._oidc_config(tmp_path)
        with rsps_lib.RequestsMock() as rsps:
            rsps.add(rsps_lib.POST, "https://iam.example.org/token",
                     json={"access_token": "tok_src"}, status=200)
            rsps.add(rsps_lib.POST, "https://iam.example.org/token",
                     json={"access_token": "tok_dst"}, status=200)
            config = load(path, fts_submit_token="cli_fts_token")

        assert config["tokens"]["fts_submit"]  == "cli_fts_token"
        assert config["tokens"]["source_read"] == "tok_src"
        assert config["tokens"]["dest_write"]  == "tok_dst"

    def test_oidc_disabled_skips_fetch(self, tmp_path):
        """When oidc.enabled=False, no token fetch is attempted."""
        data = {
            "run":    {"test_label": "t"},
            "fts":    {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
            "tokens": {"fts_submit": "t", "source_read": "t", "dest_write": "t"},
            "transfer": {
                "source_pfns_file": "s.txt",
                "dst_prefix": "https://dst.example.org/data",
            },
            "oidc": {"enabled": False},
        }
        path = str(tmp_path / "config.yaml")
        write_yaml(data, path)
        # No HTTP mock — would fail if any request is made
        config = load(path)
        assert config["tokens"]["fts_submit"] == "t"

    def test_missing_client_id_var_raises(self, tmp_path, monkeypatch):
        """Missing env var for client_id raises ConfigError."""
        # Don't set FTS_CLIENT_ID — only set source/dest vars
        monkeypatch.setenv("SRC_CLIENT_ID",    "cid_src")
        monkeypatch.setenv("SRC_CLIENT_SECRET", "csec_src")
        monkeypatch.setenv("DST_CLIENT_ID",    "cid_dst")
        monkeypatch.setenv("DST_CLIENT_SECRET", "csec_dst")
        monkeypatch.delenv("FTS_CLIENT_ID",    raising=False)
        monkeypatch.delenv("FTS_CLIENT_SECRET", raising=False)

        path, _ = self._oidc_config(tmp_path)
        with pytest.raises(ConfigError, match="client_id_var"):
            load(path)

    def test_invalid_token_endpoint_raises(self, tmp_path):
        """Non-https token_endpoint raises ConfigError during validation."""
        path, _ = self._oidc_config(tmp_path, roles={
            "fts_submit": {
                "token_endpoint":   "http://insecure.example.org/token",
                "client_id_var":    "FTS_CLIENT_ID",
                "client_secret_var": "FTS_CLIENT_SECRET",
                "scope":            "openid",
            },
        })
        with pytest.raises(ConfigError, match="https://"):
            load(path)

    def test_env_file_values_used_for_credentials(self, tmp_path, monkeypatch):
        """Credentials can be sourced from .env file."""
        import responses as rsps_lib
        env_file = str(tmp_path / ".env")
        with open(env_file, "w") as fh:
            fh.write("FTS_CLIENT_ID=file_cid\nFTS_CLIENT_SECRET=file_csec\n")
            fh.write("SRC_CLIENT_ID=file_src_cid\nSRC_CLIENT_SECRET=file_src_csec\n")
            fh.write("DST_CLIENT_ID=file_dst_cid\nDST_CLIENT_SECRET=file_dst_csec\n")

        monkeypatch.delenv("FTS_CLIENT_ID",    raising=False)
        monkeypatch.delenv("FTS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("SRC_CLIENT_ID",    raising=False)
        monkeypatch.delenv("SRC_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("DST_CLIENT_ID",    raising=False)
        monkeypatch.delenv("DST_CLIENT_SECRET", raising=False)

        # Build config pointing to the .env file we just wrote
        roles = {
            "fts_submit":  {
                "token_endpoint":   "https://iam.example.org/token",
                "client_id_var":    "FTS_CLIENT_ID",
                "client_secret_var": "FTS_CLIENT_SECRET",
                "scope":            "openid",
            },
            "source_read": {
                "token_endpoint":   "https://iam.example.org/token",
                "client_id_var":    "SRC_CLIENT_ID",
                "client_secret_var": "SRC_CLIENT_SECRET",
                "scope":            "openid storage.read:/",
            },
            "dest_write": {
                "token_endpoint":   "https://iam.example.org/token",
                "client_id_var":    "DST_CLIENT_ID",
                "client_secret_var": "DST_CLIENT_SECRET",
                "scope":            "openid storage.modify:/",
            },
        }
        data = {
            "run":   {"test_label": "oidc_env_file"},
            "fts":   {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
            "tokens": {},
            "transfer": {
                "source_pfns_file": "s.txt",
                "dst_prefix": "https://dst.example.org/data",
            },
            "oidc": {"enabled": True, "env_file": env_file, "roles": roles},
        }
        path = str(tmp_path / "config.yaml")
        write_yaml(data, path)

        with rsps_lib.RequestsMock() as rsps:
            rsps.add(rsps_lib.POST, "https://iam.example.org/token",
                     json={"access_token": "tok_fts"}, status=200)
            rsps.add(rsps_lib.POST, "https://iam.example.org/token",
                     json={"access_token": "tok_src"}, status=200)
            rsps.add(rsps_lib.POST, "https://iam.example.org/token",
                     json={"access_token": "tok_dst"}, status=200)
            config = load(path)

        assert config["tokens"]["fts_submit"]  == "tok_fts"
        assert config["tokens"]["source_read"] == "tok_src"
        assert config["tokens"]["dest_write"]  == "tok_dst"
