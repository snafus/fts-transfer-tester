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
