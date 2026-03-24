"""Unit tests for fts_framework.sequence.loader."""

import os
import tempfile

import pytest
import yaml

from fts_framework.exceptions import ConfigError
from fts_framework.sequence.loader import (
    apply_override,
    expand_range,
    generate_cases,
    load,
)


# ---------------------------------------------------------------------------
# expand_range
# ---------------------------------------------------------------------------

class TestExpandRange:
    def test_ascending(self):
        assert expand_range({"range": [10, 30, 10]}) == [10, 20, 30]

    def test_single_value(self):
        assert expand_range({"range": [5, 5, 1]}) == [5]

    def test_descending(self):
        assert expand_range({"range": [10, 2, -4]}) == [10, 6, 2]

    def test_step_2(self):
        assert expand_range({"range": [0, 6, 2]}) == [0, 2, 4, 6]

    def test_zero_step_raises(self):
        with pytest.raises(ConfigError, match="step must not be zero"):
            expand_range({"range": [1, 10, 0]})

    def test_wrong_length_raises(self):
        with pytest.raises(ConfigError, match="exactly 3"):
            expand_range({"range": [1, 10]})

    def test_float_value_raises(self):
        with pytest.raises(ConfigError, match="integer"):
            expand_range({"range": [1.0, 10, 1]})

    def test_not_dict_raises(self):
        with pytest.raises(ConfigError):
            expand_range([1, 10, 1])

    def test_missing_range_key_raises(self):
        with pytest.raises(ConfigError):
            expand_range({"step": 1})

    def test_empty_result_raises(self):
        # descending step but start < stop → empty
        with pytest.raises(ConfigError, match="empty"):
            expand_range({"range": [1, 10, -1]})


# ---------------------------------------------------------------------------
# generate_cases
# ---------------------------------------------------------------------------

class TestGenerateCases:
    def test_cartesian_single_param(self):
        sweep = {"parameters": {"transfer.max_files": [100, 200, 500]}}
        cases = generate_cases(sweep)
        assert cases == [
            {"transfer.max_files": 100},
            {"transfer.max_files": 200},
            {"transfer.max_files": 500},
        ]

    def test_cartesian_two_params(self):
        sweep = {
            "parameters": {
                "transfer.max_files": [100, 200],
                "transfer.chunk_size": [50, 100],
            }
        }
        cases = generate_cases(sweep)
        assert len(cases) == 4
        assert {"transfer.max_files": 100, "transfer.chunk_size": 50} in cases
        assert {"transfer.max_files": 200, "transfer.chunk_size": 100} in cases

    def test_cartesian_is_default_mode(self):
        sweep = {
            "parameters": {
                "transfer.max_files": [10, 20],
                "transfer.chunk_size": [5, 10],
            }
        }
        cases_explicit   = generate_cases(dict(mode="cartesian", **sweep))
        cases_default    = generate_cases(sweep)
        assert cases_explicit == cases_default

    def test_zip_mode(self):
        sweep = {
            "mode": "zip",
            "parameters": {
                "transfer.max_files":       [100, 200, 500],
                "transfer.source_pfns_file": ["a.txt", "b.txt", "c.txt"],
            },
        }
        cases = generate_cases(sweep)
        assert cases == [
            {"transfer.max_files": 100, "transfer.source_pfns_file": "a.txt"},
            {"transfer.max_files": 200, "transfer.source_pfns_file": "b.txt"},
            {"transfer.max_files": 500, "transfer.source_pfns_file": "c.txt"},
        ]

    def test_zip_unequal_lengths_raises(self):
        sweep = {
            "mode": "zip",
            "parameters": {
                "transfer.max_files":       [100, 200],
                "transfer.source_pfns_file": ["a.txt"],
            },
        }
        with pytest.raises(ConfigError, match="same length"):
            generate_cases(sweep)

    def test_bad_mode_raises(self):
        sweep = {"mode": "grid", "parameters": {"transfer.max_files": [1]}}
        with pytest.raises(ConfigError, match="cartesian.*zip"):
            generate_cases(sweep)

    def test_empty_parameters_raises(self):
        with pytest.raises(ConfigError, match="must not be empty"):
            generate_cases({"parameters": {}})

    def test_missing_parameters_raises(self):
        with pytest.raises(ConfigError, match="must not be empty"):
            generate_cases({})

    def test_scalar_param_treated_as_single_element(self):
        sweep = {"parameters": {"transfer.max_files": 42}}
        cases = generate_cases(sweep)
        assert cases == [{"transfer.max_files": 42}]

    def test_range_shorthand_expanded(self):
        sweep = {"parameters": {"transfer.max_files": {"range": [10, 30, 10]}}}
        cases = generate_cases(sweep)
        assert cases == [
            {"transfer.max_files": 10},
            {"transfer.max_files": 20},
            {"transfer.max_files": 30},
        ]

    def test_empty_list_param_raises(self):
        sweep = {"parameters": {"transfer.max_files": []}}
        with pytest.raises(ConfigError, match="empty"):
            generate_cases(sweep)


# ---------------------------------------------------------------------------
# apply_override
# ---------------------------------------------------------------------------

class TestApplyOverride:
    def _base(self):
        return {
            "transfer": {"max_files": None, "chunk_size": 200},
            "run":      {"run_id": None},
        }

    def test_sets_nested_value(self):
        cfg = self._base()
        apply_override(cfg, "transfer.max_files", 100)
        assert cfg["transfer"]["max_files"] == 100

    def test_does_not_affect_other_keys(self):
        cfg = self._base()
        apply_override(cfg, "transfer.max_files", 50)
        assert cfg["transfer"]["chunk_size"] == 200

    def test_creates_new_leaf_key(self):
        cfg = self._base()
        apply_override(cfg, "transfer.overwrite", True)
        assert cfg["transfer"]["overwrite"] is True

    def test_single_segment_raises(self):
        cfg = self._base()
        with pytest.raises(ConfigError, match="dot-separated"):
            apply_override(cfg, "transfer", 999)

    def test_missing_section_raises(self):
        cfg = self._base()
        with pytest.raises(ConfigError, match="section 'nosection' not found"):
            apply_override(cfg, "nosection.key", 1)

    def test_deep_three_levels(self):
        cfg = {"output": {"reports": {"csv": False}}}
        apply_override(cfg, "output.reports.csv", True)
        assert cfg["output"]["reports"]["csv"] is True


# ---------------------------------------------------------------------------
# load() — integration with temp files
# ---------------------------------------------------------------------------

class TestLoad:
    def _write_baseline(self, tmp_dir, content=None):
        """Write a minimal baseline config and return its path."""
        if content is None:
            content = {
                "fts":      {"endpoint": "https://fts.example.org:8446"},
                "tokens":   {"fts_submit": "tok", "source_read": "tok",
                             "dest_write": "tok"},
                "transfer": {"source_pfns_file": "s.txt",
                             "dst_prefix": "https://dst.example.org"},
            }
        path = os.path.join(tmp_dir, "baseline.yaml")
        with open(path, "w") as fh:
            yaml.dump(content, fh)
        return path

    def _write_params(self, tmp_dir, baseline_path, seq_content):
        path = os.path.join(tmp_dir, "params.yaml")
        data = {"baseline_config": baseline_path, "sequence": seq_content}
        with open(path, "w") as fh:
            yaml.dump(data, fh)
        return path

    def test_load_valid_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            bp = self._write_baseline(tmp)
            pp = self._write_params(tmp, bp, {
                "trials": 2,
                "label":  "test",
                "sweep": {
                    "parameters": {"transfer.max_files": [10, 20]},
                },
            })
            result = load(pp)
        assert result["trials"] == 2
        assert result["label"]  == "test"
        assert result["sweep_mode"] == "cartesian"
        assert len(result["cases"]) == 2
        assert result["output_base_dir"] == "sequences"

    def test_default_trials_is_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            bp = self._write_baseline(tmp)
            pp = self._write_params(tmp, bp, {
                "sweep": {"parameters": {"transfer.max_files": [10]}},
            })
            result = load(pp)
        assert result["trials"] == 1

    def test_default_output_base_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            bp = self._write_baseline(tmp)
            pp = self._write_params(tmp, bp, {
                "sweep": {"parameters": {"transfer.max_files": [10]}},
            })
            result = load(pp)
        assert result["output_base_dir"] == "sequences"

    def test_missing_file_raises(self):
        with pytest.raises(ConfigError, match="not found"):
            load("/tmp/does_not_exist_xyz.yaml")

    def test_missing_baseline_config_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            pp = os.path.join(tmp, "params.yaml")
            with open(pp, "w") as fh:
                yaml.dump({"baseline_config": "/nonexistent/path.yaml",
                           "sequence": {"sweep": {"parameters": {
                               "transfer.max_files": [10]}}}}, fh)
            with pytest.raises(ConfigError, match="not found"):
                load(pp)

    def test_no_baseline_config_key_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            pp = os.path.join(tmp, "params.yaml")
            with open(pp, "w") as fh:
                yaml.dump({"sequence": {}}, fh)
            with pytest.raises(ConfigError, match="baseline_config is required"):
                load(pp)

    def test_invalid_trials_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            bp = self._write_baseline(tmp)
            pp = self._write_params(tmp, bp, {
                "trials": 0,
                "sweep": {"parameters": {"transfer.max_files": [10]}},
            })
            with pytest.raises(ConfigError, match="trials"):
                load(pp)

    def test_label_none_allowed(self):
        with tempfile.TemporaryDirectory() as tmp:
            bp = self._write_baseline(tmp)
            pp = self._write_params(tmp, bp, {
                "label": None,
                "sweep": {"parameters": {"transfer.max_files": [10]}},
            })
            result = load(pp)
        assert result["label"] is None

    def test_zip_mode_propagated(self):
        with tempfile.TemporaryDirectory() as tmp:
            bp = self._write_baseline(tmp)
            pp = self._write_params(tmp, bp, {
                "sweep": {
                    "mode": "zip",
                    "parameters": {
                        "transfer.max_files":       [10, 20],
                        "transfer.source_pfns_file": ["a.txt", "b.txt"],
                    },
                },
            })
            result = load(pp)
        assert result["sweep_mode"] == "zip"
        assert len(result["cases"]) == 2
