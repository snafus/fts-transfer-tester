"""Unit tests for fts_framework.auth.env_loader."""

import os

import pytest

from fts_framework.auth.env_loader import load_env_file, resolve_var


def _write_env(tmp_path, content):
    path = os.path.join(str(tmp_path), ".env")
    with open(path, "w") as fh:
        fh.write(content)
    return path


class TestLoadEnvFile:
    def test_simple_key_value(self, tmp_path):
        path = _write_env(tmp_path, "FOO=bar\n")
        result = load_env_file(path)
        assert result["FOO"] == "bar"

    def test_comment_lines_skipped(self, tmp_path):
        path = _write_env(tmp_path, "# comment\nFOO=bar\n")
        result = load_env_file(path)
        assert "# comment" not in result
        assert result["FOO"] == "bar"

    def test_blank_lines_skipped(self, tmp_path):
        path = _write_env(tmp_path, "\nFOO=bar\n\n")
        result = load_env_file(path)
        assert result == {"FOO": "bar"}

    def test_double_quoted_value_stripped(self, tmp_path):
        path = _write_env(tmp_path, 'FOO="hello world"\n')
        result = load_env_file(path)
        assert result["FOO"] == "hello world"

    def test_single_quoted_value_stripped(self, tmp_path):
        path = _write_env(tmp_path, "FOO='hello world'\n")
        result = load_env_file(path)
        assert result["FOO"] == "hello world"

    def test_value_with_equals_sign(self, tmp_path):
        path = _write_env(tmp_path, "FOO=a=b\n")
        result = load_env_file(path)
        assert result["FOO"] == "a=b"

    def test_env_wins_over_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FOO", "from_env")
        path = _write_env(tmp_path, "FOO=from_file\n")
        result = load_env_file(path)
        assert result["FOO"] == "from_env"

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(IOError):
            load_env_file(os.path.join(str(tmp_path), "nonexistent.env"))

    def test_line_without_equals_skipped(self, tmp_path):
        path = _write_env(tmp_path, "NOEQUALS\nFOO=bar\n")
        result = load_env_file(path)
        assert "NOEQUALS" not in result
        assert result["FOO"] == "bar"


class TestResolveVar:
    def test_env_wins_over_file(self):
        env = {"MY_VAR": "from_env"}
        file_vars = {"MY_VAR": "from_file"}
        assert resolve_var("MY_VAR", env, file_vars) == "from_env"

    def test_file_used_when_env_absent(self):
        env = {}
        file_vars = {"MY_VAR": "from_file"}
        assert resolve_var("MY_VAR", env, file_vars) == "from_file"

    def test_none_when_absent_everywhere(self):
        assert resolve_var("MISSING", {}, {}) is None
