"""
Unit tests for fts_framework.inventory.loader.
"""

import pytest

from fts_framework.inventory.loader import load, _parse, _validate
from fts_framework.exceptions import InventoryError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_pfns(lines, path):
    """Write *lines* (list of str) to *path*, one per line."""
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# _parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_pfns_in_order(self):
        lines = ["https://a.example.org/f1.dat\n", "https://a.example.org/f2.dat\n"]
        assert _parse(lines) == [
            "https://a.example.org/f1.dat",
            "https://a.example.org/f2.dat",
        ]

    def test_skips_blank_lines(self):
        lines = ["https://a.example.org/f1.dat\n", "\n", "   \n",
                 "https://a.example.org/f2.dat\n"]
        result = _parse(lines)
        assert len(result) == 2

    def test_skips_comment_lines(self):
        lines = ["# this is a comment\n", "https://a.example.org/f1.dat\n"]
        result = _parse(lines)
        assert len(result) == 1
        assert result[0] == "https://a.example.org/f1.dat"

    def test_strips_leading_trailing_whitespace(self):
        lines = ["  https://a.example.org/f1.dat  \n"]
        assert _parse(lines) == ["https://a.example.org/f1.dat"]

    def test_empty_input_returns_empty_list(self):
        assert _parse([]) == []

    def test_all_comments_returns_empty(self):
        assert _parse(["# comment\n", "# another\n"]) == []


# ---------------------------------------------------------------------------
# _validate
# ---------------------------------------------------------------------------

class TestValidate:
    def test_empty_list_raises(self):
        with pytest.raises(InventoryError, match="no entries"):
            _validate([], "/some/file.txt")

    def test_duplicate_raises(self):
        pfns = [
            "https://a.example.org/f1.dat",
            "https://a.example.org/f1.dat",
        ]
        with pytest.raises(InventoryError, match="duplicate"):
            _validate(pfns, "/some/file.txt")

    def test_multiple_distinct_duplicates_reported(self):
        pfns = [
            "https://a.example.org/f1.dat",
            "https://a.example.org/f1.dat",
            "https://a.example.org/f2.dat",
            "https://a.example.org/f2.dat",
        ]
        with pytest.raises(InventoryError, match="2"):
            _validate(pfns, "/some/file.txt")

    def test_pfn_appearing_three_times_counts_as_one_duplicated(self):
        """A PFN appearing N times contributes 1 to the distinct duplicate count."""
        pfns = [
            "https://a.example.org/f1.dat",
            "https://a.example.org/f1.dat",
            "https://a.example.org/f1.dat",
        ]
        with pytest.raises(InventoryError, match="1"):
            _validate(pfns, "/some/file.txt")

    def test_unique_pfns_passes(self):
        pfns = [
            "https://a.example.org/f1.dat",
            "https://a.example.org/f2.dat",
        ]
        _validate(pfns, "/some/file.txt")  # should not raise


# ---------------------------------------------------------------------------
# load (integration of all helpers)
# ---------------------------------------------------------------------------

class TestLoad:
    def test_load_basic(self, tmp_path):
        p = tmp_path / "sources.txt"
        write_pfns([
            "https://storage.example.org/data/file001.dat",
            "https://storage.example.org/data/file002.dat",
        ], str(p))
        pfns = load(str(p))
        assert len(pfns) == 2
        assert pfns[0] == "https://storage.example.org/data/file001.dat"

    def test_load_skips_comments_and_blanks(self, tmp_path):
        p = tmp_path / "sources.txt"
        write_pfns([
            "# header comment",
            "",
            "https://storage.example.org/data/file001.dat",
            "  ",
            "# inline comment",
            "https://storage.example.org/data/file002.dat",
        ], str(p))
        pfns = load(str(p))
        assert len(pfns) == 2

    def test_load_single_pfn(self, tmp_path):
        p = tmp_path / "sources.txt"
        write_pfns(["https://storage.example.org/data/only.dat"], str(p))
        pfns = load(str(p))
        assert pfns == ["https://storage.example.org/data/only.dat"]

    def test_load_empty_file_raises(self, tmp_path):
        p = tmp_path / "empty.txt"
        p.write_text("")
        with pytest.raises(InventoryError, match="no entries"):
            load(str(p))

    def test_load_only_comments_raises(self, tmp_path):
        p = tmp_path / "comments.txt"
        p.write_text("# just a comment\n# another\n")
        with pytest.raises(InventoryError, match="no entries"):
            load(str(p))

    def test_load_with_duplicate_raises(self, tmp_path):
        p = tmp_path / "sources.txt"
        write_pfns([
            "https://storage.example.org/data/file001.dat",
            "https://storage.example.org/data/file001.dat",
        ], str(p))
        with pytest.raises(InventoryError, match="duplicate"):
            load(str(p))

    def test_load_file_not_found_raises(self):
        with pytest.raises(InventoryError, match="Cannot read"):
            load("/nonexistent/sources.txt")

    def test_load_preserves_order(self, tmp_path):
        pfn_list = [
            "https://storage.example.org/data/zzz.dat",
            "https://storage.example.org/data/aaa.dat",
            "https://storage.example.org/data/mmm.dat",
        ]
        p = tmp_path / "sources.txt"
        write_pfns(pfn_list, str(p))
        result = load(str(p))
        assert result == pfn_list  # order is preserved; sorting is done by planner
