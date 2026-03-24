"""
Unit tests for fts_framework.inventory.loader.
"""

import pytest

from fts_framework.inventory.loader import (
    load, _parse, _validate, _normalise_checksum,
)
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
        pfns, checksums = _parse(lines)
        assert pfns == [
            "https://a.example.org/f1.dat",
            "https://a.example.org/f2.dat",
        ]
        assert checksums == {}

    def test_skips_blank_lines(self):
        lines = ["https://a.example.org/f1.dat\n", "\n", "   \n",
                 "https://a.example.org/f2.dat\n"]
        pfns, _ = _parse(lines)
        assert len(pfns) == 2

    def test_skips_comment_lines(self):
        lines = ["# this is a comment\n", "https://a.example.org/f1.dat\n"]
        pfns, _ = _parse(lines)
        assert len(pfns) == 1
        assert pfns[0] == "https://a.example.org/f1.dat"

    def test_strips_leading_trailing_whitespace(self):
        lines = ["  https://a.example.org/f1.dat  \n"]
        pfns, _ = _parse(lines)
        assert pfns == ["https://a.example.org/f1.dat"]

    def test_empty_input_returns_empty_list(self):
        pfns, checksums = _parse([])
        assert pfns == []
        assert checksums == {}

    def test_all_comments_returns_empty(self):
        pfns, checksums = _parse(["# comment\n", "# another\n"])
        assert pfns == []
        assert checksums == {}

    def test_url_checksum_hex_parsed(self):
        lines = ["https://a.example.org/f1.dat,a1b2c3d4\n"]
        pfns, checksums = _parse(lines)
        assert pfns == ["https://a.example.org/f1.dat"]
        assert checksums == {"https://a.example.org/f1.dat": "adler32:a1b2c3d4"}

    def test_url_checksum_canonical_parsed(self):
        lines = ["https://a.example.org/f1.dat,adler32:a1b2c3d4\n"]
        pfns, checksums = _parse(lines)
        assert checksums == {"https://a.example.org/f1.dat": "adler32:a1b2c3d4"}

    def test_mixed_lines_partial_checksums(self):
        lines = [
            "https://a.example.org/f1.dat,adler32:a1b2c3d4\n",
            "https://a.example.org/f2.dat\n",
        ]
        pfns, checksums = _parse(lines)
        assert len(pfns) == 2
        assert len(checksums) == 1
        assert "https://a.example.org/f1.dat" in checksums
        assert "https://a.example.org/f2.dat" not in checksums

    def test_checksum_uppercase_normalised_to_lowercase(self):
        lines = ["https://a.example.org/f1.dat,A1B2C3D4\n"]
        _, checksums = _parse(lines)
        assert checksums["https://a.example.org/f1.dat"] == "adler32:a1b2c3d4"


class TestNormaliseChecksum:
    PFN = "https://a.example.org/f.dat"

    def test_bare_hex_accepted(self):
        assert _normalise_checksum(self.PFN, "a1b2c3d4") == "adler32:a1b2c3d4"

    def test_canonical_form_accepted(self):
        assert _normalise_checksum(self.PFN, "adler32:a1b2c3d4") == "adler32:a1b2c3d4"

    def test_canonical_uppercase_prefix_accepted(self):
        assert _normalise_checksum(self.PFN, "ADLER32:a1b2c3d4") == "adler32:a1b2c3d4"

    def test_result_always_lowercase(self):
        assert _normalise_checksum(self.PFN, "A1B2C3D4") == "adler32:a1b2c3d4"

    def test_too_short_raises(self):
        with pytest.raises(InventoryError, match="Invalid checksum"):
            _normalise_checksum(self.PFN, "a1b2c3")

    def test_too_long_raises(self):
        with pytest.raises(InventoryError, match="Invalid checksum"):
            _normalise_checksum(self.PFN, "a1b2c3d4e5")

    def test_non_hex_raises(self):
        with pytest.raises(InventoryError, match="Invalid checksum"):
            _normalise_checksum(self.PFN, "g1b2c3d4")


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
        pfns, checksums = load(str(p))
        assert len(pfns) == 2
        assert pfns[0] == "https://storage.example.org/data/file001.dat"
        assert checksums == {}

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
        pfns, _ = load(str(p))
        assert len(pfns) == 2

    def test_load_single_pfn(self, tmp_path):
        p = tmp_path / "sources.txt"
        write_pfns(["https://storage.example.org/data/only.dat"], str(p))
        pfns, _ = load(str(p))
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
        pfns, _ = load(str(p))
        assert pfns == pfn_list  # order is preserved; sorting is done by planner

    def test_load_with_checksums(self, tmp_path):
        p = tmp_path / "sources.txt"
        write_pfns([
            "https://storage.example.org/data/file001.dat,adler32:a1b2c3d4",
            "https://storage.example.org/data/file002.dat,deadbeef",
        ], str(p))
        pfns, checksums = load(str(p))
        assert len(pfns) == 2
        assert checksums["https://storage.example.org/data/file001.dat"] == "adler32:a1b2c3d4"
        assert checksums["https://storage.example.org/data/file002.dat"] == "adler32:deadbeef"

    def test_load_invalid_checksum_raises(self, tmp_path):
        p = tmp_path / "sources.txt"
        write_pfns(["https://storage.example.org/data/file001.dat,notahex!"], str(p))
        with pytest.raises(InventoryError, match="Invalid checksum"):
            load(str(p))
