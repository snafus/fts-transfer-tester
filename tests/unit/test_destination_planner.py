"""
Unit tests for fts_framework.destination.planner.
"""

import pytest

from fts_framework.destination.planner import plan, _extract_extension
from fts_framework.exceptions import ConfigError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(dst_prefix="https://storage.example.org/data",
            test_label="campaign_test",
            preserve_extension=False):
    return {
        "run": {"test_label": test_label},
        "transfer": {
            "dst_prefix": dst_prefix,
            "preserve_extension": preserve_extension,
        },
    }


# ---------------------------------------------------------------------------
# _extract_extension
# ---------------------------------------------------------------------------

class TestExtractExtension:
    def test_simple_extension(self):
        assert _extract_extension("https://s.example.org/data/file.dat") == ".dat"

    def test_no_extension(self):
        assert _extract_extension("https://s.example.org/data/file") == ""

    def test_compound_extension_returns_last(self):
        assert _extract_extension("https://s.example.org/data/file.tar.gz") == ".gz"

    def test_hidden_file_no_extension(self):
        assert _extract_extension("https://s.example.org/data/.hidden") == ""

    def test_query_string_ignored(self):
        assert _extract_extension("https://s.example.org/data/file.dat?token=abc") == ".dat"

    def test_fragment_ignored(self):
        assert _extract_extension("https://s.example.org/data/file.dat#section") == ".dat"

    def test_fits_extension(self):
        assert _extract_extension("https://s.example.org/data/image.fits") == ".fits"

    def test_uppercase_extension_preserved(self):
        # Extension case is preserved; no normalisation — storage may be case-sensitive
        assert _extract_extension("https://s.example.org/data/FILE.DAT") == ".DAT"

    def test_hidden_file_with_extension(self):
        # .hidden.dat — the leading dot is not treated as an extension separator;
        # os.path.splitext correctly returns ".dat"
        assert _extract_extension("https://s.example.org/data/.hidden.dat") == ".dat"

    def test_bare_hostname_no_extension(self):
        # No path component — hostname dots must not be treated as extensions
        assert _extract_extension("https://storage.example.org") == ""


# ---------------------------------------------------------------------------
# plan — core mapping
# ---------------------------------------------------------------------------

class TestPlan:
    def test_single_pfn_produces_correct_destination(self):
        pfns = ["https://src.example.org/data/file001.dat"]
        mapping = plan(pfns, _config())
        assert list(mapping.values())[0] == (
            "https://storage.example.org/data/campaign_test/testfile_000000"
        )

    def test_index_is_zero_padded_to_six_digits(self):
        pfns = ["https://src.example.org/data/f{:04d}.dat".format(i) for i in range(10)]
        mapping = plan(pfns, _config())
        for i, dest in enumerate(mapping.values()):
            assert dest.endswith("testfile_{:06d}".format(i))

    def test_destination_uses_dst_prefix_and_test_label(self):
        pfns = ["https://src.example.org/file.dat"]
        mapping = plan(pfns, _config(
            dst_prefix="https://dst.example.org/bucket",
            test_label="my_run",
        ))
        dest = list(mapping.values())[0]
        assert dest.startswith("https://dst.example.org/bucket/my_run/")

    def test_trailing_slash_on_prefix_is_removed(self):
        pfns = ["https://src.example.org/file.dat"]
        mapping = plan(pfns, _config(dst_prefix="https://storage.example.org/data/"))
        dest = list(mapping.values())[0]
        assert "data//campaign_test" not in dest
        assert dest.startswith("https://storage.example.org/data/campaign_test/")

    def test_mapping_is_ordered_dict(self):
        from collections import OrderedDict
        pfns = ["https://src.example.org/b.dat", "https://src.example.org/a.dat"]
        mapping = plan(pfns, _config())
        assert isinstance(mapping, OrderedDict)

    def test_empty_pfns_raises(self):
        with pytest.raises(ConfigError, match="empty"):
            plan([], _config())

    def test_returns_all_pfns(self):
        pfns = ["https://src.example.org/f{}.dat".format(i) for i in range(5)]
        mapping = plan(pfns, _config())
        assert len(mapping) == 5


# ---------------------------------------------------------------------------
# plan — deterministic sort
# ---------------------------------------------------------------------------

class TestDeterministicSort:
    def test_sorted_alphabetically(self):
        pfns = [
            "https://src.example.org/zzz.dat",
            "https://src.example.org/aaa.dat",
            "https://src.example.org/mmm.dat",
        ]
        mapping = plan(pfns, _config())
        sources = list(mapping.keys())
        assert sources == sorted(pfns)

    def test_index_assigned_to_sorted_position(self):
        pfns = [
            "https://src.example.org/c.dat",
            "https://src.example.org/a.dat",
            "https://src.example.org/b.dat",
        ]
        mapping = plan(pfns, _config())
        # "a" should get index 0, "b" → 1, "c" → 2
        assert mapping["https://src.example.org/a.dat"].endswith("testfile_000000")
        assert mapping["https://src.example.org/b.dat"].endswith("testfile_000001")
        assert mapping["https://src.example.org/c.dat"].endswith("testfile_000002")

    def test_same_input_same_mapping_regardless_of_order(self):
        pfns_a = ["https://src.example.org/f2.dat", "https://src.example.org/f1.dat"]
        pfns_b = ["https://src.example.org/f1.dat", "https://src.example.org/f2.dat"]
        assert plan(pfns_a, _config()) == plan(pfns_b, _config())


# ---------------------------------------------------------------------------
# plan — extension handling
# ---------------------------------------------------------------------------

class TestExtensionHandling:
    def test_no_extension_by_default(self):
        pfns = ["https://src.example.org/file.dat"]
        mapping = plan(pfns, _config(preserve_extension=False))
        dest = list(mapping.values())[0]
        assert not dest.endswith(".dat")
        assert dest.endswith("testfile_000000")

    def test_preserve_extension_adds_suffix(self):
        pfns = ["https://src.example.org/file.dat"]
        mapping = plan(pfns, _config(preserve_extension=True))
        dest = list(mapping.values())[0]
        assert dest.endswith("testfile_000000.dat")

    def test_preserve_extension_no_extension_file(self):
        pfns = ["https://src.example.org/noext"]
        mapping = plan(pfns, _config(preserve_extension=True))
        dest = list(mapping.values())[0]
        assert dest.endswith("testfile_000000")

    def test_preserve_extension_compound(self):
        pfns = ["https://src.example.org/archive.tar.gz"]
        mapping = plan(pfns, _config(preserve_extension=True))
        dest = list(mapping.values())[0]
        assert dest.endswith("testfile_000000.gz")

    def test_extension_consistent_across_mixed_files(self):
        pfns = [
            "https://src.example.org/a.fits",
            "https://src.example.org/b.dat",
        ]
        mapping = plan(pfns, _config(preserve_extension=True))
        destinations = list(mapping.values())
        # a.fits → index 0, b.dat → index 1 (alphabetical)
        assert destinations[0].endswith("testfile_000000.fits")
        assert destinations[1].endswith("testfile_000001.dat")
