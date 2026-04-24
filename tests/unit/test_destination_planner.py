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


def _dsts(mapping):
    return [dst for src, dst in mapping]


def _srcs(mapping):
    return [src for src, dst in mapping]


def _by_src(mapping):
    return {src: dst for src, dst in mapping}


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
        assert _dsts(mapping)[0] == (
            "https://storage.example.org/data/campaign_test/testfile_000000"
        )

    def test_index_is_zero_padded_to_six_digits(self):
        pfns = ["https://src.example.org/data/f{:04d}.dat".format(i) for i in range(10)]
        mapping = plan(pfns, _config())
        for i, dst in enumerate(_dsts(mapping)):
            assert dst.endswith("testfile_{:06d}".format(i))

    def test_destination_uses_dst_prefix_and_test_label(self):
        pfns = ["https://src.example.org/file.dat"]
        mapping = plan(pfns, _config(
            dst_prefix="https://dst.example.org/bucket",
            test_label="my_run",
        ))
        dest = _dsts(mapping)[0]
        assert dest.startswith("https://dst.example.org/bucket/my_run/")

    def test_trailing_slash_on_prefix_is_removed(self):
        pfns = ["https://src.example.org/file.dat"]
        mapping = plan(pfns, _config(dst_prefix="https://storage.example.org/data/"))
        dest = _dsts(mapping)[0]
        assert "data//campaign_test" not in dest
        assert dest.startswith("https://storage.example.org/data/campaign_test/")

    def test_mapping_is_list(self):
        pfns = ["https://src.example.org/b.dat", "https://src.example.org/a.dat"]
        mapping = plan(pfns, _config())
        assert isinstance(mapping, list)

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
        sources = _srcs(mapping)
        assert sources == sorted(pfns)

    def test_index_assigned_to_sorted_position(self):
        pfns = [
            "https://src.example.org/c.dat",
            "https://src.example.org/a.dat",
            "https://src.example.org/b.dat",
        ]
        mapping = plan(pfns, _config())
        by_src = _by_src(mapping)
        assert by_src["https://src.example.org/a.dat"].endswith("testfile_000000")
        assert by_src["https://src.example.org/b.dat"].endswith("testfile_000001")
        assert by_src["https://src.example.org/c.dat"].endswith("testfile_000002")

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
        dest = _dsts(mapping)[0]
        assert not dest.endswith(".dat")
        assert dest.endswith("testfile_000000")

    def test_preserve_extension_adds_suffix(self):
        pfns = ["https://src.example.org/file.dat"]
        mapping = plan(pfns, _config(preserve_extension=True))
        dest = _dsts(mapping)[0]
        assert dest.endswith("testfile_000000.dat")

    def test_preserve_extension_no_extension_file(self):
        pfns = ["https://src.example.org/noext"]
        mapping = plan(pfns, _config(preserve_extension=True))
        dest = _dsts(mapping)[0]
        assert dest.endswith("testfile_000000")

    def test_preserve_extension_compound(self):
        pfns = ["https://src.example.org/archive.tar.gz"]
        mapping = plan(pfns, _config(preserve_extension=True))
        dest = _dsts(mapping)[0]
        assert dest.endswith("testfile_000000.gz")

    def test_extension_consistent_across_mixed_files(self):
        pfns = [
            "https://src.example.org/a.fits",
            "https://src.example.org/b.dat",
        ]
        mapping = plan(pfns, _config(preserve_extension=True))
        destinations = _dsts(mapping)
        # a.fits → index 0, b.dat → index 1 (alphabetical)
        assert destinations[0].endswith("testfile_000000.fits")
        assert destinations[1].endswith("testfile_000001.dat")


# ---------------------------------------------------------------------------
# plan — duplicate PFN / sampling support
# ---------------------------------------------------------------------------

class TestDuplicatePfnSupport:
    def test_duplicate_pfns_each_get_own_destination(self):
        pfn = "https://src.example.org/file.dat"
        mapping = plan([pfn, pfn, pfn], _config())
        assert len(mapping) == 3
        dsts = _dsts(mapping)
        assert len(set(dsts)) == 3  # all destinations distinct

    def test_duplicate_pfns_destinations_sequential(self):
        pfn = "https://src.example.org/file.dat"
        mapping = plan([pfn, pfn], _config())
        dsts = _dsts(mapping)
        assert dsts[0].endswith("testfile_000000")
        assert dsts[1].endswith("testfile_000001")


# ---------------------------------------------------------------------------
# Multi-destination weighted distribution
# ---------------------------------------------------------------------------

def _multi_config(destinations, test_label="campaign_test", preserve_extension=False):
    return {
        "run": {"test_label": test_label},
        "transfer": {
            "destinations": destinations,
            "preserve_extension": preserve_extension,
        },
    }


def _dest_specs(*weights):
    return [
        {"prefix": "https://site-{}.example.org/data".format(i), "weight": w}
        for i, w in enumerate(weights)
    ]


class TestMultiDestination:
    def test_all_pfns_mapped(self):
        pfns = ["https://src.example.org/f{:03d}".format(i) for i in range(100)]
        mapping = plan(pfns, _multi_config(_dest_specs(5, 3, 2)))
        assert len(mapping) == 100

    def test_proportional_distribution(self):
        pfns = ["https://src.example.org/f{:03d}".format(i) for i in range(100)]
        mapping = plan(pfns, _multi_config(_dest_specs(5, 3, 2)))
        counts = {}
        for src, dst in mapping:
            host = dst.split("/")[2]
            counts[host] = counts.get(host, 0) + 1
        assert counts["site-0.example.org"] == 50
        assert counts["site-1.example.org"] == 30
        assert counts["site-2.example.org"] == 20

    def test_remainder_distributed(self):
        # 10 files, weights [1,1,1] → total must be 10
        pfns = ["https://src.example.org/f{:02d}".format(i) for i in range(10)]
        mapping = plan(pfns, _multi_config(_dest_specs(1, 1, 1)))
        assert len(mapping) == 10
        counts = {}
        for src, dst in mapping:
            host = dst.split("/")[2]
            counts[host] = counts.get(host, 0) + 1
        assert sum(counts.values()) == 10

    def test_per_destination_index_restarts(self):
        pfns = ["https://src.example.org/f{:03d}".format(i) for i in range(4)]
        # weights [1,1] → 2 files each
        mapping = plan(pfns, _multi_config(_dest_specs(1, 1)))
        dsts = _dsts(mapping)
        site0 = [d for d in dsts if "site-0" in d]
        site1 = [d for d in dsts if "site-1" in d]
        assert any("testfile_000000" in d for d in site0)
        assert any("testfile_000001" in d for d in site0)
        assert any("testfile_000000" in d for d in site1)
        assert any("testfile_000001" in d for d in site1)

    def test_single_destination_in_list(self):
        pfns = ["https://src.example.org/f{:03d}".format(i) for i in range(10)]
        mapping = plan(pfns, _multi_config(_dest_specs(1)))
        assert len(mapping) == 10
        assert all("site-0" in dst for src, dst in mapping)

    def test_equal_weights_equal_distribution(self):
        pfns = ["https://src.example.org/f{:03d}".format(i) for i in range(60)]
        mapping = plan(pfns, _multi_config(_dest_specs(2, 2, 2)))
        counts = {}
        for src, dst in mapping:
            host = dst.split("/")[2]
            counts[host] = counts.get(host, 0) + 1
        assert all(c == 20 for c in counts.values())

    def test_uses_destinations_prefix_not_dst_prefix(self):
        pfns = ["https://src.example.org/f001"]
        config = {
            "run": {"test_label": "t"},
            "transfer": {
                "destinations": [{"prefix": "https://chosen.example.org/data", "weight": 1}],
                "dst_prefix": "https://ignored.example.org/data",
                "preserve_extension": False,
            },
        }
        mapping = plan(pfns, config)
        assert "chosen.example.org" in _dsts(mapping)[0]
        assert "ignored.example.org" not in _dsts(mapping)[0]

    def test_deterministic_across_calls(self):
        pfns = ["https://src.example.org/f{:03d}".format(i) for i in range(30)]
        cfg = _multi_config(_dest_specs(2, 1))
        m1 = plan(pfns, cfg)
        m2 = plan(pfns, cfg)
        assert m1 == m2

    def test_preserve_extension_multi_destination(self):
        pfns = ["https://src.example.org/file.dat"]
        mapping = plan(pfns, _multi_config(_dest_specs(1), preserve_extension=True))
        assert _dsts(mapping)[0].endswith(".dat")


class TestMultiDestinationValidation:
    def _base(self, tmp_path):
        import yaml, os
        data = {
            "run":   {"test_label": "t"},
            "fts":   {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
            "tokens": {"fts_submit": "t", "source_read": "t", "dest_write": "t"},
            "transfer": {"source_pfns_file": "s.txt"},
        }
        path = str(tmp_path / "config.yaml")
        with open(path, "w") as fh:
            yaml.dump(data, fh)
        return path, data

    def test_valid_destinations_accepted(self, tmp_path):
        from fts_framework.config.loader import load
        import yaml
        path, data = self._base(tmp_path)
        data["transfer"]["destinations"] = [
            {"prefix": "https://site-a.example.org/data", "weight": 2},
            {"prefix": "https://site-b.example.org/data", "weight": 1},
        ]
        with open(path, "w") as fh:
            yaml.dump(data, fh)
        config = load(path)
        assert len(config["transfer"]["destinations"]) == 2

    def test_missing_prefix_raises(self, tmp_path):
        from fts_framework.config.loader import load
        from fts_framework.exceptions import ConfigError
        import yaml
        path, data = self._base(tmp_path)
        data["transfer"]["destinations"] = [{"prefix": "http://bad.example.org", "weight": 1}]
        with open(path, "w") as fh:
            yaml.dump(data, fh)
        with pytest.raises(ConfigError, match="prefix"):
            load(path)

    def test_zero_weight_raises(self, tmp_path):
        from fts_framework.config.loader import load
        from fts_framework.exceptions import ConfigError
        import yaml
        path, data = self._base(tmp_path)
        data["transfer"]["destinations"] = [
            {"prefix": "https://site-a.example.org/data", "weight": 0}
        ]
        with open(path, "w") as fh:
            yaml.dump(data, fh)
        with pytest.raises(ConfigError, match="weight"):
            load(path)

    def test_empty_list_raises(self, tmp_path):
        from fts_framework.config.loader import load
        from fts_framework.exceptions import ConfigError
        import yaml
        path, data = self._base(tmp_path)
        data["transfer"]["destinations"] = []
        with open(path, "w") as fh:
            yaml.dump(data, fh)
        with pytest.raises(ConfigError, match="non-empty"):
            load(path)

    def test_neither_dst_prefix_nor_destinations_raises(self, tmp_path):
        from fts_framework.config.loader import load
        from fts_framework.exceptions import ConfigError
        import yaml
        path, data = self._base(tmp_path)
        # No dst_prefix, no destinations
        with open(path, "w") as fh:
            yaml.dump(data, fh)
        with pytest.raises(ConfigError, match="dst_prefix"):
            load(path)
