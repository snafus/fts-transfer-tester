"""
fts_framework.destination.planner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deterministic destination URL mapping for transfer campaigns.

Maps each source PFN to a destination URL of the form::

    {dst_prefix}/{test_label}/testfile_{N:06d}[.ext]

where *N* is the zero-based index of the PFN in the alphabetically sorted
inventory.  The sort guarantees the same mapping is produced on every run from
the same input set, regardless of the order PFNs appear in the inventory file.

The mapping is an ``OrderedDict`` keyed by source PFN, preserving the sorted
iteration order.  It must be persisted to the run manifest before any network
activity begins; resuming a run uses the persisted mapping verbatim.

Usage::

    from fts_framework.destination.planner import plan
    mapping = plan(pfns, config)
"""

import logging
import os

from fts_framework.exceptions import ConfigError

logger = logging.getLogger(__name__)


def plan(pfns, config):
    # type: (list, dict) -> list
    """Compute and return the source→destination mapping for *pfns*.

    PFNs are sorted alphabetically to produce a stable, deterministic index.
    If ``transfer.destinations`` is set, files are distributed across
    destinations proportionally by weight.  Otherwise ``transfer.dst_prefix``
    is used for all files.

    Duplicate PFNs in *pfns* are fully supported; each occurrence receives its
    own destination URL with a unique index.

    Args:
        pfns (list[str]): Source PFN list as returned by ``inventory.loader``.
            May contain duplicates (e.g. when sampling is used to pad to
            ``max_files``).
        config (dict): Validated framework config dict.

    Returns:
        list[tuple[str, str]]: List of ``(source_pfn, destination_url)`` pairs
            in sorted-PFN order.  A list (not dict) so that duplicate source
            PFNs are preserved.

    Raises:
        ConfigError: If ``pfns`` is empty.
    """
    if not pfns:
        raise ConfigError("Cannot plan destinations: PFN list is empty.")

    destinations = config["transfer"].get("destinations")
    if destinations:
        return _plan_multi_destination(pfns, destinations, config)
    return _plan_single_destination(pfns, config)


def _plan_single_destination(pfns, config):
    # type: (list, dict) -> list
    dst_prefix   = config["transfer"]["dst_prefix"].rstrip("/")
    test_label   = config["run"]["test_label"]
    preserve_ext = config["transfer"]["preserve_extension"]

    sorted_pfns = sorted(pfns)
    mapping = []
    for idx, pfn in enumerate(sorted_pfns):
        ext  = _extract_extension(pfn) if preserve_ext else ""
        dest = "{}/{}/testfile_{:06d}{}".format(dst_prefix, test_label, idx, ext)
        mapping.append((pfn, dest))

    logger.info(
        "Destination mapping: %d PFNs → %s/%s/testfile_NNNNNN",
        len(mapping), dst_prefix, test_label,
    )
    return mapping


def _plan_multi_destination(pfns, destinations, config):
    # type: (list, list, dict) -> list
    """Distribute *pfns* across *destinations* proportionally by weight.

    Files are partitioned into contiguous groups, one per destination, in
    the order the destinations appear in the config.  Within each group the
    per-destination file index restarts from zero so naming is compact.

    Weight example: weights [5, 3, 2] with 100 files → 50, 30, 20 files.
    Remainder files (from integer rounding) are assigned one each to the
    destinations with the largest fractional parts, in config order.
    """
    test_label   = config["run"]["test_label"]
    preserve_ext = config["transfer"]["preserve_extension"]

    sorted_pfns = sorted(pfns)
    n           = len(sorted_pfns)
    total_w     = sum(d["weight"] for d in destinations)

    # Compute exact float allocation then floor; distribute remainder by largest fraction
    raw      = [n * d["weight"] / float(total_w) for d in destinations]
    counts   = [int(r) for r in raw]
    leftover = n - sum(counts)

    # Give remainder slots to destinations with the largest fractional part
    fractions = [(raw[i] - counts[i], i) for i in range(len(destinations))]
    fractions.sort(key=lambda x: (-x[0], x[1]))
    for k in range(leftover):
        counts[fractions[k][1]] += 1

    mapping = []
    offset  = 0
    for dest_cfg, count in zip(destinations, counts):
        prefix = dest_cfg["prefix"].rstrip("/")
        for local_idx, pfn in enumerate(sorted_pfns[offset:offset + count]):
            ext  = _extract_extension(pfn) if preserve_ext else ""
            dest = "{}/{}/testfile_{:06d}{}".format(
                prefix, test_label, local_idx, ext
            )
            mapping.append((pfn, dest))
        logger.info(
            "Destination mapping: %d PFNs → %s/%s/testfile_NNNNNN (weight %d)",
            count, prefix, test_label, dest_cfg["weight"],
        )
        offset += count

    return mapping


def _extract_extension(pfn):
    # type: (str) -> str
    """Return the file extension from *pfn* (including the dot), or ``""`` if none.

    Only the final path component is considered; query strings and fragments
    are ignored.  A leading dot (hidden file convention) is not treated as an
    extension.

    Examples::

        >>> _extract_extension("https://s.example.org/data/file.dat")
        '.dat'
        >>> _extract_extension("https://s.example.org/data/file")
        ''
        >>> _extract_extension("https://s.example.org/data/file.tar.gz")
        '.gz'
        >>> _extract_extension("https://s.example.org/data/.hidden")
        ''
    """
    # Strip query string then fragment (well-formed URL order: ?query before #fragment).
    path = pfn.split("?")[0].split("#")[0]
    basename = path.rsplit("/", 1)[-1]

    # A pure hidden file (e.g. ".hidden") has no extension; a hidden file that also
    # has an extension (e.g. ".hidden.dat") correctly returns ".dat" via os.path.splitext.
    # This is intentionally more correct than the DESIGN.md §7 pseudocode, which uses
    # rsplit(".", 1) and would return ".hidden.dat" for that case.
    if basename.startswith(".") and "." not in basename[1:]:
        return ""

    # Guard: if there is no path separator after the authority (scheme://host),
    # basename contains the hostname and its dots are not extension separators.
    # We strip the scheme prefix before the slash check so that the "//" in
    # "https://" does not falsely satisfy the test.
    after_authority = path.split("://", 1)[-1] if "://" in path else path
    if "/" not in after_authority:
        return ""

    # Extension case is preserved (e.g. ".DAT" stays ".DAT"); no normalisation
    # is applied since destination storage may be case-sensitive.
    _, ext = os.path.splitext(basename)
    return ext
