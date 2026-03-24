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

from collections import OrderedDict

from fts_framework.exceptions import ConfigError

logger = logging.getLogger(__name__)


def plan(pfns, config):
    # type: (list, dict) -> OrderedDict
    """Compute and return the source→destination mapping for *pfns*.

    PFNs are sorted alphabetically to produce a stable, deterministic index.
    Each source PFN maps to a destination URL following the framework naming
    convention.

    Args:
        pfns (list[str]): Source PFN list as returned by ``inventory.loader``.
        config (dict): Validated framework config dict.

    Returns:
        OrderedDict[str, str]: Mapping of source PFN → destination URL,
            iterable in sorted-PFN order.

    Raises:
        ConfigError: If ``pfns`` is empty.
    """
    if not pfns:
        raise ConfigError("Cannot plan destinations: PFN list is empty.")

    dst_prefix = config["transfer"]["dst_prefix"].rstrip("/")
    test_label = config["run"]["test_label"]
    preserve_ext = config["transfer"]["preserve_extension"]

    # Sort by Unicode code-point order (equivalent to ASCII byte order for all-ASCII
    # HTTPS PFNs).  This is deterministic across all CPython implementations.
    sorted_pfns = sorted(pfns)
    mapping = OrderedDict()

    for idx, pfn in enumerate(sorted_pfns):
        ext = _extract_extension(pfn) if preserve_ext else ""
        dest = "{}/{}/testfile_{:06d}{}".format(dst_prefix, test_label, idx, ext)
        mapping[pfn] = dest

    logger.info(
        "Destination mapping computed: %d PFNs → %s/%s/testfile_NNNNNN",
        len(mapping),
        dst_prefix,
        test_label,
    )
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
