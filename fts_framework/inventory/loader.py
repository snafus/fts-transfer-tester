"""
fts_framework.inventory.loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PFN inventory file loader.

Reads a plain-text file of source Physical File Names (PFNs), one per line.
Empty lines and lines beginning with ``#`` are ignored.  Duplicate PFNs are
rejected to prevent accidental double-submission.

Expected file format::

    # Optional comment
    https://storage.example.org/data/file001.dat
    https://storage.example.org/data/file002.dat
    ...

Usage::

    from fts_framework.inventory.loader import load
    pfns = load("/path/to/sources.txt")
"""

import logging

from fts_framework.exceptions import InventoryError

logger = logging.getLogger(__name__)


def load(path):
    # type: (str) -> list
    """Load and return the list of source PFNs from *path*.

    Lines are stripped of leading/trailing whitespace.  Blank lines and lines
    starting with ``#`` are silently skipped.  All remaining lines are treated
    as PFNs.

    Args:
        path (str): Path to the PFN inventory file.

    Returns:
        list[str]: Non-empty list of unique PFN strings, in file order.

    Raises:
        InventoryError: If the file cannot be read, contains no PFNs after
            filtering, or contains duplicate entries.
    """
    logger.info("Loading PFN inventory from: %s", path)

    lines = _read_lines(path)
    pfns = _parse(lines)
    _validate(pfns, path)

    logger.info("Loaded %d PFNs from %s", len(pfns), path)
    return pfns


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_lines(path):
    # type: (str) -> list
    """Return raw lines from *path*.

    Raises:
        InventoryError: On any I/O error.
    """
    try:
        with open(path, "r") as fh:
            return fh.readlines()
    except IOError as exc:
        raise InventoryError(
            "Cannot read PFN inventory file {!r}: {}".format(path, exc)
        )


def _parse(lines):
    # type: (list) -> list
    """Strip, filter blank lines and comments, return remaining entries."""
    pfns = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        pfns.append(stripped)
    return pfns


def _validate(pfns, path):
    # type: (list, str) -> None
    """Validate that the PFN list is non-empty and contains no duplicates.

    Raises:
        InventoryError: If the list is empty or duplicates are found.
    """
    if not pfns:
        raise InventoryError(
            "PFN inventory file {!r} contains no entries after filtering "
            "blank lines and comments.".format(path)
        )

    seen = set()           # type: set
    duplicated = set()     # type: set  — distinct PFNs that appear more than once
    for pfn in pfns:
        if pfn in seen:
            duplicated.add(pfn)
        seen.add(pfn)

    if duplicated:
        # Report up to 5 distinct duplicated PFNs to keep the message readable
        examples = sorted(duplicated)[:5]
        raise InventoryError(
            "PFN inventory file {!r} contains {} duplicated PFN(s). "
            "First examples: {}".format(path, len(duplicated), examples)
        )
