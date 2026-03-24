"""
fts_framework.inventory.loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PFN inventory file loader.

Reads a plain-text file of source Physical File Names (PFNs), one per line.
Empty lines and lines beginning with ``#`` are ignored.  Duplicate PFNs are
rejected to prevent accidental double-submission.

Two line formats are accepted and may be mixed within the same file::

    # URL-only
    https://storage.example.org/data/file001.dat

    # URL with pre-supplied ADLER32 checksum (8-char hex or adler32:<hex>)
    https://storage.example.org/data/file002.dat,adler32:a1b2c3d4
    https://storage.example.org/data/file003.dat,a1b2c3d4

When checksums are present the runner skips the Want-Digest HEAD fetch and
uses them directly in the submission payload.

Usage::

    from fts_framework.inventory.loader import load
    pfns, checksums = load("/path/to/sources.txt")
"""

import logging

from fts_framework.exceptions import InventoryError

logger = logging.getLogger(__name__)


def load(path):
    # type: (str) -> tuple
    """Load source PFNs and any pre-supplied checksums from *path*.

    Lines are stripped of leading/trailing whitespace.  Blank lines and lines
    starting with ``#`` are silently skipped.  Each remaining line is either a
    bare PFN or a ``pfn,checksum`` pair.

    Args:
        path (str): Path to the PFN inventory file.

    Returns:
        tuple: ``(pfns, checksums)`` where *pfns* is a non-empty list of PFN
            strings in file order and *checksums* is a dict mapping PFN to
            ``"adler32:<8-hex>"`` for lines that supplied a checksum.  The
            dict is empty when no checksums are present in the file.

    Raises:
        InventoryError: If the file cannot be read, contains no PFNs after
            filtering, contains duplicate entries, or contains a malformed
            checksum value.
    """
    logger.info("Loading PFN inventory from: %s", path)

    lines = _read_lines(path)
    pfns, checksums = _parse(lines)
    _validate(pfns, path)

    if checksums:
        logger.info(
            "Loaded %d PFNs from %s (%d with pre-supplied checksums)",
            len(pfns), path, len(checksums),
        )
    else:
        logger.info("Loaded %d PFNs from %s", len(pfns), path)
    return pfns, checksums


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
    # type: (list) -> tuple
    """Strip, filter blank lines and comments, parse pfn and optional checksum.

    Returns:
        tuple: ``(pfns, checksums)`` — list of PFN strings and dict of
            PFN → ``"adler32:<hex>"`` for lines that included a checksum.
    """
    pfns = []
    checksums = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        pfn, checksum = _parse_line(stripped)
        pfns.append(pfn)
        if checksum is not None:
            checksums[pfn] = checksum
    return pfns, checksums


def _parse_line(stripped):
    # type: (str) -> tuple
    """Return ``(pfn, checksum_or_None)`` for a single non-blank, non-comment line."""
    if "," in stripped:
        pfn, raw = stripped.split(",", 1)
        return pfn.strip(), _normalise_checksum(pfn.strip(), raw.strip())
    return stripped, None


def _normalise_checksum(pfn, raw):
    # type: (str, str) -> str
    """Normalise a checksum value from an inventory line to ``adler32:<8-hex>``.

    Accepts ``adler32:<8-hex>`` (canonical) or a bare ``<8-hex>`` string.

    Raises:
        InventoryError: If the value cannot be parsed as a valid ADLER32 hex.
    """
    if raw.lower().startswith("adler32:"):
        hex_part = raw[8:]
    else:
        hex_part = raw
    if len(hex_part) == 8:
        try:
            int(hex_part, 16)
            return "adler32:{}".format(hex_part.lower())
        except ValueError:
            pass
    raise InventoryError(
        "Invalid checksum {!r} for PFN {!r}: "
        "expected adler32:<8-hex> or bare 8-character hex string".format(raw, pfn)
    )


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
    duplicated = set()     # type: set
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
