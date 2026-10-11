"""
fts_framework.fts.copy_mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~
HTTP third-party-copy mode selection via URL opaque data.

gfal2's HTTP plugin reads ``?copy_mode=pull|push`` from the source or
destination URL; an explicit value overrides the server-side default and
disables gfal2's pull→push→streamed fallback, so a requested mode is used or
the transfer fails.  The framework puts the parameter on the **destination**
URL only (the framework-owned URL; source PFNs are user inventory).

FTS3 echoes the query back in ``dest_surl``.  ``split()`` recovers the clean
URL for internal use (cleanup DELETE, retry matching, reminting) and the
echoed mode for verification.
"""

ALLOWED_COPY_MODES = ("pull", "push")

_PARAM = "copy_mode="


def add(url, mode):
    # type: (str, str) -> str
    """Return *url* with ``copy_mode=<mode>`` appended to its query string."""
    sep = "&" if "?" in url else "?"
    return "{}{}{}{}".format(url, sep, _PARAM, mode)


def split(url):
    # type: (str) -> tuple
    """Return ``(url_without_copy_mode, mode_or_None)``.

    Other query parameters are preserved in their original order.
    """
    if "?" not in url:
        return url, None
    base, query = url.split("?", 1)
    mode = None
    kept = []
    for part in query.split("&"):
        if part.startswith(_PARAM):
            mode = part[len(_PARAM):]
        elif part:
            kept.append(part)
    if kept:
        return "{}?{}".format(base, "&".join(kept)), mode
    return base, mode
