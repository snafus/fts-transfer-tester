#!/usr/bin/env python3
"""
Remove storage tokens from payloads persisted by earlier framework versions.

Versions up to and including 0.2.0 wrote each job's POST body to
``runs/<run_id>/submitted_payloads/`` with the per-file ``source_tokens`` and
``destination_tokens`` intact.  This rewrites those files in place with every
token replaced by ``"<REDACTED>"``, exactly as current versions write them.
Nothing else in a payload is changed, and files that are already clean are
left untouched.

Run it only after upgrading: an older framework version resuming a scrubbed
run would repost the placeholder instead of a token.  Current versions restore
tokens from the live config on resume.

Usage:
  python tools/redact_payload_tokens.py DIR [DIR ...] [--dry-run]

Each DIR is searched recursively for ``submitted_payloads`` directories, so a
runs directory, a sequences directory or a single run all work.
"""

import argparse
import json
import os
import re
import sys

from fts_framework.persistence import store

_PAYLOAD_RE = re.compile(r"^chunk_\d+_r\d+(_a[0-9a-f]+)?\.json$")


def _payload_files(top):
    for root, _, names in os.walk(top):
        if os.path.basename(root) != "submitted_payloads":
            continue
        for name in sorted(names):
            if _PAYLOAD_RE.match(name):
                yield os.path.join(root, name)


def main():
    ap = argparse.ArgumentParser(
        description="Redact storage tokens in persisted FTS3 job payloads.",
    )
    ap.add_argument("dirs", nargs="+", metavar="DIR")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change without writing")
    args = ap.parse_args()

    scanned = redacted = failed = 0
    for top in args.dirs:
        if not os.path.isdir(top):
            sys.stderr.write("not a directory: {}\n".format(top))
            failed += 1
            continue
        for path in _payload_files(top):
            scanned += 1
            try:
                with open(path, "r") as fh:
                    payload = json.load(fh)
                clean = store.redact_payload(payload)
                if clean == payload:
                    continue
                if not args.dry_run:
                    tmp_path = path + ".tmp"
                    with open(tmp_path, "w") as fh:
                        json.dump(clean, fh, indent=2)
                    os.replace(tmp_path, path)
            except (IOError, OSError, ValueError) as exc:
                sys.stderr.write("FAILED {}: {}\n".format(path, exc))
                failed += 1
                continue
            redacted += 1
            print("{} {}".format(
                "would redact" if args.dry_run else "redacted", path))

    print("{} payload file(s) scanned, {} {}, {} failed".format(
        scanned, redacted, "to redact" if args.dry_run else "redacted", failed,
    ))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
