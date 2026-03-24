"""
fts_framework.sequence.__main__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CLI entry point for ``fts-sequence`` / ``python -m fts_framework.sequence``.

Usage::

    fts-sequence params.yaml
    fts-sequence params.yaml --resume sequences/20260324_abc123_scale_test/
    fts-sequence params.yaml --runs-dir /data/runs --log-level DEBUG

Token resolution follows the same five-level priority as ``fts-run``; see
the README for details.
"""

import argparse
import logging
import sys

from fts_framework.persistence.store import _DEFAULT_RUNS_DIR
from fts_framework.sequence.runner import run_sequence


def main():
    # type: () -> None
    """CLI entry point for the ``fts-sequence`` console script."""
    parser = argparse.ArgumentParser(
        prog="fts-sequence",
        description="FTS3 test framework — parameter-sweep sequence runner",
    )
    parser.add_argument(
        "params",
        help="Path to sequence parameter YAML file",
    )
    parser.add_argument(
        "--resume",
        metavar="SEQUENCE_DIR",
        default=None,
        help="Resume an interrupted sequence from its output directory",
    )
    parser.add_argument(
        "--runs-dir",
        default=_DEFAULT_RUNS_DIR,
        metavar="DIR",
        help="Base directory for individual run outputs (default: {})".format(
            _DEFAULT_RUNS_DIR
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    parser.add_argument(
        "--token",
        default=None,
        metavar="TOKEN",
        help="Bearer token for all roles",
    )
    parser.add_argument(
        "--fts-submit-token",
        default=None,
        metavar="TOKEN",
        help="Bearer token for FTS3 job submission",
    )
    parser.add_argument(
        "--source-read-token",
        default=None,
        metavar="TOKEN",
        help="Bearer token for source storage reads",
    )
    parser.add_argument(
        "--dest-write-token",
        default=None,
        metavar="TOKEN",
        help="Bearer token for destination storage writes",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        sequence_dir = run_sequence(
            params_file=args.params,
            resume_dir=args.resume,
            runs_dir=args.runs_dir,
            token=args.token,
            fts_submit_token=args.fts_submit_token,
            source_read_token=args.source_read_token,
            dest_write_token=args.dest_write_token,
        )
        print("Sequence complete: {}".format(sequence_dir))
        sys.exit(0)
    except Exception as exc:
        logging.getLogger(__name__).error(
            "Sequence failed: %s", exc, exc_info=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
