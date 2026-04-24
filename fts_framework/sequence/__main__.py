"""
fts_framework.sequence.__main__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CLI entry point for ``fts-sequence`` / ``python -m fts_framework.sequence``.

Usage::

    fts-sequence params.yaml
    fts-sequence params.yaml --resume sequences/20260324_abc123_scale_test/
    fts-sequence --rerun-failed sequences/20260324_abc123_scale_test/
    fts-sequence params.yaml --runs-dir /data/runs --log-level DEBUG

Token resolution follows the same five-level priority as ``fts-run``; see
the README for details.
"""

import argparse
import logging
import os
import sys

from fts_framework.persistence.store import _DEFAULT_RUNS_DIR
from fts_framework.sequence import state as seq_state
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
        nargs="?",
        default=None,
        help="Path to sequence parameter YAML file (optional with --rerun-failed,"
             " which defaults to <SEQUENCE_DIR>/params.yaml)",
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--resume",
        metavar="SEQUENCE_DIR",
        default=None,
        help="Resume an interrupted sequence from its output directory",
    )
    mode.add_argument(
        "--rerun-failed",
        metavar="SEQUENCE_DIR",
        default=None,
        help="Reset all failed trials to pending and rerun them",
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

    log = logging.getLogger(__name__)

    # Resolve params file and resume_dir from the chosen mode.
    resume_dir = args.resume

    if args.rerun_failed:
        seq_dir = args.rerun_failed
        params_file = args.params or os.path.join(seq_dir, "params.yaml")
        try:
            state = seq_state.load(seq_dir)
            n_reset = seq_state.reset_failed_to_pending(seq_dir, state)
            log.info("Marked %d failed trial(s) as pending for rerun", n_reset)
            if n_reset == 0:
                log.warning("No failed trials found in %s — nothing to rerun", seq_dir)
        except Exception as exc:
            log.error("Failed to reset state in %s: %s", seq_dir, exc)
            sys.exit(1)
        resume_dir = seq_dir
    else:
        params_file = args.params
        if params_file is None:
            parser.error("params is required unless --rerun-failed is given")

    try:
        sequence_dir = run_sequence(
            params_file=params_file,
            resume_dir=resume_dir,
            runs_dir=args.runs_dir,
            token=args.token,
            fts_submit_token=args.fts_submit_token,
            source_read_token=args.source_read_token,
            dest_write_token=args.dest_write_token,
        )
        print("Sequence complete: {}".format(sequence_dir))
        sys.exit(0)
    except Exception as exc:
        log.error("Sequence failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
