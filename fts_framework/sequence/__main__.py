"""
fts_framework.sequence.__main__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CLI entry point for ``fts-sequence`` / ``python -m fts_framework.sequence``.

Usage::

    fts-sequence params.yaml
    fts-sequence params.yaml --resume sequences/20260324_abc123_scale_test/
    fts-sequence --rerun-failed sequences/20260324_abc123_scale_test/
    fts-sequence --cancel-jobs sequences/20260324_abc123_scale_test/
    fts-sequence params.yaml --runs-dir /data/runs --log-level DEBUG

Token resolution follows the same five-level priority as ``fts-run``; see
the README for details.
"""

import argparse
import logging
import os
import sys

from fts_framework.sequence import state as seq_state
from fts_framework.sequence import reporter as seq_reporter
from fts_framework.sequence.runner import run_sequence
from fts_framework.sequence import loader as seq_loader
from fts_framework.config import loader as cfg_loader
from fts_framework.fts import canceller as fts_canceller
from fts_framework.fts import client as fts_client_mod


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
        "--check-tokens",
        action="store_true",
        default=False,
        help="Resolve and verify tokens (including OIDC fetch) then exit",
    )
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
    mode.add_argument(
        "--cancel-jobs",
        metavar="SEQUENCE_DIR",
        default=None,
        help="Cancel all non-terminal FTS3 jobs associated with the sequence",
    )
    mode.add_argument(
        "--report",
        metavar="SEQUENCE_DIR",
        default=None,
        help="Regenerate summary reports from an existing sequence directory "
             "(safe to run mid-sequence)",
    )
    mode.add_argument(
        "--skip-from-case",
        metavar="SEQUENCE_DIR",
        default=None,
        help="Mark all pending trials in cases from --from-case-index onwards as skipped",
    )

    parser.add_argument(
        "--runs-dir",
        default=None,
        metavar="DIR",
        help="Base directory for individual run outputs "
             "(default: <sequence_dir>/runs/)",
    )
    parser.add_argument(
        "--from-case-index",
        type=int,
        default=None,
        metavar="N",
        help="First case index to skip (required with --skip-from-case)",
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

    # --check-tokens: report token sources and attempt full resolution (OIDC fetch).
    if args.check_tokens:
        params_file = args.params
        if params_file is None:
            parser.error("params is required with --check-tokens")
        try:
            seq_params = seq_loader.load(params_file)
        except Exception as exc:
            log.error("Failed to load sequence params: %s", exc)
            sys.exit(1)
        baseline = seq_params["baseline_config_path"]

        # First show planned sources (no OIDC fetch yet)
        try:
            sources = cfg_loader.identify_token_sources(
                baseline,
                token=args.token,
                fts_submit_token=args.fts_submit_token,
                source_read_token=args.source_read_token,
                dest_write_token=args.dest_write_token,
            )
        except Exception as exc:
            log.error("Cannot read baseline config %s: %s", baseline, exc)
            sys.exit(1)

        print("\nToken source plan:")
        print("  {:<16} {}".format("Role", "Source"))
        print("  {:<16} {}".format("----", "------"))
        for role in ("fts_submit", "source_read", "dest_write"):
            print("  {:<16} {}".format(role, sources[role]))
        print()

        # Now attempt actual resolution (fetches OIDC tokens if configured)
        missing = [r for r, s in sources.items() if s == "MISSING"]
        if missing:
            print("ERROR: no token source for: {}".format(", ".join(missing)))
            sys.exit(1)

        print("Fetching tokens...")
        try:
            config = cfg_loader.load(
                baseline,
                token=args.token,
                fts_submit_token=args.fts_submit_token,
                source_read_token=args.source_read_token,
                dest_write_token=args.dest_write_token,
            )
        except Exception as exc:
            print("FAILED: {}".format(exc))
            sys.exit(1)

        print("\nToken check result:")
        for role in ("fts_submit", "source_read", "dest_write"):
            val = config["tokens"].get(role) or ""
            status = "OK ({} chars)".format(len(val)) if val else "MISSING"
            print("  {:<16} {}  [{}]".format(role, status, sources[role]))
        print()
        sys.exit(0)

    # --cancel-jobs: cancel all non-terminal FTS3 jobs in a sequence directory.
    if args.cancel_jobs:
        seq_dir = args.cancel_jobs
        try:
            job_ids = fts_canceller.collect_job_ids_from_sequence(
                seq_dir, runs_dir=args.runs_dir or "runs",
            )
        except Exception as exc:
            log.error("Failed to read sequence state from %s: %s", seq_dir, exc)
            sys.exit(1)

        if not job_ids:
            print("No non-terminal jobs found in {}.".format(seq_dir))
            sys.exit(0)

        print("Found {} non-terminal job(s) to cancel.".format(len(job_ids)))

        try:
            state = seq_state.load(seq_dir)
            baseline = state.get("baseline_config")
            if not baseline:
                raise ValueError("baseline_config not recorded in state.json")
            config = cfg_loader.load(
                baseline,
                token=args.token,
                fts_submit_token=args.fts_submit_token,
                source_read_token=args.source_read_token,
                dest_write_token=args.dest_write_token,
            )
        except Exception as exc:
            log.error("Failed to load config for FTS3 connection: %s", exc)
            sys.exit(1)

        fts_session = fts_client_mod.build_session(
            config["tokens"]["fts_submit"],
            config["fts"].get("ssl_verify", True),
        )
        fts_client = fts_client_mod.FTSClient(
            config["fts"]["endpoint"], fts_session,
        )

        results = fts_canceller.cancel_jobs(job_ids, fts_client)

        ok  = sum(1 for r in results if r["cancelled"])
        err = len(results) - ok
        print("\nCancel results:")
        for r in results:
            status = "OK" if r["cancelled"] else "FAILED ({})".format(r["error"])
            print("  {} {}".format(r["job_id"], status))
        print("\n{}/{} jobs cancelled{}.".format(
            ok, len(results),
            ", {} error(s)".format(err) if err else "",
        ))
        sys.exit(0 if err == 0 else 1)

    # --report: regenerate summary reports without running anything.
    if args.report:
        seq_dir = args.report
        try:
            state = seq_state.load(seq_dir)
            runs_dir_val = (
                args.runs_dir
                or state.get("runs_dir")
                or os.path.join(seq_dir, "runs")
            )
            seq_reporter.generate_summary(seq_dir, state, runs_dir=runs_dir_val)
        except Exception as exc:
            log.error("Failed to generate report for %s: %s", seq_dir, exc,
                      exc_info=True)
            sys.exit(1)
        sys.exit(0)

    # --skip-from-case: mark pending trials in cases >= N as skipped.
    if args.skip_from_case:
        seq_dir = args.skip_from_case
        if args.from_case_index is None:
            parser.error("--skip-from-case requires --from-case-index N")
        try:
            state = seq_state.load(seq_dir)
            n_skipped = seq_state.skip_cases_from(
                seq_dir, state, args.from_case_index,
            )
            if n_skipped == 0:
                log.warning(
                    "No pending trials found in cases from index %d — nothing skipped",
                    args.from_case_index,
                )
            else:
                log.info(
                    "Marked %d trial(s) as skipped (cases from index %d onwards)",
                    n_skipped, args.from_case_index,
                )
        except Exception as exc:
            log.error("Failed to update state in %s: %s", seq_dir, exc,
                      exc_info=True)
            sys.exit(1)
        sys.exit(0)

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
            parser.error("params is required unless --rerun-failed or --cancel-jobs is given")

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
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:
        log.error("Sequence failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
