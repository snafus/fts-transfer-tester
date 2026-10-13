# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor bumps may change
behaviour).

## [0.2.0] - 2026-10-13

### Upgrade notes

- **Sequence execution order changed.** `sequence.execution.order` now
  defaults to `trial_major` in `sequential` mode: one round of every case, then
  the next round. To keep the old behaviour (all trials of a case before the
  next case), set `order: case_major`.
- **Metric definitions changed.** Peak aggregate throughput, concurrency and
  retry statistics are computed differently (see *Fixed*). Summary tables from
  before 0.2.0 can't be compared directly with new ones. Run
  `fts-sequence --recompute <SEQUENCE_DIR>` (or `fts-run --recompute <RUN_ID>`)
  to rebuild old results from their stored normalised records. Retry
  statistics for old runs that used framework retry can't be fully
  reconstructed, because older retry records don't identify the planned
  transfer.
- **Summary schema.** `throughput_p10` was added to the snapshot, the run
  reports and the sequence summary CSV and markdown tables. Timeseries buckets
  carry `bucket_span_s`. Retry records carry `source_surl` and `dest_surl`.
  File and retry records carry `origin_dest_surl`.

### Added

- X.509 VOMS proxy authentication (`auth.mode: x509`, `--x509-proxy`). One
  proxy covers every role. Delegation is done outside the framework; the
  proxy and delegation lifetimes are checked at campaign start (#11).
- HTTP-TPC copy direction: `transfer.copy_mode: pull | push`, appended to the
  destination URL and checked by FTS3 echoing it back (#12).
- `transfer.destination_naming: uuid | date`. Every real resubmission gets a
  fresh destination, so a duplicate live job can never write to the same path
  as the original.
- `sequence.execution.mode: parallel_cases`, which runs distinct cases
  concurrently, and `sequence.execution.order: trial_major | case_major` (#13).
- `fts-run --cancel-job JOB_ID` for cancelling a single job.
- `fts-run --recompute RUN_ID` and `fts-sequence --recompute SEQUENCE_DIR`
  rebuild metrics and reports offline from stored normalised records (#15).
- `throughput_p10` (#14).

### Fixed

- Peak aggregate throughput could be lower than the campaign aggregate. The
  truncated last timeseries bucket was divided by the full bucket width. The
  peak now uses full-width buckets only and is never below the aggregate (#14).
- Concurrency ignored FAILED and CANCELED transfers. They now count as
  active; throughput stays finished-only (#16).
- Mean concurrency was biased low by an empty trailing one-second bucket (#16).
- Retry statistics now count once per planned transfer across framework retry
  rounds, so `retry_rate` can no longer exceed 1. Padded duplicate sources are
  counted separately (#16, #17).
- With `max_files` padding, merging framework-retry results replaced every
  occurrence of a retried source, so records were duplicated and successful
  siblings lost. Sequential-mode retry also re-transferred siblings that had
  already succeeded (#17).

## [0.1.2] - 2026-05-16

### Added

- `transfer.nostreams` for parallel-stream transfers (#7).

### Fixed

- Crash when `nostreams` is the string `'None'` from YAML (#8).

## [0.1.1] - 2026-05-13

### Added

- OIDC client-credentials token generation, with optional audience and
  `{dst_prefix_path}` / `{src_prefix_path}` scope templates. Tokens are
  refreshed at the start of each trial.
- Weighted multi-destination support, interleaved by smooth weighted
  round-robin.
- `transfer.shuffle_source_pfns`. When `max_files` exceeds the inventory, the
  inventory is sampled with repetition.
- `fts-sequence --check-tokens`, `--cancel-jobs`, `--report` and
  `--skip-from-case`, and the `SKIPPED` trial status.
- `peak_aggregate_throughput_bytes_per_s` in the snapshot and summary
  reports.
- Sequence run directories named by case and trial, with `runs/index.json`
  listing job IDs.
- Resume and cancel instructions printed on Ctrl-C.

### Fixed

- Polling survives transient 401/403 auth errors (#6).
- Storage tokens are placed in per-file arrays for FTS3 3.12+ and always
  forwarded when configured.
- 500-recovery scan handles string `job_metadata` and int coercion, and
  retries at +5s, +15s and +60s.
- SUBMISSION_FAILED chunks were excluded from the framework retry loop.
- The sequence reporter used the wrong snapshot keys for file counts.

## [0.1.0] - 2026-04-24

Initial release.

[0.2.0]: https://github.com/snafus/fts-transfer-tester/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/snafus/fts-transfer-tester/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/snafus/fts-transfer-tester/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/snafus/fts-transfer-tester/releases/tag/v0.1.0
