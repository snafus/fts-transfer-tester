# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor bumps may change
behaviour).

## [0.3.0] - 2026-11-26

### Upgrade notes

- **Tokens in existing run directories.** Run directories written by 0.2.0 or
  earlier hold storage tokens in `submitted_payloads/` (see *Security*).
  Redact them in place with
  `python tools/redact_payload_tokens.py runs/ sequences/`, after upgrading
  every host that might resume those runs: an older version resuming a
  redacted run would repost the `<REDACTED>` placeholder as the token.
- **Summary schema.** `summary.csv` gains a `nostreams` column between the
  `param_*` columns and the metrics, and the per-case and per-run tables gain
  a `Streams` column. The snapshot carries `nostreams_requested`. Runs made
  before this version show `-` until rebuilt with
  `fts-sequence --recompute <SEQUENCE_DIR>` (or `fts-run --recompute <RUN_ID>`).

### Security

- Storage tokens are no longer written to `submitted_payloads/`. The persisted
  copy of each job payload kept the per-file `source_tokens` and
  `destination_tokens` in clear text, because redaction only covered an older
  payload layout. They are now replaced with `<REDACTED>`. A resume that has
  to repost a persisted payload takes the tokens from the current config,
  which also means it no longer reposts tokens that have expired since the
  original attempt (#22).

### Added

- OIDC token fetches retry transient failures (connection errors, timeouts,
  HTTP 429 and 5xx) with exponential backoff, so a brief IAM outage no longer
  aborts a sequence at the per-trial token refresh. Tunable with
  `oidc.fetch_retry_max` (default 4) and `oidc.fetch_retry_backoff_s`
  (default 5); `fetch_retry_max: 0` restores the previous single-attempt
  behaviour (#20).
- The requested stream count (`transfer.nostreams`) is shown in every
  summary: the run report header (console, markdown, HTML), the snapshot
  (`nostreams_requested`), and the sequence summaries, as a `Streams` column
  in the per-case and per-run tables and a `nostreams` column in
  `summary.csv` and `summary.json`. An unset `nostreams` reads `default`
  (FTS3 decides). The value is the one actually submitted, so a non-integer
  sequence override that submission drops also reads `default`. It is not
  the count FTS3 used (#21, #22).
- `tools/redact_payload_tokens.py` redacts tokens in payloads persisted by
  earlier versions (#22).

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

[0.3.0]: https://github.com/snafus/fts-transfer-tester/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/snafus/fts-transfer-tester/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/snafus/fts-transfer-tester/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/snafus/fts-transfer-tester/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/snafus/fts-transfer-tester/releases/tag/v0.1.0
