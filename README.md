# FTS3 REST Test Framework

Production-grade, automation-first FTS3 transfer benchmarking framework written in Python 3.6.8. Drives bulk file transfer campaigns against FTS3 endpoints via REST exclusively, harvests file-level metrics as the authoritative source of truth, and persists all raw and normalised data for reproducible offline analysis.

---

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Inventory File Format](#inventory-file-format)
- [Configuration Reference](#configuration-reference)
- [CLI Usage](#cli-usage)
- [Sequence Runner](#sequence-runner)
- [Programmatic Usage](#programmatic-usage)
- [Run Directory Layout](#run-directory-layout)
- [Reports](#reports)
- [Campaign Resumption](#campaign-resumption)
- [Framework Retry](#framework-retry)
- [Cleanup](#cleanup)
- [Throughput Estimation Method](#throughput-estimation-method)
- [Metrics Reference](#metrics-reference)
- [Exit Codes](#exit-codes)
- [Testing](#testing)
- [Architecture Overview](#architecture-overview)
- [Design Constraints and Non-Goals](#design-constraints-and-non-goals)

---

## Features

- **REST-native** — all FTS3 interaction via the REST API; no `fts-rest-cli`, no `davix`, no subprocess
- **File-level metrics are authoritative** — all metrics derive from `GET /jobs/{id}/files`; job-level state is used only for polling
- **Raw-data-first** — every REST response written to disk before processing; reports can always be regenerated from persisted data without re-contacting FTS3
- **Deterministic destination mapping** — source PFNs sorted and mapped to `{dst_prefix}/{test_label}/testfile_{N:06d}` at campaign start; mapping persisted in the run manifest
- **Campaign resumption** — interrupted runs are detected by `run_id` in FTS3 `job_metadata`; the poller re-attaches to in-flight jobs
- **500-recovery on submission** — a POST returning HTTP 500 does not mean the job was not created; the framework scans FTS3 before resubmitting
- **Framework-level retry** — failed/canceled files can be resubmitted in configurable retry rounds (default off)
- **Pre/post cleanup** — WebDAV HTTP DELETE against destination files, non-fatal
- **ADLER32 checksums** — fetched via WebDAV `Want-Digest` HEAD requests before submission, verified by FTS3 end-to-end
- **SSL verification control** — `true | false | /path/to/ca-bundle.pem`
- **Console, JSON, Markdown, HTML, CSV, and timeseries CSV reports**
- **Parameter-sweep sequence runner** — run a baseline config across a sweep of parameters (cartesian or zip) with per-case trial repetition and resumption
- **Python 3.6.8 compatible** — no dataclasses, no walrus operator, no numpy

---

## Requirements

| Requirement | Version |
|---|---|
| Python | 3.6.8 |
| requests | 2.27.1 |
| urllib3 | 1.26.18 |
| PyYAML | 5.4.1 |
| certifi | 2021.10.8 |
| FTS3 server | 3.11+ (JWT token auth required) |

---

## Installation

### Local development (recommended)

```bash
# Create virtual environment with uv (or python3.6 -m venv .venv)
uv venv .venv
source .venv/bin/activate

# Install framework in editable mode
uv pip install -e .

# Install test dependencies
uv pip install pytest responses freezegun pyflakes PyYAML requests
```

### Production install (Python 3.6.8 target)

```bash
python3.6 -m venv .venv
source .venv/bin/activate
pip install pip==21.3.1          # last pip with Python 3.6 support
pip install -r requirements.txt  # pinned production deps
pip install -e .
```

### Verify

```bash
fts-run --help
```

---

## Quick Start

**1. Create a PFN list** — one URL per line (HTTPS or `davs://`), optionally with a pre-computed ADLER32 checksum:

```
# Plain URL list
https://source.example.org/data/file_001.dat
davs://source.example.org/data/file_002.dat

# URL + checksum (skips Want-Digest HEAD request)
https://source.example.org/data/file_001.dat,adler32:1a2b3c4d
https://source.example.org/data/file_002.dat,1a2b3c4d
```

**2. Copy and edit the example config:**

```bash
cp config/example_config.yaml my_campaign.yaml
$EDITOR my_campaign.yaml
```

At minimum, set:
- `fts.endpoint`
- `tokens.fts_submit`, `tokens.source_read`, `tokens.dest_write`
- `transfer.source_pfns_file`
- `transfer.dst_prefix`
- `run.test_label`

**3. Run the campaign:**

```bash
fts-run my_campaign.yaml
```

The framework exits 0 if `success_rate >= min_success_threshold`, 1 otherwise.

---

## Inventory File Format

The `source_pfns_file` supports two line formats, which may be mixed:

**Plain URL** — ADLER32 fetched via `Want-Digest` HEAD before submission:
```
https://source.example.org/data/file_001.dat
davs://source.example.org/data/file_002.dat
```

**URL with pre-computed checksum** — skips the Want-Digest fetch for that file:
```
https://source.example.org/data/file_001.dat,adler32:1a2b3c4d
https://source.example.org/data/file_002.dat,1a2b3c4d
```

Both `adler32:<8-hex>` and bare 8-character hex strings are accepted. `davs://` URLs are rewritten to `https://` before any direct HTTP call.

When `verify_checksum` is `none` or `target`, the pre-submission checksum fetch is skipped for all files regardless of the inventory format, and no checksum is included in the FTS3 job payload.

---

## Configuration Reference

Full example: [`config/example_config.yaml`](config/example_config.yaml)

### `run`

| Key | Type | Default | Description |
|---|---|---|---|
| `run_id` | string \| null | auto-generated | Unique run identifier `{YYYYMMDD_HHMMSS}_{8hex}`. Set explicitly to resume a prior run. |
| `test_label` | string | required | Human label used in destination paths and all reports. |

### `fts`

| Key | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | required | FTS3 REST base URL, e.g. `https://fts.example.org:8446` |
| `ssl_verify` | bool \| string | `true` | SSL certificate verification. `true`, `false`, or path to a CA bundle file. |

### `tokens`

Three token roles are required. They may share the same value in single-IAM deployments.

| Key | Description |
|---|---|
| `fts_submit` | Bearer token for FTS3 REST API (`POST /jobs`, `GET /jobs/*`) |
| `source_read` | Bearer token for WebDAV `Want-Digest` HEAD requests on source storage |
| `dest_write` | Bearer token for WebDAV DELETE on destination storage (cleanup only) |

Tokens are never written to disk. The config copy in `runs/<run_id>/config.yaml` has all token values replaced with `<REDACTED>`.

#### Token resolution order (highest priority wins)

Tokens can be supplied from multiple sources. When the same role is supplied by more than one source the highest-priority source wins:

| Priority | Source |
|---|---|
| 1 (highest) | `--fts-submit-token` / `--source-read-token` / `--dest-write-token` CLI flags |
| 2 | `--token` CLI flag (applies the same value to all three roles) |
| 3 | `FTS_SUBMIT_TOKEN` / `SOURCE_READ_TOKEN` / `DEST_WRITE_TOKEN` environment variables |
| 4 | `FTS_TOKEN` environment variable (applies to all three roles) |
| 5 (lowest) | `tokens` section in the YAML config |

The `tokens` YAML section may be omitted entirely if all three roles are satisfied through environment variables or CLI flags.

### `transfer`

| Key | Type | Default | Description |
|---|---|---|---|
| `source_pfns_file` | string | required | Path to source PFN list (see [Inventory file format](#inventory-file-format)) |
| `dst_prefix` | string | required | Destination base URL (`https://` or `davs://`) |
| `preserve_extension` | bool | `false` | If true, append original file extension to destination filename |
| `checksum_algorithm` | string | `"adler32"` | Only ADLER32 is supported |
| `verify_checksum` | string | `"both"` | FTS3 checksum mode: `both`, `source`, `target`, `none`. Modes `none` and `target` skip the pre-submission Want-Digest fetch. |
| `overwrite` | bool | `false` | Allow FTS3 to overwrite existing destination files |
| `max_files` | int \| null | `null` | Limit the number of source PFNs used. `null` uses all. Applied before destination planning and checksum fetch. |
| `chunk_size` | int | `200` | Files per FTS3 job. Maximum 200 (FTS3 limit). |
| `priority` | int | `3` | FTS3 job priority 1 (lowest) to 5 (highest) |
| `activity` | string | `"default"` | FTS3 activity share label |
| `job_metadata` | dict | `{}` | User-supplied key/value pairs merged into `job_metadata`. Framework keys (`run_id`, `chunk_index`, `retry_round`, `test_label`) always take priority. |

### `concurrency`

| Key | Type | Default | Description |
|---|---|---|---|
| `want_digest_workers` | int | `8` | Parallel threads for pre-submission ADLER32 HEAD requests |

### `submission`

| Key | Type | Default | Description |
|---|---|---|---|
| `scan_window_s` | int | `300` | After a POST /jobs returns HTTP 500, scan this many seconds back in FTS3 to detect if the job was actually created. Minimum enforced: 60s. |

### `polling`

| Key | Type | Default | Description |
|---|---|---|---|
| `initial_interval_s` | int | `30` | First polling interval in seconds |
| `backoff_multiplier` | float | `1.5` | Multiplier applied to the interval after each round |
| `max_interval_s` | int | `300` | Maximum polling interval (cap on backoff) |
| `campaign_timeout_s` | int | `86400` | Raise `PollingTimeoutError` if jobs are still active after this many seconds |

### `cleanup`

| Key | Type | Default | Description |
|---|---|---|---|
| `before` | bool | `false` | DELETE destination files before submitting transfers |
| `after` | bool | `false` | DELETE successfully transferred destination files after the campaign |

Cleanup failures are logged as warnings and never abort the campaign. HTTP 404 on DELETE is treated as success (idempotent).

### `retry`

| Key | Type | Default | Description |
|---|---|---|---|
| `fts_retry_max` | int | `2` | FTS3-level retries per file (embedded in the job submission payload) |
| `framework_retry_max` | int | `0` | Framework-level resubmission rounds. `0` disables. If > 0, FAILED and CANCELED files are resubmitted after each round until this limit or until no failures remain. |
| `min_success_threshold` | float | `0.95` | Campaign PASS threshold. `threshold_passed = success_rate >= min_success_threshold`. |

### `output`

| Key | Type | Default | Description |
|---|---|---|---|
| `base_dir` | string | `"runs"` | Base directory for all run outputs. Can be overridden by `--runs-dir` on the CLI. |
| `timeseries_bucket_s` | int | `60` | Bucket width in seconds for the timeseries throughput/concurrency CSV |
| `reports.console` | bool | `true` | Print summary to stdout at campaign end |
| `reports.json` | bool | `true` | Write `reports/summary.json` and `metrics/snapshot.json` |
| `reports.markdown` | bool | `true` | Write `reports/report.md` |
| `reports.html` | bool | `false` | Write `reports/report.html` |
| `reports.csv` | bool | `true` | Write `reports/files.csv` (per-file metrics) |
| `reports.timeseries_csv` | bool | `true` | Write `reports/timeseries.csv` (per-bucket throughput and concurrency) |

---

## CLI Usage

```
fts-run <config> [options]
```

| Argument | Description |
|---|---|
| `config` | Path to campaign YAML config file (required) |
| `--runs-dir DIR` | Base directory for run outputs (default: `runs/`) |
| `--log-level LEVEL` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`) |
| `--token TOKEN` | Bearer token for all three roles (overrides `FTS_TOKEN` env var and YAML) |
| `--fts-submit-token TOKEN` | Token for FTS3 job submission (overrides `FTS_SUBMIT_TOKEN` env var) |
| `--source-read-token TOKEN` | Token for source storage reads (overrides `SOURCE_READ_TOKEN` env var) |
| `--dest-write-token TOKEN` | Token for destination storage writes (overrides `DEST_WRITE_TOKEN` env var) |

Token precedence: per-role CLI flags > `--token` > per-role env vars > `FTS_TOKEN` > YAML. See [Token resolution order](#token-resolution-order).

### Examples

```bash
# Basic run
fts-run campaign.yaml

# Supply tokens via CLI (no tokens section needed in YAML)
fts-run campaign.yaml --token "$BEARER_TOKEN"

# Per-role tokens
fts-run campaign.yaml \
  --fts-submit-token "$FTS_TOKEN" \
  --source-read-token "$SRC_TOKEN" \
  --dest-write-token "$DST_TOKEN"

# Supply tokens via environment variables
export FTS_TOKEN="$BEARER_TOKEN"
fts-run campaign.yaml

# Custom output directory and verbose logging
fts-run campaign.yaml --runs-dir /data/fts-runs --log-level DEBUG

# Resume an interrupted run (set run_id in config to the prior run_id)
fts-run campaign.yaml   # with run.run_id set in YAML to the existing run_id

# Pipe JSON report to jq
cat runs/<run_id>/metrics/snapshot.json | jq '.success_rate'
```

---

## Sequence Runner

The sequence runner executes a baseline campaign config repeatedly across a sweep of parameter variations, with configurable trial repetition and full resumption support.

```
fts-sequence <params> [options]
```

| Argument | Description |
|---|---|
| `params` | Path to sequence parameter YAML file (required) |
| `--resume SEQUENCE_DIR` | Resume an interrupted sequence from its output directory |
| `--runs-dir DIR` | Base directory for individual run outputs (default: `runs/`) |
| `--log-level LEVEL` | Logging verbosity (default: `INFO`) |
| `--token TOKEN` | Shared bearer token for all roles |
| `--fts-submit-token TOKEN` | Per-role FTS3 submission token |
| `--source-read-token TOKEN` | Per-role source-read token |
| `--dest-write-token TOKEN` | Per-role destination-write token |

### Sequence parameter file

```yaml
baseline_config: "config/my_campaign.yaml"

sequence:
  trials: 3          # repeat each case N times (for statistical reliability)
  label: "scale_test"

  sweep:
    mode: cartesian  # cartesian (default) | zip
    parameters:
      # Explicit list:
      transfer.max_files: [100, 200, 500]
      # Different source file sets:
      # transfer.source_pfns_file: ["small_files.txt", "large_files.txt"]
      # Integer range shorthand (inclusive stop): generates [50, 100, 150, 200]
      # transfer.chunk_size: {range: [50, 200, 50]}

  output:
    base_dir: "sequences"
```

**Sweep modes:**
- `cartesian` — full cross-product of all parameter lists. The example above produces 3 cases × 3 trials = 9 runs.
- `zip` — elements paired positionally; all lists must have equal length. Useful when parameters are logically coupled (e.g. file count + matching source file).

**Parameter keys** use dot-notation matching the baseline config structure (e.g. `transfer.max_files`, `transfer.source_pfns_file`).  Any scalar value in the baseline config can be overridden.

### Output layout

```
sequences/<sequence_id>/
    params.yaml             copy of the sequence parameter file
    state.json              resumption state (one entry per trial)
    reports/
        summary.md
        summary.json
        summary.csv         one row per trial: params + key metrics
```

Individual campaign runs are written to `runs/<run_id>/` as normal.

### Resumption

Each trial is recorded in `state.json` as `pending | running | completed | failed` before it starts.  A trial left in `running` state (process interrupted mid-run) is treated as pending on resume.

```bash
# Start a sequence
fts-sequence params.yaml

# Resume after interruption
fts-sequence params.yaml --resume sequences/20260324_abc123_scale_test/
```

### Summary reports

The sequence reporter reads `metrics/snapshot.json` from each completed trial and produces:

- **`summary.csv`** — one row per trial with all case parameters and key metrics (`success_rate`, `throughput_mean/p50/p90/stddev`, `campaign_wall_s`, etc.)
- **`summary.json`** — full structured data including per-case aggregates
- **`summary.md`** — human-readable table with per-case mean ± stdev across trials

Failed trials are excluded from per-case aggregates but appear in the raw runs table.

---

## Programmatic Usage

```python
from fts_framework.config.loader import load
from fts_framework.runner import run_campaign

config = load("campaign.yaml")
snapshot = run_campaign(config, runs_dir="runs")

print("Success rate: {:.1%}".format(snapshot["success_rate"]))
print("Threshold passed:", snapshot["threshold_passed"])
print("Peak concurrency:", snapshot["peak_concurrency"])
```

### Working with file records directly

```python
from fts_framework.persistence.store import load_normalized

file_records, retry_records, dm_records = load_normalized(run_id, runs_dir="runs")

failed = [f for f in file_records if f["file_state"] == "FAILED"]
for f in failed:
    print(f["source_surl"], f["reason"])
```

### Regenerating reports offline

```python
from fts_framework.persistence.store import load_normalized, load_manifest
from fts_framework.metrics.engine import compute
from fts_framework.reporting.renderer import render_all

run_id = "20260323_143201_a3f7c1b9"
file_records, retry_records, _ = load_normalized(run_id)
snapshot = compute(file_records, retry_records, config, run_id)
render_all(snapshot, config, runs_dir="runs")
```

---

## Run Directory Layout

Each campaign produces a self-contained directory:

```
runs/
└── 20260323_143201_a3f7c1b9/
    ├── manifest.json              # destination mapping, subjob list, run state
    ├── config.yaml                # config copy with tokens redacted
    ├── submitted_payloads/
    │   ├── chunk_000_round_0.json # POST /jobs body for chunk 0, round 0
    │   └── chunk_001_round_0.json
    ├── raw/
    │   ├── jobs/
    │   │   └── <job_id>.json      # terminal job state from GET /jobs/{id}
    │   ├── files/
    │   │   └── <job_id>.json      # raw file records from GET /jobs/{id}/files
    │   ├── retries/
    │   │   └── <job_id>_<fid>.json
    │   └── dm/
    │       └── <job_id>.json
    ├── normalized/
    │   ├── file_records.json
    │   ├── retry_records.json
    │   └── dm_records.json
    ├── metrics/
    │   └── snapshot.json
    ├── cleanup_pre.json           # cleanup audit (if cleanup.before: true)
    ├── cleanup_post.json          # cleanup audit (if cleanup.after: true)
    └── reports/
        ├── summary.json           # if reports.json: true
        ├── report.md              # if reports.markdown: true
        ├── report.html            # if reports.html: true
        ├── files.csv              # if reports.csv: true
        └── timeseries.csv         # if reports.timeseries_csv: true
```

**Key invariant**: submitted payloads are written before the POST /jobs request. If the process crashes mid-campaign, every submitted job is recoverable from `manifest.json`.

---

## Reports

### Console

Printed to stderr at campaign end. Includes counts, success rate, throughput percentiles, and threshold result.

### JSON (`metrics/snapshot.json` and `reports/summary.json`)

Complete `MetricsSnapshot` dict. All numeric fields are present; `null` where a value cannot be computed (e.g. throughput when no files finished).

### Markdown (`reports/report.md`)

Structured sections:
1. Run metadata
2. Transfer counts and rates
3. Throughput statistics (mean, p50, p90, p95, p99, max, aggregate)
4. Duration statistics
5. Retry summary
6. Concurrency
7. Failure reasons
8. Per-subjob table (job IDs and FTS monitor URLs)

### HTML (`reports/report.html`)

Same content as Markdown, rendered as a self-contained HTML file.

### Per-file CSV (`reports/files.csv`)

One row per file record. Columns: `file_id`, `job_id`, `file_state`, `source_surl`, `dest_surl`, `filesize`, `throughput`, `throughput_wire`, `throughput_wall`, `wall_duration_s`, `tx_duration`, `start_time`, `finish_time`, `checksum`, `reason`.

### Timeseries CSV (`reports/timeseries.csv`)

Per-bucket aggregate throughput and active-transfer concurrency. Columns: `bucket_start`, `bucket_start_ts`, `bucket_end`, `bucket_end_ts`, `active_transfers`, `aggregate_throughput_bytes_s`, `aggregate_throughput_mb_s`.

See [Throughput estimation method](#throughput-estimation-method) for the full derivation.

---

## Throughput Estimation Method

The framework cannot observe instantaneous wire throughput directly from the FTS3 REST API — only per-file start and finish timestamps and file sizes are available. Two complementary estimates are produced.

### Constant-rate model

Each finished transfer *i* is modelled as transferring its bytes at a constant rate throughout its active interval:

```
rate_i  =  filesize_i / (finish_i − start_i)          [bytes/s]
```

This is the assumption of **uniform data flow** over the observed wall-clock duration. It is an approximation: real transfers exhibit slow-start, rate variation, and queuing delays. The model is unbiased in expectation but will smooth over transient bursts and stalls within a single transfer.

Files with zero filesize, zero or negative wall duration, or missing timestamps are excluded from all throughput estimates.

### Aggregate throughput timeseries (`reports/timeseries.csv`)

Time is divided into non-overlapping buckets of width *W* seconds (default 60, configurable via `output.timeseries_bucket_s`). The aggregate throughput for bucket *b* covering `[t_b, t_b + W)` is:

```
B_b  =  Σ_i  rate_i × overlap(i, b)          [bytes]

             ⎧ min(finish_i, t_b + W) − max(start_i, t_b)   if > 0
overlap(i,b) = ⎨
             ⎩ 0                                              otherwise

throughput_b  =  B_b / W          [bytes/s]
```

That is, each transfer contributes bytes to each bucket in proportion to how many seconds of its active interval fall within that bucket. Summing over all transfers and dividing by the bucket width gives the mean aggregate data rate for the bucket. The active-transfer count for bucket *b* is the number of transfers for which `overlap(i, b) > 0`.

This estimator is **time-weighted**: a transfer that spans multiple buckets contributes to each in proportion to its overlap, so no bytes are counted twice and no bytes are lost at bucket boundaries.

### Per-second timeline (`snapshot.json` → `concurrency_timeline`)

The same constant-rate model is applied at 1-second resolution using a **difference-array / prefix-sum** algorithm (O(N + T), where N is the number of transfers and T is the campaign duration in seconds):

For each transfer *i*, define:
- `+rate_i` added to a running accumulator at second `floor(start_i)`
- `−rate_i` removed at second `floor(finish_i)`

A single linear scan over the timeline produces the aggregate throughput at each second. This is algebraically equivalent to the overlap integral above with *W* = 1 s, using integer-second boundaries. The same pass also accumulates `bytes_in_flight` (total filesize of all active transfers at each second) using ±filesize increments.

### Limitations

- The constant-rate assumption introduces error proportional to within-transfer rate variance. It is most accurate for large numbers of concurrent transfers, where the central-limit effect smooths individual deviations.
- Wall duration includes any TCP slow-start, protocol handshake, and checksum verification time, not only wire transfer time. This causes `rate_i` to be an underestimate of peak wire throughput for short or small files.
- The FTS3 agent also reports a per-file `throughput` value (in MiB/s, converted to bytes/s on ingest). This value is used for the per-file statistics (mean, stddev, percentiles) but **not** for the timeseries, which is derived entirely from timestamps and file sizes. The two estimates will generally agree in aggregate but may differ per-file.

---

## Campaign Resumption

If a campaign is interrupted (crash, network loss, `Ctrl+C`), it can be resumed by rerunning with the same `run_id`:

```yaml
run:
  run_id: "20260323_143201_a3f7c1b9"   # from the interrupted run
```

The resume controller:
1. Detects the existing run directory via `manifest.json`
2. Queries FTS3 for the status of all known job IDs
3. Re-attaches the poller to any jobs still in flight
4. Skips already-terminal jobs and proceeds directly to harvest

**Note**: checksums are not persisted between runs. On resume, retry submissions omit checksum fields and FTS3 skips end-to-end checksum verification for those files.

---

## Framework Retry

With `retry.framework_retry_max > 0`, after the initial campaign completes the framework identifies all FAILED and CANCELED files and resubmits them as new FTS3 jobs:

```yaml
retry:
  fts_retry_max: 2           # FTS3-level (per file, within each job)
  framework_retry_max: 2     # up to 2 additional submission rounds
  min_success_threshold: 0.99
```

Each retry round:
- Creates new FTS3 jobs for failed files only
- Polls and harvests independently
- Merges results back into the authoritative file record list (by source PFN)
- Writes updated normalised records to disk

The retry loop stops early if a round produces no failures.

---

## Cleanup

Cleanup uses direct WebDAV HTTP DELETE against the destination storage — not via FTS3.

```yaml
cleanup:
  before: true    # DELETE destination files before submitting (ensure clean state)
  after: false    # DELETE transferred files after campaign (leave no test data)
```

The `dest_write` token is used for cleanup sessions. HTTP 404 is treated as success. All other errors are logged as warnings. An audit log is written to `cleanup_pre.json` / `cleanup_post.json`.

---

## Metrics Reference

All metrics appear in `metrics/snapshot.json`.

### Counts

| Field | Description |
|---|---|
| `total_files` | Total file records harvested |
| `finished` | Files with `file_state = FINISHED` |
| `failed` | Files with `file_state = FAILED` |
| `canceled` | Files with `file_state = CANCELED` |
| `not_used` | Files with `file_state = NOT_USED` (excluded from success rate) |
| `staging_unsupported` | Files in STAGING state (tape not supported; counted as failed) |

### Rates

| Field | Formula | Description |
|---|---|---|
| `success_rate` | `finished / (total - not_used - staging_unsupported)` | Fraction of eligible files that completed successfully |
| `failure_rate` | `(failed + canceled) / eligible` | Fraction of eligible files that failed |
| `threshold_passed` | `success_rate >= min_success_threshold` | Campaign PASS/FAIL |

### Campaign time

| Field | Description |
|---|---|
| `campaign_start` | ISO 8601 timestamp of the earliest file `start_time` |
| `campaign_end` | ISO 8601 timestamp of the latest file `finish_time` |
| `campaign_wall_s` | `campaign_end − campaign_start` in seconds; `null` if no valid timestamps |

### Throughput

Primary source is the FTS3 agent-reported `throughput` field (MiB/s, converted to bytes/s on ingest). Falls back to `filesize / tx_duration` (wire throughput) when the primary is zero or absent.

| Field | Description |
|---|---|
| `throughput_mean` | Mean per-file throughput (bytes/s) |
| `throughput_stddev` | Sample standard deviation of per-file throughput (bytes/s); `null` if fewer than 2 files |
| `throughput_p50/p90/p95/p99` | Percentiles (bytes/s) |
| `throughput_max` | Maximum per-file throughput observed (bytes/s) |
| `aggregate_throughput_bytes_per_s` | `total_finished_bytes / campaign_wall_s` |

### Duration

Computed from `finish_time − start_time` (wall clock, includes protocol overhead and checksum verification).

| Field | Description |
|---|---|
| `duration_mean_s` | Mean wall duration (seconds) |
| `duration_stddev_s` | Sample standard deviation of wall duration (seconds); `null` if fewer than 2 files |
| `duration_p50/p90/p95_s` | Percentiles (seconds) |

### Retries

| Field | Description |
|---|---|
| `total_retries` | Total retry records across all files |
| `files_with_retries` | Number of distinct files that had at least one FTS3-level retry |
| `retry_rate` | `files_with_retries / total_files` |
| `retry_distribution` | `{"1": N, "2": N, ...}` — how many files had each retry count |

### Concurrency

Derived from file `start_time`/`finish_time` timestamps using a difference-array / prefix-sum at 1-second resolution. See [Throughput Estimation Method](#throughput-estimation-method) for the full derivation.

| Field | Description |
|---|---|
| `peak_concurrency` | Maximum simultaneously active transfers |
| `mean_concurrency` | Mean active transfers per second |
| `concurrency_timeline` | Per-second timeline: `[{"t": epoch, "active": N, "bytes_in_flight": B, "throughput_bytes_s": R}]` |

### Failure reasons

`failure_reasons` is a dict mapping the raw FTS3 reason string to a count. Empty reasons appear as `"UNKNOWN"`.

---

## Exit Codes

### `fts-run`

| Code | Meaning |
|---|---|
| `0` | Campaign completed and `threshold_passed = true` |
| `1` | Campaign completed but `threshold_passed = false`, or campaign raised an unhandled exception |

### `fts-sequence`

| Code | Meaning |
|---|---|
| `0` | Sequence loop completed (individual trial failures are logged and do not affect exit code) |
| `1` | Unhandled exception in the sequence runner itself (e.g. invalid params file, unreadable baseline config) |

---

## Testing

```bash
# Run all unit tests
pytest tests/unit/ -v

# Run with tox against Python 3.6.8 (CI target)
tox -e py36

# Run local tox environment (uses installed Python)
tox -e local

# Run pyflakes linter
python -m pyflakes fts_framework/
```

Integration tests require a live FTS3 endpoint:

```bash
FTS_INTEGRATION_ENDPOINT=https://fts.example.org:8446 pytest tests/integration/ -v
```

**775 unit tests** cover all modules: config loader, inventory, destination planner, checksum fetcher, FTS client, submission (including 500-recovery), poller, collector, persistence, resume controller, metrics engine, cleanup manager, reporting renderer, runner orchestration, and sequence runner (loader, state, reporter).

---

## Architecture Overview

```
sequence/runner.py  (sequence orchestration — wraps runner.py per trial)
│
runner.py  (single-campaign orchestration)
│
├── config/loader.py          Load and validate YAML config
├── inventory/loader.py       Load and validate source PFN list
├── destination/planner.py    Compute deterministic src→dst mapping
├── checksum/fetcher.py       Parallel ADLER32 Want-Digest HEAD requests
├── fts/
│   ├── client.py             Authenticated requests.Session builder
│   ├── submission.py         Chunk mapping, payload construction, POST /jobs + 500-recovery
│   ├── poller.py             Poll GET /jobs/{id} to terminal state with backoff
│   └── collector.py          Harvest GET /jobs/{id}/files, retries, dm
├── persistence/store.py      All disk I/O (manifest, raw, normalised, reports)
├── resume/controller.py      Detect interrupted runs, re-attach poller
├── metrics/engine.py         Pure metric computation from FileRecord list
├── reporting/renderer.py     Console, JSON, Markdown, HTML report generation
├── cleanup/manager.py        WebDAV HTTP DELETE for pre/post cleanup
├── sequence/
│   ├── loader.py             Parse sequence params; generate cases (cartesian/zip)
│   ├── state.py              state.json persistence and resumption tracking
│   ├── runner.py             Outer sweep loop; apply overrides; call run_campaign()
│   └── reporter.py           Aggregate trial snapshots; write summary reports
└── exceptions.py             Typed exception hierarchy
```

### Module boundaries (enforced)

- `fts/client.py` — session creation only; no business logic
- `fts/submission.py` — payload construction and POST; no polling
- `fts/poller.py` — polling loop only; no harvesting
- `fts/collector.py` — REST harvesting only; no metric computation
- `metrics/engine.py` — computation only; no I/O
- `persistence/store.py` — all disk I/O; no computation
- `cleanup/manager.py` — WebDAV DELETE only; uses `requests` not FTS3

### FTS3 REST endpoints used

```
POST /jobs                              Submit transfer job
GET  /jobs/{id}                         Poll job state
GET  /jobs/{id}/files                   Harvest file records (authoritative)
GET  /jobs/{id}/files/{fid}/retries     Harvest retry history
GET  /jobs/{id}/dm                      Harvest data-management records
GET  /whoami                            Validate token at campaign start
GET  /optimizer/current                 Log link optimizer state at campaign start
```

### Exception hierarchy

```
FTSFrameworkError
├── ConfigError
├── InventoryError
├── ChecksumFetchError
├── SubmissionError
├── PollingTimeoutError
├── TokenExpiredError          Always propagates; never swallowed
├── PersistenceError
├── CleanupError
└── ResumeError
```

---

## Design Constraints and Non-Goals

### Constraints

- Python 3.6.8 only — no walrus operator, no dataclasses, no `dict[str, int]` generic syntax
- No numpy, pandas, or scipy — all metrics use `statistics` (stdlib) and list comprehensions
- REST-only — no `fts-rest-cli`, no `davix`, no subprocess calls
- Tokens are never written to disk in any form
- Raw data is always persisted before any processing (reproducibility guarantee)
- File-level records (`GET /jobs/{id}/files`) are the sole authoritative source for all metrics

### Not supported

- Tape/staging workflows (`STAGING` state → treated as unsupported error)
- XRootD protocol operations
- SRM, gsiftp, dCache endpoints
- Token refresh (delegated to FTS3 server; framework has no OIDC logic)
- FTS3 link optimizer configuration
- Multi-protocol checksum algorithms other than ADLER32
- Real-time streaming metrics
