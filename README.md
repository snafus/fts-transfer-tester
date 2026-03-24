# FTS3 REST Test Framework

Production-grade, automation-first FTS3 transfer benchmarking framework written in Python 3.6.8. Drives bulk file transfer campaigns against FTS3 endpoints via REST exclusively, harvests file-level metrics as the authoritative source of truth, and persists all raw and normalised data for reproducible offline analysis.

---

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration Reference](#configuration-reference)
- [CLI Usage](#cli-usage)
- [Programmatic Usage](#programmatic-usage)
- [Run Directory Layout](#run-directory-layout)
- [Reports](#reports)
- [Campaign Resumption](#campaign-resumption)
- [Framework Retry](#framework-retry)
- [Cleanup](#cleanup)
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
- **Console, JSON, Markdown, HTML reports**
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

**1. Create a PFN list** — one HTTPS source URL per line:

```
https://source.example.org/data/file_001.dat
https://source.example.org/data/file_002.dat
https://source.example.org/data/file_003.dat
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

### `transfer`

| Key | Type | Default | Description |
|---|---|---|---|
| `source_pfns_file` | string | required | Path to newline-separated HTTPS source PFN list |
| `dst_prefix` | string | required | Destination HTTPS/WebDAV base URL |
| `preserve_extension` | bool | `false` | If true, append original file extension to destination filename |
| `checksum_algorithm` | string | `"adler32"` | Only ADLER32 is supported |
| `verify_checksum` | string | `"both"` | FTS3 checksum mode: `both`, `source`, `target`, `none` |
| `overwrite` | bool | `false` | Allow FTS3 to overwrite existing destination files |
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
| `reports.console` | bool | `true` | Print summary to stderr at campaign end |
| `reports.json` | bool | `true` | Write `reports/summary.json` and `metrics/snapshot.json` |
| `reports.markdown` | bool | `true` | Write `reports/summary.md` |
| `reports.html` | bool | `false` | Write `reports/summary.html` |

---

## CLI Usage

```
fts-run <config> [--runs-dir DIR] [--log-level LEVEL]
```

| Argument | Description |
|---|---|
| `config` | Path to campaign YAML config file (required) |
| `--runs-dir DIR` | Base directory for run outputs (default: `runs/`) |
| `--log-level LEVEL` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`) |

### Examples

```bash
# Basic run
fts-run campaign.yaml

# Custom output directory and verbose logging
fts-run campaign.yaml --runs-dir /data/fts-runs --log-level DEBUG

# Resume an interrupted run (set run_id in config to the prior run_id)
fts-run campaign.yaml   # with run.run_id set in YAML to the existing run_id

# Pipe JSON report to jq
cat runs/<run_id>/metrics/snapshot.json | jq '.success_rate'
```

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
        ├── summary.json
        ├── summary.md
        └── summary.html           # if html: true
```

**Key invariant**: submitted payloads are written before the POST /jobs request. If the process crashes mid-campaign, every submitted job is recoverable from `manifest.json`.

---

## Reports

### Console

Printed to stderr at campaign end. Includes counts, success rate, throughput percentiles, and threshold result.

### JSON (`metrics/snapshot.json` and `reports/summary.json`)

Complete `MetricsSnapshot` dict. All numeric fields are present; `null` where a value cannot be computed (e.g. throughput when no files finished).

### Markdown (`reports/summary.md`)

Structured sections:
1. Run metadata
2. Transfer counts and rates
3. Throughput statistics (mean, p50, p90, p95, p99, max, aggregate)
4. Duration statistics
5. Retry summary
6. Concurrency
7. Failure reasons
8. Per-subjob table (job IDs and FTS monitor URLs)

### HTML (`reports/summary.html`)

Same content as Markdown, rendered as a self-contained HTML file.

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

### Throughput

Primary source is the FTS3 agent-reported `throughput` field. Falls back to `filesize / tx_duration` (wire throughput) when the primary is zero or absent.

| Field | Description |
|---|---|
| `throughput_mean` | Mean throughput across finished files (bytes/s) |
| `throughput_p50/p90/p95/p99` | Percentiles (bytes/s) |
| `throughput_max` | Maximum throughput observed (bytes/s) |
| `aggregate_throughput_bytes_per_s` | `total_bytes / campaign_wall_time` |

### Duration

Computed from `finish_time - start_time` (wall clock, not wire-only).

| Field | Description |
|---|---|
| `duration_mean_s` | Mean wall duration (seconds) |
| `duration_p50/p90/p95_s` | Percentiles (seconds) |

### Retries

| Field | Description |
|---|---|
| `total_retries` | Total retry records across all files |
| `files_with_retries` | Number of distinct files that had at least one FTS3-level retry |
| `retry_rate` | `files_with_retries / total_files` |
| `retry_distribution` | `{"1": N, "2": N, ...}` — how many files had each retry count |

### Concurrency

Estimated from file `start_time`/`finish_time` timestamps using 1-second buckets.

| Field | Description |
|---|---|
| `peak_concurrency` | Maximum simultaneously active transfers |
| `mean_concurrency` | Mean active transfers per second |
| `concurrency_timeline` | `[{"t": epoch, "active": N}]` per-second timeline |

### Failure reasons

`failure_reasons` is a dict mapping the raw FTS3 reason string to a count. Empty reasons appear as `"UNKNOWN"`.

---

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Campaign completed and `threshold_passed = true` |
| `1` | Campaign completed but `threshold_passed = false`, or campaign raised an unhandled exception |

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

**615 unit tests** cover all modules: config loader, inventory, destination planner, checksum fetcher, FTS client, submission (including 500-recovery), poller, collector, persistence, resume controller, metrics engine, cleanup manager, reporting renderer, and runner orchestration.

---

## Architecture Overview

```
runner.py  (top-level orchestration)
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
