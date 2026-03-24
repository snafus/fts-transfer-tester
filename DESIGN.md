# FTS3 REST Test Framework — Architecture and Module Design

**Python 3.6.8 | Standard-library-first | REST-native | Reproducibility-first**

---

## 1. Executive Summary

This document specifies a production-grade, automation-first FTS3 REST transfer test framework implemented in Python 3.6.8. The framework drives bulk file transfer campaigns against FTS3 endpoints via REST exclusively, harvests file-level metrics as the authoritative source of truth, and persists all raw and normalised data for reproducible offline analysis.

The design is organised into twelve discrete modules with explicit interfaces, a deterministic data model, and a persistence layout that supports full campaign resumption after interruption. All design decisions are concrete and implementation-ready; no section requires further architectural elaboration before coding begins.

Core philosophy: **raw data first, transformation second**. Every REST response is written to disk before any processing occurs. Reports can always be regenerated from persisted data without re-contacting FTS3.

---

## 2. Scope and Non-Goals

### In Scope

- Submitting FTS3 transfer jobs via `POST /jobs` (REST only, no CLI)
- Polling job and file state via `GET /jobs/{id}`, `GET /jobs/{id}/files`, `GET /jobs/{id}/files/{fid}/retries`, `GET /jobs/{id}/dm`
- Pre-submission ADLER32 checksum acquisition via WebDAV `Want-Digest`
- Pre/post campaign cleanup via WebDAV HTTP DELETE
- File-level metric computation: throughput, duration, success rate, retry distributions, latency percentiles, concurrency estimates
- Deterministic destination path mapping with run manifest persistence
- Campaign resumption after interruption using `run_id` embedded in FTS3 job metadata
- Framework-level retry of FTS3-failed files (configurable, default off)
- SSL verification control: `true | false | <ca_bundle_path>`
- Console, JSON, and Markdown reports; optional HTML
- Cross-run comparison
- Python 3.6.8 compatibility throughout

### Not In Scope

- FTS3 CLI interaction
- Tape/staging workflows (`STAGING` state treated as unsupported error)
- XRootD protocol operations (checksum fetch, cleanup)
- Link-level FTS3 optimizer configuration
- Token refresh (delegated to FTS3 server)
- dCache, SRM, or gsiftp endpoints
- Real-time streaming metrics
- Multi-protocol checksum algorithms other than ADLER32

### Boundaries

The framework is a **test and benchmarking client**. It does not manage FTS3 server configuration, storage quotas, or AAI policy. It consumes tokens as opaque strings and does not perform OIDC flows.

---

## 3. Python 3.6.8 Compatibility Strategy

### Language Features

| Feature | Status | Notes |
|---|---|---|
| f-strings | Allowed | Introduced 3.6 |
| `typing` module | Allowed | Use `Dict`, `List`, `Tuple`, `Optional`, `Any` from `typing` |
| `dataclasses` | **Forbidden** | Introduced 3.7; use `collections.namedtuple` or plain dicts |
| Postponed annotations (`from __future__ import annotations`) | **Forbidden** | 3.7+ |
| `dict` / `list` as generic type hints (`dict[str, int]`) | **Forbidden** | 3.9+; use `typing.Dict[str, int]` |
| `asyncio.run()` | **Forbidden** | 3.7+; use `loop.run_until_complete()` if async needed |
| Walrus operator `:=` | **Forbidden** | 3.8+ |
| `pathlib.Path` | Allowed | Available 3.4+ |
| `concurrent.futures.ThreadPoolExecutor` | Allowed | Used for parallel Want-Digest fetches |

### Dependency Pinning

```
requests==2.27.1
urllib3==1.26.18
PyYAML==5.4.1
certifi==2021.10.8
```

Testing only (not in production requirements):
```
pytest==4.6.11
responses==0.13.4
freezegun==1.1.0
```

No numpy, pandas, or dataclasses backport required. All metrics use `statistics` (stdlib) and list comprehensions.

### Typing Strategy

All public function signatures carry type annotations using `typing` module types. Internal helpers may omit annotations where trivial. No `TypedDict` (3.8+); use plain `dict` with inline comments for structured dicts.

### Packaging

- `setup.py` (not `pyproject.toml` — requires pip 19+ which may not be available on Python 3.6 targets)
- `requirements.txt` with pinned versions
- `tox.ini` with `py36` environment
- No namespace packages; explicit `__init__.py` in every module

---

## 4. Assumptions

| Assumption | Detail |
|---|---|
| Authentication | OIDC Bearer JWT tokens provided by caller. FTS3 server handles token refresh and validation. Three token roles (`fts_submit`, `source_read`, `dest_write`) may share the same value in single-IAM deployments. |
| PFN accessibility | All source PFNs are reachable via HTTPS/WebDAV from the machine running the framework. `Want-Digest` HEAD requests succeed or return a retryable error. |
| Destination semantics | Destination prefix is an HTTPS/WebDAV base URL. Destination directory structure will be created implicitly by the storage on first write (standard WebDAV behaviour). |
| Overwrite behaviour | Controlled by `overwrite` flag in FTS3 job params. If `false`, pre-existing files at destination cause file-level failure. |
| Checksum behaviour | FTS3 performs ADLER32 verification at both source and destination (`verify_checksum: both`). Source checksum is supplied by the framework from a pre-submission Want-Digest call. |
| Cleanup permissions | The `dest_write` token has DELETE permission on the destination prefix. Cleanup failures are logged but do not abort the campaign. |
| API stability | FTS3 REST API as of FTS 3.11+. WLCG JWT token auth supported server-side. |
| FTS3 server version | 3.11 or later. Required for full JWT token support and `job_metadata` field. |
| Tape/staging | No tape-backed endpoints. `STAGING` state is treated as an unsupported error. |
| Network | WAN transfers; latency and throughput metrics are meaningful at file sizes > 1 MB. |

---

## 5. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        runner.py                             │
│                  (top-level orchestration)                   │
└───────────────┬─────────────────────────────────────────────┘
                │
    ┌───────────▼──────────────────────────────────────────┐
    │               Execution Pipeline                      │
    │                                                       │
    │  1. ConfigLoader ──► Config object                    │
    │  2. InventoryLoader ──► [SourcePFN]                   │
    │  3. DestinationPlanner ──► {pfn: dest_url} manifest   │
    │  4. ChecksumFetcher ──► {pfn: "adler32:hex"}          │
    │  5. CleanupManager (pre) ──► DELETE dest files        │
    │  6. SubmissionEngine ──► [SubjobRecord]               │
    │  7. PollingEngine ──► terminal job states             │
    │  8. HarvestCollector ──► file/retry/dm records        │
    │  9. MetricsEngine ──► MetricsSnapshot                 │
    │  10. PersistenceLayer ──► runs/<run_id>/              │
    │  11. ReportingLayer ──► console/json/markdown         │
    │  12. CleanupManager (post) ──► DELETE dest files      │
    └───────────────────────────────────────────────────────┘

    ┌──────────────┐     ┌──────────────────────────────────┐
    │  FTSClient   │────►│  FTS3 REST API                   │
    │  (requests   │     │  POST /jobs                      │
    │   session)   │     │  GET  /jobs/{id}                 │
    └──────────────┘     │  GET  /jobs/{id}/files           │
                         │  GET  /jobs/{id}/files/{fid}/    │
                         │       retries                    │
                         │  GET  /jobs/{id}/dm              │
                         │  GET  /whoami                    │
                         │  GET  /optimizer/current         │
                         └──────────────────────────────────┘

    ┌──────────────┐     ┌──────────────────────────────────┐
    │  WebDAVClient│────►│  Storage endpoints               │
    │  (requests)  │     │  HEAD (Want-Digest)              │
    └──────────────┘     │  DELETE (cleanup)                │
                         └──────────────────────────────────┘

    ┌─────────────────────────────────────────────────────┐
    │  ResumeController                                    │
    │  On startup: scan runs/<run_id>/manifest.json        │
    │  Query GET /jobs?job_id=... for active subjobs        │
    │  Re-attach poller to in-flight jobs                  │
    └─────────────────────────────────────────────────────┘
```

### Module Responsibilities

| Module | Responsibility | Inputs | Outputs |
|---|---|---|---|
| `config.loader` | Load and validate YAML config | config YAML path | `Config` dict |
| `inventory.loader` | Load and validate PFN list | PFN file path | `List[str]` |
| `destination.planner` | Compute deterministic dest mapping | config, pfn list | `OrderedDict[str, str]`, manifest entry |
| `checksum.fetcher` | HEAD Want-Digest per PFN, parallel | pfn list, token, ssl config | `Dict[str, str]` (pfn→"adler32:hex") |
| `cleanup.manager` | WebDAV DELETE pre/post | dest urls, token, ssl config | audit log |
| `fts.client` | Authenticated requests session | config | `requests.Session` |
| `fts.submission` | Chunk + submit FTS3 jobs | mapping, checksums, config | `List[SubjobRecord]` |
| `fts.poller` | Poll jobs to terminal state | subjob list, client | updated `SubjobRecord` list |
| `fts.collector` | Harvest file/retry/DM records | terminal subjob list, client | raw JSON, `List[FileRecord]` |
| `metrics.engine` | Compute all metrics | `List[FileRecord]`, config | `MetricsSnapshot` |
| `persistence.store` | Write raw/normalised/manifest | all records | `runs/<run_id>/` tree |
| `reporting.*` | Render reports | `MetricsSnapshot`, manifest | console/json/md/html |
| `resume.controller` | Detect and re-attach prior runs | run_id, manifest | resumed subjob list |

### Error Handling Per Module

Each module raises typed exceptions from `fts_framework.exceptions`:

- `ConfigError` — malformed or missing config values
- `InventoryError` — unreadable or empty PFN file
- `ChecksumFetchError` — Want-Digest failed after retries
- `SubmissionError` — FTS3 POST /jobs returned non-2xx and recovery scan found no job
- `PollingTimeoutError` — campaign_timeout_s exceeded
- `TokenExpiredError` — FTS3 returned HTTP 401; operator must re-acquire token
- `PersistenceError` — disk write failure
- `CleanupError` — DELETE failed (logged, not fatal by default)
- `ResumeError` — manifest missing or corrupt; run cannot be resumed
- `_TransientHTTPError` — internal; signals retryable HTTP status to the retry wrapper

---

## 6. Data Model

All records are plain Python dicts with defined keys. Field names map directly to FTS3 REST response fields where applicable.

### Config

```python
{
    "run": {
        "run_id": str,           # e.g. "20260323_143201_a3f7"
        "test_label": str        # e.g. "campaign_march_2026"
    },
    "fts": {
        "endpoint": str,         # "https://fts.example.org:8446"
        "ssl_verify": ...        # True | False | "/path/to/ca.pem"
    },
    "tokens": {
        "fts_submit": str,
        "source_read": str,
        "dest_write": str
    },
    "transfer": {
        "source_pfns_file": str,
        "dst_prefix": str,
        "preserve_extension": bool,   # default False
        "checksum_algorithm": str,    # "adler32"
        "verify_checksum": str,       # "both"
        "overwrite": bool,
        "chunk_size": int,            # default 200, max 200
        "priority": int,              # 1-5, default 3
        "activity": str,              # default "default"
        "job_metadata": dict
    },
    "concurrency": {
        "want_digest_workers": int    # default 8
    },
    "submission": {
        "scan_window_s": int          # default 300 (5 min); minimum 60; 500-recovery job scan window
    },
    "polling": {
        "initial_interval_s": int,    # default 30
        "backoff_multiplier": float,  # default 1.5
        "max_interval_s": int,        # default 300
        "campaign_timeout_s": int     # default 86400
    },
    "cleanup": {
        "before": bool,
        "after": bool
    },
    "retry": {
        "fts_retry_max": int,             # FTS3-level, default 2
        "framework_retry_max": int,       # framework-level, default 0
        "min_success_threshold": float    # default 0.95
    },
    "output": {
        "base_dir": str,
        "reports": {
            "console": bool,
            "json": bool,
            "markdown": bool,
            "html": bool
        }
    }
}
```

### SubjobRecord

```python
{
    "job_id": str,
    "chunk_index": int,
    "run_id": str,
    "retry_round": int,          # 0 = initial submission
    "submitted_at": str,         # ISO8601
    "file_count": int,
    "status": str,               # FTS3 job state
    "terminal": bool,
    "payload_path": str          # path to persisted POST body
}
```

### FileRecord

Maps directly to FTS3 `/jobs/{id}/files` response fields:

```python
{
    # Identity
    "job_id": str,
    "file_id": int,
    "run_id": str,
    "chunk_index": int,
    "retry_round": int,

    # Transfer addresses
    "source_surl": str,
    "dest_surl": str,

    # State
    "file_state": str,           # FINISHED | FAILED | CANCELED | NOT_USED | STAGING
    "reason": str,               # FTS3 failure reason string

    # Timestamps (ISO8601 strings as returned by FTS3)
    "start_time": str,
    "finish_time": str,
    "staging_start": None,       # reserved for future tape support
    "staging_finished": None,

    # Transfer metrics (from FTS3 record)
    "filesize": int,             # bytes
    "tx_duration": float,        # seconds (wire transfer only)
    "throughput": float,         # bytes/sec, agent-reported (primary)

    # Computed metrics (added by MetricsEngine, not from FTS3)
    "throughput_wire": float,        # filesize / tx_duration
    "throughput_wall": float,        # filesize / (finish - start)
    "wall_duration_s": float,        # finish_time - start_time

    # Checksum
    "checksum": str,             # "adler32:hex" as submitted

    # Metadata
    "job_metadata": dict,
    "file_metadata": dict
}
```

### RetryRecord

```python
{
    "job_id": str,
    "file_id": int,
    "attempt": int,
    "datetime": str,
    "reason": str,
    "transfer_host": str
}
```

### DMRecord

```python
{
    "job_id": str,
    "file_id": int,
    "dm_state": str,
    "start_time": str,
    "finish_time": str,
    "reason": str
}
```

### MetricsSnapshot

```python
{
    "run_id": str,
    "test_label": str,
    "generated_at": str,

    # Counts
    "total_files": int,
    "finished": int,
    "failed": int,
    "canceled": int,
    "not_used": int,
    "staging_unsupported": int,

    # Rates
    "success_rate": float,       # finished / (total - not_used)
    "failure_rate": float,
    "threshold_passed": bool,    # success_rate >= min_success_threshold

    # Throughput (bytes/sec) — primary source
    "throughput_mean": float,
    "throughput_p50": float,
    "throughput_p90": float,
    "throughput_p95": float,
    "throughput_p99": float,
    "throughput_max": float,
    "aggregate_throughput_bytes_per_s": float,

    # Duration (seconds)
    "duration_mean_s": float,
    "duration_p50_s": float,
    "duration_p90_s": float,
    "duration_p95_s": float,

    # Retries
    "total_retries": int,
    "files_with_retries": int,
    "retry_rate": float,         # files_with_retries / total_files
    "retry_distribution": dict,  # {"0": N, "1": N, "2": N, ...}

    # Concurrency
    "peak_concurrency": int,
    "mean_concurrency": float,
    "concurrency_timeline": list,  # [{"t": epoch, "active": N}]

    # Failures
    "failure_reasons": dict,     # {reason_category: count}

    # Throughput timeline
    "throughput_timeline": list, # [{"t": epoch, "bytes_per_s": N}]

    # SSL warning
    "ssl_verify_disabled": bool
}
```

### TimelineBucket

```python
{
    "t": int,               # Unix epoch, start of bucket
    "active": int,          # files active during this second
    "bytes_per_s": float    # sum(filesize) for files finishing in bucket / bucket_width
}
```

### Error Taxonomy

FTS3 `reason` strings are free text. Categorise by substring matching:

| Category | Pattern match |
|---|---|
| `SOURCE_ERROR` | `"SOURCE"` or `"source"` in reason |
| `DESTINATION_ERROR` | `"DESTINATION"` or `"destination"` in reason |
| `TRANSFER_ERROR` | `"TRANSFER"` or `"transfer"` in reason |
| `TIMEOUT` | `"timeout"` or `"timed out"` in reason |
| `PERMISSION` | `"permission"` or `"403"` in reason |
| `NOT_FOUND` | `"not found"` or `"404"` or `"no such"` in reason |
| `CHECKSUM_MISMATCH` | `"checksum"` in reason |
| `CANCELED` | file_state == `CANCELED` |
| `STAGING_UNSUPPORTED` | file_state == `STAGING` |
| `UNKNOWN` | no pattern matched |

---

## 7. Submission Design

### Destination Mapping Algorithm

```python
def compute_destination_mapping(pfns, dst_prefix, test_label, preserve_extension):
    # type: (List[str], str, str, bool) -> OrderedDict
    sorted_pfns = sorted(pfns)           # deterministic ordering
    mapping = OrderedDict()
    for i, pfn in enumerate(sorted_pfns):
        if preserve_extension:
            ext = "." + pfn.rsplit(".", 1)[-1] if "." in pfn.rsplit("/", 1)[-1] else ""
        else:
            ext = ""
        dest = "{}/{}/testfile_{:06d}{}".format(dst_prefix, test_label, i, ext)
        mapping[pfn] = dest
    return mapping
```

The mapping is written to `runs/<run_id>/manifest.json` immediately after computation and before any network activity.

### Chunking

```python
def chunk(items, size=200):
    # type: (List, int) -> List[List]
    return [items[i:i + size] for i in range(0, len(items), size)]
```

Chunks are indexed globally (`chunk_index` 0-based). The last chunk may be smaller than `size`.

### Payload Construction

```python
def build_payload(chunk_mapping, checksums, config, run_id, chunk_index, retry_round):
    # type: (OrderedDict, Dict[str, str], dict, str, int, int) -> dict
    files = []
    for src, dst in chunk_mapping.items():
        files.append({
            "sources": [src],
            "destinations": [dst],
            "checksum": checksums.get(src, ""),
            "filesize": 0,         # FTS3 will determine if not known
            "metadata": {"pfn": src}
        })

    return {
        "files": files,
        "params": {
            "verify_checksum": config["transfer"]["verify_checksum"],
            "reuse": False,
            "bring_online": -1,        # tape disabled
            "copy_pin_lifetime": -1,
            "job_metadata": _build_job_metadata(config, run_id, chunk_index, retry_round),
            "priority": config["transfer"]["priority"],   # 1–5; default 3
            "strict_copy": False,
            "overwrite": config["transfer"]["overwrite"],
            "activity": config["transfer"]["activity"],   # FTS3 activity share label
            "retry": config["retry"]["fts_retry_max"]
        }
    }
```

### Job Metadata Construction

Framework-reserved keys are merged with user-supplied metadata from config. User keys never overwrite framework keys.

```python
_FRAMEWORK_METADATA_KEYS = {"run_id", "chunk_index", "retry_round", "test_label"}

def _build_job_metadata(config, run_id, chunk_index, retry_round):
    # type: (dict, str, int, int) -> dict
    user_meta = config["transfer"].get("job_metadata") or {}
    merged = dict(user_meta)                    # user fields first
    merged.update({                             # framework fields always win
        "run_id": run_id,
        "chunk_index": chunk_index,
        "retry_round": retry_round,
        "test_label": config["run"]["test_label"]
    })
    return merged
```

`activity` and `priority` are top-level FTS3 params (not inside `job_metadata`) and passed directly from config. Both are configurable per campaign run.

### Submission Loop

```
for chunk_index, chunk_pairs in enumerate(chunks):
    payload = build_payload(chunk_pairs, checksums, config, run_id, chunk_index, retry_round=0)
    persist payload → runs/<run_id>/submitted_payloads/chunk_{chunk_index:04d}_r0.json
    response = fts_client.post("/jobs", payload)
    if response.status_code != 200:
        raise SubmissionError(chunk_index, response.status_code, response.text)
    job_id = response.json()["job_id"]
    record SubjobRecord and append to manifest
```

Payloads are persisted **before** the POST. If the process crashes between persist and POST, the resume controller detects the un-submitted payload (no matching `job_id` in manifest) and resubmits.

---

## 8. Cleanup Design (WebDAV DELETE)

Cleanup uses direct HTTP DELETE against destination storage endpoints. FTS3 is not involved.

### Client

`cleanup.manager` uses a dedicated `requests.Session` authenticated with `dest_write` token and the same SSL config as the main client.

### Pre-Cleanup Algorithm

```
if config.cleanup.before:
    for dest_url in all_destination_urls:
        response = webdav_session.delete(dest_url)
        if response.status_code in (200, 204, 404):
            log OK (404 = already absent, idempotent)
        else:
            log WARNING and continue   # never abort on cleanup failure
    write audit log → runs/<run_id>/cleanup_pre.json
```

### Post-Cleanup Algorithm

```
if config.cleanup.after:
    # Only delete files that were successfully transferred
    successful_dests = [f["dest_surl"] for f in file_records if f["file_state"] == "FINISHED"]
    for dest_url in successful_dests:
        response = webdav_session.delete(dest_url)
        log result
    write audit log → runs/<run_id>/cleanup_post.json
```

### Safety Model

- 404 is always treated as success (idempotent)
- DELETE failures are logged but never raise exceptions or abort the campaign
- Cleanup audit logs are written regardless of outcome
- The destination prefix itself is never deleted — only individual file URLs

---

## 9. Polling and State Harvesting

### FTS3 Job State Machine

```
SUBMITTED → READY → ACTIVE → FINISHED       (all files ok)
                           → FAILED          (all files failed)
                           → FINISHEDDIRTY   (mixed outcome)
                           → CANCELED        (aborted)
                    ACTIVE → STAGING         (unsupported: log + treat as error)
```

Terminal states: `FINISHED`, `FAILED`, `FINISHEDDIRTY`, `CANCELED`

### Polling Loop

```python
def poll_to_completion(subjobs, fts_client, config):
    interval = config["polling"]["initial_interval_s"]
    deadline = time.time() + config["polling"]["campaign_timeout_s"]
    active = {s["job_id"]: s for s in subjobs if not s["terminal"]}

    while active:
        if time.time() > deadline:
            raise PollingTimeoutError(list(active.keys()))

        time.sleep(interval)

        for job_id in list(active.keys()):
            resp = fts_client.get("/jobs/{}".format(job_id))
            state = resp.json()["job_state"]
            persist_raw(resp.json(), "jobs", job_id)

            if state in TERMINAL_STATES:
                active[job_id]["status"] = state
                active[job_id]["terminal"] = True
                del active[job_id]
            elif state == "STAGING":
                log_warning("STAGING observed for job {} — unsupported".format(job_id))
                active[job_id]["status"] = "STAGING_UNSUPPORTED"
                active[job_id]["terminal"] = True
                del active[job_id]

        interval = min(interval * config["polling"]["backoff_multiplier"],
                       config["polling"]["max_interval_s"])
```

### Harvest Sequence (per terminal job)

```
1. GET /jobs/{id}/files          → persist raw → normalise to FileRecord list
2. GET /jobs/{id}/files/{fid}/retries  (for each file_id) → persist raw → RetryRecord list
3. GET /jobs/{id}/dm             → persist raw → DMRecord list
4. GET /jobs/{id}                → already persisted during polling; read from cache
```

File records from step 1 are the **authoritative source** for all metrics. Job-level data is supplementary.

### REST Call → Use Case Mapping

| Endpoint | Use case |
|---|---|
| `POST /jobs` | Submit new transfer job |
| `GET /jobs/{id}` | Poll job terminal state |
| `GET /jobs/{id}/files` | Harvest per-file outcomes (authoritative) |
| `GET /jobs/{id}/files/{fid}/retries` | Harvest per-file retry history |
| `GET /jobs/{id}/dm` | Harvest data-management records |
| `GET /whoami` | Validate token identity at campaign start |
| `GET /optimizer/current` | Log link optimizer state at campaign start |

### Snapshot Persistence

Every poll response is written to disk immediately:
```
runs/<run_id>/raw/jobs/<job_id>_poll_<N>.json
runs/<run_id>/raw/files/<job_id>.json
runs/<run_id>/raw/retries/<job_id>_<file_id>.json
runs/<run_id>/raw/dm/<job_id>.json
```

### Resume Logic

On startup with an existing `run_id`:

```
1. Load manifest.json
2. For each SubjobRecord where terminal=False:
   a. GET /jobs/{job_id}
   b. If terminal state → harvest immediately
   c. If non-terminal → add to active poll set
3. For each chunk with no job_id (crash between persist and POST):
   a. Resubmit payload from submitted_payloads/
4. Continue polling as normal
```

---

## 10. Metrics Methodology

### Success and Failure Rates

```
eligible = total_files - not_used_count - staging_unsupported_count
success_rate = finished_count / eligible      (if eligible > 0 else 0.0)
failure_rate = (failed_count + canceled_count) / eligible
```

`FINISHEDDIRTY` jobs contribute their individual file outcomes. The job state itself is not used for rate calculations — only file states.

`threshold_passed = success_rate >= config["retry"]["min_success_threshold"]`

### Per-File Throughput

```python
# Primary (agent-reported, from FTS3 file record)
throughput_primary = file["throughput"]      # bytes/sec, None if absent

# Wire (from tx_duration)
if file["tx_duration"] and file["tx_duration"] > 0 and file["filesize"] > 0:
    throughput_wire = file["filesize"] / file["tx_duration"]
else:
    throughput_wire = None

# Wall (from timestamps)
wall = (parse_iso(file["finish_time"]) - parse_iso(file["start_time"])).total_seconds()
if wall > 0 and file["filesize"] > 0:
    throughput_wall = file["filesize"] / wall
else:
    throughput_wall = None
```

All three values stored in `FileRecord`. `throughput_primary` drives aggregate statistics; `throughput_wire` used as fallback if primary is None.

### Aggregate Throughput

```
# Sum of bytes transferred / total campaign wall time
campaign_start = min(f["start_time"] for f in finished_files)
campaign_end   = max(f["finish_time"] for f in finished_files)
total_bytes    = sum(f["filesize"] for f in finished_files)
aggregate_throughput = total_bytes / (campaign_end - campaign_start)
```

### Concurrency Estimation

```python
def estimate_concurrency(file_records, bucket_width_s=1):
    # type: (List[dict], int) -> List[dict]
    finished = [f for f in file_records if f["start_time"] and f["finish_time"]]
    if not finished:
        return []
    t_min = min(parse_epoch(f["start_time"]) for f in finished)
    t_max = max(parse_epoch(f["finish_time"]) for f in finished)
    buckets = []
    t = t_min
    while t <= t_max:
        active = sum(
            1 for f in finished
            if parse_epoch(f["start_time"]) <= t < parse_epoch(f["finish_time"])
        )
        buckets.append({"t": int(t), "active": active})
        t += bucket_width_s
    return buckets
```

### Latency Percentiles

Computed with `statistics.median` and a simple percentile function (stdlib — no numpy):

```python
def percentile(data, p):
    # type: (List[float], float) -> float
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(sorted_data) - 1)
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * (k - lo)
```

Percentiles computed at p50, p90, p95, p99 for: per-file throughput, per-file wall duration.

### Edge Case Handling

| Case | Handling |
|---|---|
| `throughput` is None or 0 | Use `throughput_wire`; if also None, exclude file from throughput stats |
| `tx_duration` is 0 | `throughput_wire = None` |
| `filesize` is 0 | Exclude from throughput; count in success/failure rate normally |
| `start_time` or `finish_time` missing | Exclude from duration and throughput; count in success/failure rate |
| All files excluded from throughput | Report `throughput_mean = None`; do not error |
| Retry record has empty reason | `reason = "UNKNOWN"` |
| `FINISHEDDIRTY` job | Process all file records individually; no special job-level handling |

---

## 11. Storage and Reproducibility

### Directory Structure

```
runs/
└── <run_id>/
    ├── manifest.json               # run identity, subjob list, dest mapping, state
    ├── config.yaml                 # copy of config used for this run (tokens redacted)
    ├── submitted_payloads/
    │   ├── chunk_0000_r0.json      # exact POST body for chunk 0, retry round 0
    │   ├── chunk_0001_r0.json
    │   └── chunk_0000_r1.json      # framework retry round 1 (if applicable)
    ├── raw/
    │   ├── jobs/
    │   │   ├── <job_id>_poll_0.json
    │   │   └── <job_id>_poll_N.json
    │   ├── files/
    │   │   └── <job_id>.json
    │   ├── retries/
    │   │   └── <job_id>_<file_id>.json
    │   └── dm/
    │       └── <job_id>.json
    ├── normalized/
    │   ├── file_records.json       # List[FileRecord]
    │   ├── retry_records.json      # List[RetryRecord]
    │   └── dm_records.json         # List[DMRecord]
    ├── metrics/
    │   └── snapshot.json           # MetricsSnapshot
    ├── cleanup_pre.json            # audit log (if cleanup.before)
    ├── cleanup_post.json           # audit log (if cleanup.after)
    └── reports/
        ├── summary.json
        ├── report.md
        └── report.html             # optional
```

### manifest.json Schema

```json
{
    "run_id": "20260323_143201_a3f7",
    "test_label": "campaign_march_2026",
    "created_at": "2026-03-23T14:32:01Z",
    "config_hash": "sha256:...",
    "fts_endpoint": "https://fts.example.org:8446",
    "fts_monitor_base": "https://fts.example.org:8449/fts3/ftsmon/#/job/",
    "ssl_verify_disabled": false,
    "destination_mapping": {"<src_pfn>": "<dest_url>", "...": "..."},
    "subjobs": [
        {
            "job_id": "abc123",
            "chunk_index": 0,
            "retry_round": 0,
            "submitted_at": "...",
            "file_count": 200,
            "status": "FINISHED",
            "terminal": true,
            "payload_path": "submitted_payloads/chunk_0000_r0.json",
            "fts_monitor_url": "https://fts.example.org:8449/fts3/ftsmon/#/job/abc123"
        }
    ],
    "completed": false
}
```

### Reproducibility Guarantees

1. **Destination mapping is fixed** at planning time and persisted. A rerun of the same `run_id` uses identical destinations.
2. **All submitted payloads** are persisted before transmission. Any job can be manually resubmitted from disk.
3. **All raw REST responses** are persisted before normalisation. Reports can be regenerated with `python -m fts_framework report --run-id <id>` without any network access.
4. **Config is copied** (tokens redacted) into the run directory. The exact config for any historical run is recoverable.

---

## 12. SSL Verification Design

### Config Schema

```yaml
fts:
  ssl_verify: true          # true | false | "/etc/pki/tls/certs/ca-bundle.crt"
```

### Session Setup

```python
def build_session(token, ssl_verify):
    # type: (str, Union[bool, str]) -> requests.Session
    session = requests.Session()
    session.headers.update({"Authorization": "Bearer {}".format(token)})
    session.verify = ssl_verify
    if ssl_verify is False:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        logger.warning("SSL verification DISABLED. This session is insecure.")
    elif isinstance(ssl_verify, str):
        logger.info("SSL verification using CA bundle: %s", ssl_verify)
    else:
        logger.info("SSL verification enabled (system default)")
    return session
```

### Report Annotations

When `ssl_verify = false`:
- `MetricsSnapshot.ssl_verify_disabled = True`
- All report formats include a prominent warning block:
  ```
  ⚠ WARNING: SSL certificate verification was DISABLED for this run.
    Results should not be used as security-validated production benchmarks.
  ```

### Warning Suppression

`urllib3.disable_warnings` is called only when `ssl_verify is False`. It is scoped to the session setup, not globally. The log entry is always written regardless.

---

## 13. Security and Secrets

### Token Handling

- Tokens are read from config YAML at startup and stored in memory only
- Tokens are never written to disk, logs, or report files
- `config.yaml` copied to run directory has token values replaced with `"<REDACTED>"`
- Token values are never included in `manifest.json`, `submitted_payloads/`, or any report

### Log Redaction

```python
def redact(value):
    # type: (str) -> str
    if len(value) > 16:
        return value[:8] + "..." + value[-4:]
    return "<REDACTED>"
```

Applied to any log line that might include Authorization header content.

### Manifest Hygiene

- `manifest.json` contains no credentials
- `submitted_payloads/` contain no tokens (FTS3 job payload does not include tokens)
- File permissions on run directory: `0o700` (owner only)

### Audit Metadata

Each `manifest.json` records:
- `fts_endpoint`
- `ssl_verify_disabled` flag
- `created_at` timestamp
- `config_hash` (SHA-256 of the redacted config, for reproducibility verification)

---

## 14. Failure Handling and Recovery

### Network / HTTP Errors

All FTS3 REST calls use a retry wrapper with exponential backoff:

```python
def fts_request_with_retry(session, method, url, max_retries=3, **kwargs):
    backoff = 5
    for attempt in range(max_retries):
        try:
            resp = session.request(method, url, timeout=30, **kwargs)
            if resp.status_code in (429, 502, 503, 504):
                raise _TransientHTTPError(resp.status_code)
            return resp
        except (requests.ConnectionError, requests.Timeout, _TransientHTTPError) as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff)
            backoff *= 2
```

### Token Expiry

Token expiry is managed by FTS3. If FTS3 returns `401` during polling, the framework:
1. Logs the error with timestamp
2. Raises `TokenExpiredError` with the job_id
3. Halts the campaign (does not silently skip)

The operator must re-acquire a token and resume the run.

### 500 Response on Job Submission (Job May Exist)

FTS3 may accept and persist a job internally but return HTTP 500 with no `job_id`. The framework must not blindly resubmit — that would create a duplicate job.

**Recovery flow:**

```python
def submit_with_500_recovery(fts_client, payload, config, run_id, chunk_index, retry_round):
    response = fts_client.post("/jobs", payload)

    if response.status_code == 200:
        return response.json()["job_id"]

    if response.status_code == 500:
        logger.warning("POST /jobs returned 500 for chunk %d — scanning for existing job", chunk_index)
        time.sleep(5)   # allow FTS3 DB to settle

        scan_window_h = config["submission"]["scan_window_s"] / 3600.0
        jobs = fts_client.get(
            "/jobs",
            params={"time_window": scan_window_h, "state_in": "SUBMITTED,READY,ACTIVE,FINISHED,FAILED,FINISHEDDIRTY"}
        ).json()

        matches = [
            j for j in jobs
            if isinstance(j.get("job_metadata"), dict)
            and j["job_metadata"].get("run_id") == run_id
            and j["job_metadata"].get("chunk_index") == chunk_index
            and j["job_metadata"].get("retry_round") == retry_round
        ]

        if len(matches) == 1:
            job_id = matches[0]["job_id"]
            logger.warning("Recovered job_id %s from metadata scan after 500", job_id)
            return job_id

        if len(matches) > 1:
            # take most recently submitted
            matches.sort(key=lambda j: j.get("submit_time", ""), reverse=True)
            job_id = matches[0]["job_id"]
            logger.warning("Multiple matches on metadata scan — using most recent: %s", job_id)
            return job_id

        # no match — job was not created
        raise SubmissionError(chunk_index, 500, "500 and no matching job found in scan window")

    raise SubmissionError(chunk_index, response.status_code, response.text)
```

`scan_window_s` (default 300s / 5 min) is configurable. The `(run_id, chunk_index, retry_round)` triple is unique per submission attempt, preventing false matches across campaigns.

### Partial Submission

If the process crashes after submitting N of M chunks:
- `manifest.json` records which chunks have `job_id` entries
- Resume controller detects the gap and resubmits missing chunks
- Duplicate submission prevention: before resubmitting, query `GET /jobs?job_metadata.run_id=<run_id>&job_metadata.chunk_index=<N>` to check if a job already exists

### Polling Failures

A single poll failure does not abort the campaign. After `max_retries` consecutive failures for a single job, the job is marked `POLL_ERROR` and excluded from metrics with a warning.

### Local Corruption

If `manifest.json` is unreadable or missing, the run cannot be resumed. The operator must start a new run. The framework never overwrites an existing run directory for a different `run_id`.

### Framework-Level Retry

When `framework_retry_max > 0` and enabled:

```
after all initial subjobs reach terminal state:
    failed_files = [f for f in file_records if f["file_state"] in ("FAILED", "CANCELED")]
    if failed_files and retry_round < framework_retry_max:
        build new chunk mapping for failed_files (same dest_url from manifest)
        fetch checksums for failed source PFNs
        submit new FTS3 jobs with retry_round incremented
        poll and harvest as normal
        merge results into file_records (latest outcome per source PFN wins)
```

---

## 15. Reporting Design

### Console Summary

Printed to stdout at campaign completion:

```
═══════════════════════════════════════════════════
 FTS3 Test Framework — Run Summary
 Run ID   : 20260323_143201_a3f7
 Label    : campaign_march_2026
 Endpoint : https://fts.example.org:8446
═══════════════════════════════════════════════════
 Files total       : 1000
 Finished          : 987
 Failed            : 11
 Canceled          : 2
 Not used          : 0
 Success rate      : 98.70%  [PASS ≥ 95.00%]

 Throughput (agent-reported, bytes/sec)
   Mean    : 450.2 MB/s
   p50     : 461.1 MB/s
   p90     : 512.3 MB/s
   p95     : 521.0 MB/s

 Duration (wall, seconds)
   Mean    : 4.2s    p50: 3.9s    p90: 7.1s

 Peak concurrency  : 42 files
 Total retries     : 18 (5 files)
 Aggregate throughput: 38.2 GB/s

 SSL verify        : ENABLED
═══════════════════════════════════════════════════
```

### JSON Summary (`reports/summary.json`)

Full `MetricsSnapshot` dict serialised as JSON. Machine-readable for CI integration and cross-run comparison.

### Markdown Report (`reports/report.md`)

Structured sections:
1. Run metadata
2. Transfer outcomes table
3. Throughput statistics table
4. Duration statistics table
5. Concurrency chart (ASCII or data table)
6. Retry distribution table
7. Failure reasons breakdown
8. Per-subjob summary table (job_id, file count, state, FTS monitor URL)
9. SSL warning (if applicable)

### Optional HTML Report (`reports/report.html`)

Markdown report converted to HTML using stdlib `html` module and a minimal inline CSS template. No external dependencies.

### Cross-Run Comparison

```
python -m fts_framework compare --run-ids <id1> <id2> [<id3>...]
```

Loads `metrics/snapshot.json` from each run directory and produces a side-by-side comparison table (console + JSON).

---

## 16. Testing Strategy

### Unit Tests

| Component | Test approach |
|---|---|
| `destination.planner` | Assert deterministic mapping for fixed input; test extension handling |
| `checksum.fetcher` | Test hex detection, base64 decode path, hard-fail path |
| `fts.submission` | Test chunking at boundary (200, 201, 400 files); test payload field mapping |
| `metrics.engine` | Test each formula with synthetic file records; test all edge cases from §10 |
| `config.loader` | Test required field validation; test ssl_verify parsing |
| `persistence.store` | Test directory structure creation; test token redaction in config copy |

### REST Mocking

Use `responses` library (compatible with Python 3.6) to mock all FTS3 REST interactions:

```python
@responses.activate
def test_submit_single_chunk():
    responses.add(responses.POST, "https://fts.test/jobs",
                  json={"job_id": "test-job-001"}, status=200)
    ...
```

### Resume / Recovery Tests

1. Simulate crash after 2 of 5 chunks submitted → verify resume submits remaining 3
2. Simulate crash during polling → verify poller re-attaches to in-flight jobs
3. Simulate duplicate submission scenario → verify deduplication query fires

### Cleanup Safety Tests

1. Verify 404 on DELETE is treated as success
2. Verify cleanup failure does not abort campaign
3. Verify post-cleanup only targets FINISHED files

### SSL Tests

1. `ssl_verify=True` → session.verify is True, no warnings suppressed
2. `ssl_verify=False` → InsecureRequestWarning suppressed, log entry written, report flag set
3. `ssl_verify="/path/ca.pem"` → session.verify is the path string

### Python 3.6.8 Compatibility Tests

`tox -e py36` runs full suite under Python 3.6.8. CI pipeline must include this environment. Syntax checks via `pyflakes` (compatible with 3.6).

### Large-Scale Tests

Integration tests against a real FTS3 endpoint (optional, gated by env var `FTS_INTEGRATION_ENDPOINT`). Parameterised for 10, 100, 1000 file inventories.

---

## 17. Implementation Recommendation

### Package Layout

```
fts_testframework/
├── fts_framework/
│   ├── __init__.py
│   ├── exceptions.py
│   ├── runner.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── loader.py
│   ├── inventory/
│   │   ├── __init__.py
│   │   └── loader.py
│   ├── destination/
│   │   ├── __init__.py
│   │   └── planner.py
│   ├── checksum/
│   │   ├── __init__.py
│   │   └── fetcher.py
│   ├── cleanup/
│   │   ├── __init__.py
│   │   └── manager.py
│   ├── fts/
│   │   ├── __init__.py
│   │   ├── client.py
│   │   ├── submission.py
│   │   ├── poller.py
│   │   └── collector.py
│   ├── metrics/
│   │   ├── __init__.py
│   │   └── engine.py
│   ├── persistence/
│   │   ├── __init__.py
│   │   └── store.py
│   ├── reporting/
│   │   ├── __init__.py
│   │   ├── console.py
│   │   ├── json_report.py
│   │   ├── markdown_report.py
│   │   └── html_report.py
│   └── resume/
│       ├── __init__.py
│       └── controller.py
├── tests/
│   ├── unit/
│   └── integration/
├── config/
│   └── example_config.yaml
├── setup.py
├── requirements.txt
├── requirements-dev.txt
├── tox.ini
├── DESIGN.md
└── CLAUDE.md
```

### Dependencies

**Production (`requirements.txt`):**
```
requests==2.27.1
urllib3==1.26.18
PyYAML==5.4.1
certifi==2021.10.8
```

**Development (`requirements-dev.txt`):**
```
pytest==4.6.11
responses==0.13.4
freezegun==1.1.0
pyflakes==2.4.0
tox==3.28.0
```

### Config Format

YAML (loaded via PyYAML). Schema validated in `config.loader` using explicit required-field checks (no jsonschema dependency). See §6 for full schema.

### Model Strategy

Named tuples for immutable records passed between modules. Plain dicts for mutable state (SubjobRecord, manifest). No dataclasses. No ORM.

### Environment Setup

```bash
python3.6 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip==21.3.1   # last pip release supporting Python 3.6
pip install -e .
pip install -r requirements-dev.txt
```

`setup.py` is the single source of truth for package metadata. `requirements.txt` pins exact versions. `setup.py` `install_requires` uses minimum version constraints only (e.g. `requests>=2.27.1,<3`).

### Packaging and CI

```ini
# tox.ini
[tox]
envlist = py36

[testenv]
deps = -rrequirements-dev.txt
commands = pytest tests/unit/ -v
```

Entry point:
```python
# setup.py
entry_points={"console_scripts": ["fts-run=fts_framework.runner:main"]}
```

---

## 18. Pseudocode and Flow Diagrams

### Full Execution Flow

```
main(config_path):
    config = load_config(config_path)
    run_id = config["run"]["run_id"] or generate_run_id()
    init_run_directory(run_id, config)

    # Validate token
    whoami = fts_client.get("/whoami")
    log_optimizer_state(fts_client.get("/optimizer/current"))

    # Check for existing run (resume)
    if run_exists(run_id):
        subjobs = resume_controller.load(run_id)
    else:
        pfns = inventory.load(config)
        mapping = destination.plan(pfns, config)
        checksums = checksum.fetch_all(mapping.keys(), config)
        persistence.write_manifest(run_id, mapping)

        cleanup.pre(mapping.values(), config)

        subjobs = submission.submit_all(mapping, checksums, config, run_id)
        persistence.update_manifest(run_id, subjobs)

    # Poll all subjobs to terminal state
    poller.run(subjobs, fts_client, config)

    # Harvest
    file_records, retry_records, dm_records = collector.harvest_all(subjobs, fts_client)
    persistence.write_raw_and_normalized(run_id, file_records, retry_records, dm_records)

    # Framework retry loop
    retry_round = 1
    while config["retry"]["framework_retry_max"] > 0 and retry_round <= config["retry"]["framework_retry_max"]:
        failed = [f for f in file_records if f["file_state"] in ("FAILED", "CANCELED")]
        if not failed:
            break
        new_subjobs = submission.submit_retry_round(failed, mapping, config, run_id, retry_round)
        poller.run(new_subjobs, fts_client, config)
        new_records, new_retries, new_dm = collector.harvest_all(new_subjobs, fts_client)
        file_records = merge_records(file_records, new_records)
        retry_round += 1

    # Metrics and reporting
    snapshot = metrics.compute(file_records, retry_records, config)
    persistence.write_metrics(run_id, snapshot)
    reporting.render_all(snapshot, config)

    cleanup.post(file_records, config)
    persistence.mark_completed(run_id)
```

### Submission Loop

```
chunks = chunk(list(mapping.items()), size=200)
for i, chunk_pairs in enumerate(chunks):
    payload = build_payload(chunk_pairs, checksums, config, run_id, chunk_index=i, retry_round=0)
    persist payload to submitted_payloads/chunk_{i:04d}_r0.json
    response = POST /jobs with payload
    job_id = response["job_id"]
    append SubjobRecord to manifest
```

### Polling Loop

```
active_jobs = {job_id: subjob for each non-terminal subjob}
interval = initial_interval_s
while active_jobs and not timed_out:
    sleep(interval)
    for job_id in snapshot(active_jobs):
        state = GET /jobs/{job_id} → job_state
        persist raw response
        if state in TERMINAL_STATES or state == "STAGING":
            mark terminal, remove from active_jobs
    interval = min(interval * backoff_multiplier, max_interval_s)
```

### Retry Collection

```
for each terminal subjob:
    files = GET /jobs/{id}/files
    for each file in files:
        retries = GET /jobs/{id}/files/{file_id}/retries
        persist raw
        append to retry_records
```

### Cleanup Flow

```
pre_cleanup(dest_urls):
    for url in dest_urls:
        resp = DELETE url
        log(url, resp.status_code, "ok" if resp.status_code in (200,204,404) else "warn")
    write cleanup_pre.json

post_cleanup(file_records):
    for f in file_records where file_state == "FINISHED":
        resp = DELETE f["dest_surl"]
        log result
    write cleanup_post.json
```

### Metrics Aggregation

```
eligible_files = [f for f in file_records if f["file_state"] not in ("NOT_USED", "STAGING_UNSUPPORTED")]
finished = [f for f in eligible_files if f["file_state"] == "FINISHED"]

success_rate = len(finished) / len(eligible_files)

throughputs = [f["throughput"] for f in finished if f["throughput"]]
if not throughputs:
    throughputs = [f["throughput_wire"] for f in finished if f["throughput_wire"]]

snapshot = MetricsSnapshot(
    success_rate=success_rate,
    throughput_mean=mean(throughputs),
    throughput_p50=percentile(throughputs, 50),
    ...
)
```

### Resume Logic

```
resume(run_id):
    manifest = load manifest.json
    subjobs = manifest["subjobs"]
    to_poll = []
    to_submit = []

    for chunk_index, payload_path in enumerate(all_payload_paths):
        matching = [s for s in subjobs if s["chunk_index"] == chunk_index]
        if not matching:
            to_submit.append((chunk_index, payload_path))
        elif not matching[0]["terminal"]:
            to_poll.append(matching[0])

    for chunk_index, payload_path in to_submit:
        payload = load(payload_path)
        resp = POST /jobs with payload
        update manifest with new job_id

    return to_poll + newly_submitted_subjobs
```

---

## 19. Acceptance Criteria

| Criterion | Measure |
|---|---|
| 200-file chunking respected | No FTS3 job payload contains more than 200 file entries |
| Python 3.6.8 compatibility | Full test suite passes under `tox -e py36` with zero syntax or import errors |
| SSL verification toggle | `ssl_verify=false` produces InsecureRequestWarning suppression + log entry + report flag; `ssl_verify=true` and path both function correctly |
| Correct resume after interruption | Framework re-attaches to in-flight jobs and does not duplicate submissions |
| All required metrics computed | `MetricsSnapshot` contains non-None values for success_rate, throughput_mean, p50/p90/p95, duration_mean, peak_concurrency, retry_rate |
| Raw REST data stored | `runs/<run_id>/raw/` contains at minimum one file per FTS3 response type |
| Offline report regeneration | `fts-run report --run-id <id>` produces identical report from persisted data with no network access |
| FINISHEDDIRTY handled correctly | Job with mixed file outcomes produces correct per-file metrics and accurate success_rate |
| Tokens never persisted | No token string appears in any file under `runs/<run_id>/` |
| Cleanup idempotent | Two consecutive pre-cleanup runs produce identical audit logs; second run treats 404 as success |
| Framework retry (when enabled) | Failed files from round 0 are resubmitted with incremented `retry_round` in `job_metadata`; final metrics reflect best outcome per file |
| `STAGING` state handled | File in `STAGING` state is counted in `staging_unsupported`, excluded from throughput, flagged in report |
| FTS monitor URLs recorded | Each `SubjobRecord` in manifest includes the FTS WebMonitor URL for that job |

---

## 20. Optional Enhancements

### Phase 1 (post-MVP)

- HTML report generation from Markdown
- Cross-run comparison CLI (`fts-run compare`)
- Parallel Want-Digest fetch pool size auto-detection based on endpoint rate limits

### Phase 2

- Tape/staging support: detect `STAGING` state, implement configurable staging timeout, add `staging_duration` metric
- S3 endpoint support for cleanup (AWS SDK or `boto3` as optional dependency)
- Prometheus metrics exposition endpoint (expose live campaign progress)

### Backlog

| Epic | Story | Notes |
|---|---|---|
| Analytics | YAML-configurable normalised schema export | For loading into Elasticsearch or SQLite |
| Observability | Live polling progress bar | `tqdm` optional dep or simple stdout progress |
| Resilience | Per-token expiry monitoring | Decode JWT `exp` claim; warn N minutes before expiry |
| Interoperability | Rucio rule trigger post-campaign | Invoke Rucio REST to register transferred files as replicas |
| Reporting | IVOA-compatible provenance metadata | Attach campaign provenance to report for archive ingest |

### YAML Config Schema (full example)

```yaml
run:
  run_id: null                        # null = auto-generate
  test_label: "campaign_march_2026"

fts:
  endpoint: "https://fts.example.org:8446"
  ssl_verify: true

tokens:
  fts_submit: "eyJhbGciOiJSUzI1NiJ9..."
  source_read: "eyJhbGciOiJSUzI1NiJ9..."
  dest_write: "eyJhbGciOiJSUzI1NiJ9..."

transfer:
  source_pfns_file: "sources.txt"
  dst_prefix: "https://storage.example.org/test-data"
  preserve_extension: false
  checksum_algorithm: adler32
  verify_checksum: both
  overwrite: false
  chunk_size: 200
  priority: 3
  activity: default
  job_metadata: {}

concurrency:
  want_digest_workers: 8

submission:
  scan_window_s: 300              # 500-recovery job scan window (default 5 min)

polling:
  initial_interval_s: 30
  backoff_multiplier: 1.5
  max_interval_s: 300
  campaign_timeout_s: 86400

cleanup:
  before: false
  after: false

retry:
  fts_retry_max: 2
  framework_retry_max: 0
  min_success_threshold: 0.95

output:
  base_dir: "runs"
  reports:
    console: true
    json: true
    markdown: true
    html: false
```
