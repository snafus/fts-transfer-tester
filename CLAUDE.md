# CLAUDE.md — FTS3 REST Test Framework

## Project Overview

Production-grade FTS3 REST transfer test framework. Drives bulk file transfer campaigns against FTS3 endpoints via REST, harvests file-level metrics as the authoritative source of truth, and persists all raw and normalised data for reproducible offline analysis.

Full architecture is in `DESIGN.md`. This file contains working rules for Claude when implementing or modifying this codebase.

---

## Project Setup

### Environment

Use `venv` (stdlib) and `setup.py` (legacy). No `pyproject.toml`, no `poetry`, no `conda`, no `virtualenv`.

```bash
# Local development (uses whatever Python is installed via uv)
uv venv .venv
uv pip install -e .
uv pip install pytest responses freezegun pyflakes PyYAML requests
source .venv/bin/activate
pytest tests/unit/ -v          # or: tox -e local
```

CI (GitHub Actions) tests against Python 3.6.8 with all pinned versions from
`requirements.txt`. See `.github/workflows/ci.yml`.

`setup.py` is the single source of truth for package metadata and dependencies.
`requirements.txt` pins exact 3.6.8-compatible versions for CI.
`setup.py` `install_requires` uses minimum-version constraints only.

### pip Version (CI only)

pip 21.3.1 is the last release with Python 3.6 support. Pinned in CI workflow.
Local development uses uv which manages its own pip.

---

## Hard Constraints

### Python 3.6.8 — Non-Negotiable

Every line of code must be compatible with Python 3.6.8. Enforcement:

| Forbidden | Reason | Use instead |
|---|---|---|
| `dataclasses` | Python 3.7+ | `collections.namedtuple` or plain dict |
| `dict[str, int]` as type hint | Python 3.9+ | `typing.Dict[str, int]` |
| `list[str]` as type hint | Python 3.9+ | `typing.List[str]` |
| `from __future__ import annotations` | Python 3.7+ | Write annotations inline |
| `:=` walrus operator | Python 3.8+ | Explicit assignment |
| `asyncio.run()` | Python 3.7+ | `loop.run_until_complete()` |
| `pathlib.Path.is_relative_to()` | Python 3.9+ | String prefix check |
| `TypedDict` | Python 3.8+ | Plain dict with comment |

Always run `tox -e py36` before considering any implementation complete.

### Dependencies — Pinned, Minimal

```
requests==2.27.1
urllib3==1.26.18
PyYAML==5.4.1
certifi==2021.10.8
```

Do **not** add new production dependencies without explicit user approval. Do not add numpy, pandas, or any data science library — all metrics use `statistics` (stdlib) and list comprehensions.

---

## Architecture Rules

### Module Boundaries

Each module has one responsibility. Do not add logic to a module that belongs to another:

- `fts/client.py` — session creation only, no business logic
- `fts/submission.py` — payload construction and POST, no polling
- `fts/poller.py` — polling loop only, no harvesting
- `fts/collector.py` — REST harvesting only, no metric computation
- `metrics/engine.py` — computation only, no I/O
- `persistence/store.py` — all disk I/O, no computation
- `cleanup/manager.py` — WebDAV DELETE only, uses `requests` not FTS3

### Raw Data First

Every REST response must be written to `runs/<run_id>/raw/` **before** any processing. Never process a response without first persisting it. This is the reproducibility guarantee.

### File-Level Records Are Authoritative

All metrics derive from `GET /jobs/{id}/files` records. Job-level data (`GET /jobs/{id}`) is used only for state polling, never for metric computation.

### Tokens Are Never Written to Disk

Tokens must never appear in:
- Any file under `runs/<run_id>/`
- Log output
- Report files

The config copy written to `runs/<run_id>/config.yaml` must have all token values replaced with `"<REDACTED>"`.

---

## 500-Recovery on Submission

POST /jobs returning 500 does not mean the job was not created. Always scan before resubmitting:

1. Wait 5s after 500
2. `GET /jobs?time_window=<scan_window_h>&state_in=SUBMITTED,READY,ACTIVE,...`
3. Match client-side on `job_metadata.run_id + chunk_index + retry_round`
4. One match → recover job_id; multiple → take most recent; none → safe to resubmit

`scan_window_s` config key (default 300s). Framework keys in `job_metadata` always overwrite user-supplied keys.

## Key Design Decisions (do not revisit without user instruction)

| Decision | Value |
|---|---|
| Checksum algorithm | ADLER32 only |
| Checksum source | WebDAV `Want-Digest: adler32` HEAD request pre-submission |
| Want-Digest format | Expect hex; detect base64 and convert; hard fail if neither parses |
| Destination pattern | `{dst_prefix}/{test_label}/testfile_{N:06d}[.ext]` |
| File extension | Optional; default off (`preserve_extension: false`) |
| Throughput primary source | `file.throughput` (agent-reported); fallback to `filesize/tx_duration` |
| Concurrency model | Observe only; derive from file timestamps; never touch FTS3 link config API |
| FINISHEDDIRTY jobs | Compute metrics on individual file outcomes; do not treat job as monolithic |
| STAGING state | Unsupported error; mark file failed; log; continue |
| Cleanup mechanism | `requests` HTTP DELETE against WebDAV/HTTPS; not via FTS3 |
| Token refresh | Delegated to FTS3 server; framework has no refresh logic |
| Token model | Role-keyed: `fts_submit`, `source_read`, `dest_write`; same value permitted |
| Framework retry | Default off; opt-in via `framework_retry_max > 0` |
| run_id | Framework-generated `{timestamp}_{short_uuid}` |
| 500 recovery scan window | Configurable `scan_window_s`, default 300s (5 min) |
| job_metadata merge | User fields + framework fields; framework keys always win |
| `activity` | Configurable per campaign; top-level FTS3 param, not inside job_metadata |
| `priority` | Configurable per campaign (1–5, default 3); top-level FTS3 param |
| Tape/staging | No tape support; do not add tape logic without explicit instruction |

---

## FTS3 REST API Reference

Endpoints this framework uses:

```
POST /jobs                              — submit job
GET  /jobs/{id}                         — poll state
GET  /jobs/{id}/files                   — harvest file records (authoritative)
GET  /jobs/{id}/files/{fid}/retries     — harvest retry history
GET  /jobs/{id}/dm                      — harvest DM records
GET  /whoami                            — validate token at startup
GET  /optimizer/current                 — log link state at startup
```

FTS3 terminal job states: `FINISHED`, `FAILED`, `FINISHEDDIRTY`, `CANCELED`
FTS3 file states: `SUBMITTED`, `READY`, `ACTIVE`, `FINISHED`, `FAILED`, `CANCELED`, `NOT_USED`, `STAGING`

`FINISHEDDIRTY` = partial success (mixed file outcomes). Not a failure. Process file records individually.

---

## Directory Structure

```
runs/<run_id>/
    manifest.json
    config.yaml                 (tokens redacted)
    submitted_payloads/
    raw/jobs/ raw/files/ raw/retries/ raw/dm/
    normalized/
    metrics/
    cleanup_pre.json
    cleanup_post.json
    reports/
```

---

## Testing Rules

- Unit tests must not make real HTTP calls — use `responses` library to mock
- All edge cases from `DESIGN.md §10` must have corresponding test cases
- Resume/recovery tests must simulate crash scenarios explicitly
- `tox -e py36` must pass before any feature is considered complete
- Integration tests are gated by `FTS_INTEGRATION_ENDPOINT` env var

---

## What NOT to Do

- Do not use CLI tools (`fts-rest-cli`, `davix`, `curl` subprocess) — REST only via `requests`
- Do not add SRM, gsiftp, or XRootD protocol handling
- Do not implement token refresh — FTS3 handles it
- Do not interact with the FTS3 link optimizer configuration API
- Do not add tape/staging logic
- Do not use numpy, pandas, or scipy for metrics — use `statistics` stdlib
- Do not add docstrings or comments to code you didn't change
- Do not add error handling for scenarios that cannot occur given the design constraints
