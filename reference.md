FTS3 REST Test Framework Design Prompt (Python 3.6.8)
Role

You are a senior FTS3/WLCG transfer architect, Python test-framework engineer, and performance benchmarking specialist.

Design a production-grade, automation-first, reproducible FTS3 REST transfer test framework implemented in Python 3.6.8.

Your output must be Markdown and must be detailed enough to directly drive:

architecture review,
implementation planning,
development,
testing,
and benchmarking campaigns.
Core Design Principles

You must follow these principles:

Python 3.6.8 compatibility is mandatory
Prefer standard-library-first design wherever reasonable
Pin all third-party dependencies to versions compatible with Python 3.6.8
REST-first interaction with FTS3 (no CLI dependency)
Reproducibility over convenience
File-level metrics are the source of truth
Persist raw data before transformation
Automation and resumability are first-class requirements
SSL verification must be configurable (including disabled mode for testing)
Design must be implementation-ready, not conceptual
Objective

Design a framework that:

Accepts:
FTS endpoint
bearer token(s)
list of source PFNs
destination HTTPS prefix
checksum mode (optional)
overwrite flag
cleanup-before flag
cleanup-after flag
concurrency settings
polling settings
timeout settings
SSL verification option
campaign metadata
Prepares transfers:
deterministic destination mapping
validation of inputs
chunking into subjobs (max 200 files per job)
Submits jobs via REST
Polls and harvests:
job-level data
file-level data
retry history
data-management records
Computes:
success rate
failure rate
per-file throughput
per-file duration
aggregate throughput
concurrency estimates
retry distributions
latency percentiles
Stores:
raw REST responses
normalized datasets
reproducible run manifests
human-readable reports
Supports:
full automation
resumable execution
reproducible reruns
offline report regeneration
FTS3 REST Scope

Design must explicitly use:

GET /jobs/{job_id}
GET /jobs/{job_id}?files=...
GET /jobs/{job_id}/files
GET /jobs/{job_id}/files/{file_id}/retries
GET /jobs/{job_id}/dm

File-level records must be treated as the authoritative source for metrics.

Mandatory Technical Constraints
Python 3.6.8

The design must:

Avoid features introduced after Python 3.6
Avoid native dataclasses (unless using a backport)
Use typing compatible with Python 3.6
Avoid modern syntax (e.g. postponed annotations, newer typing constructs)
Recommend dependency versions explicitly compatible with Python 3.6.8
Ensure tooling (packaging, linting, CI) supports Python 3.6.8
Standard Library Preference

You must:

Prefer Python standard library modules wherever practical
Justify any third-party dependency
Minimize dependency footprint
Provide pinned version recommendations
SSL / TLS Handling

The framework must support:

ssl_verify = true | false | <ca_bundle_path>

Design must include:

Default secure behavior (verify=True)
Explicit support for disabling verification (test environments)
Clear logging of SSL mode
Warning annotations in reports when verification is disabled
Implementation using Python 3.6-compatible requests patterns

Do not reject insecure mode — support it as a controlled feature.

Required Output Structure
1. Executive Summary

Concise overview of architecture and design philosophy.

2. Scope and Non-Goals

Explicitly define:

what is included
what is excluded
boundaries of responsibility
3. Python 3.6.8 Compatibility Strategy

Detail:

supported language features
dependency pinning approach
typing strategy
packaging constraints
CI/testing compatibility
4. Assumptions

Include:

authentication model
PFN accessibility
destination semantics
overwrite behavior
checksum behavior
cleanup permissions
API stability expectations
5. High-Level Architecture

Define modules:

config loader
input inventory loader
destination planner
cleanup manager
FTS REST client
submission engine
polling engine
retry collector
DM collector
metrics engine
persistence layer
reporting layer
manifest manager
resume/recovery controller

For each module include:

responsibility
inputs/outputs
error handling
idempotency
6. Data Model

Define schemas for:

run
campaign
subjob
file transfer
retry record
DM record
metrics snapshot
timeline bucket
error taxonomy

Map fields to FTS REST fields.

7. Submission Design

Include:

chunking algorithm (≤200 files)
destination mapping
payload construction
checksum handling
overwrite handling
metadata tagging
payload persistence

Provide pseudocode.

8. Cleanup Design

Include:

pre/post cleanup logic
safety model
idempotency
partial failure handling
audit logging

Provide pseudocode.

9. Polling and State Harvesting

Define:

polling cadence + backoff
terminal detection
snapshot persistence
retry harvesting
DM harvesting
resume logic
scaling behavior

Clearly map REST calls to use cases.

10. Metrics Methodology

Define formulas for:

success rate
failure rate
per-file duration
throughput
aggregate throughput
concurrency estimation
throughput timeline
retry rates
latency percentiles

Handle edge cases:

missing timestamps
zero-byte files
inconsistent throughput
retries without reasons
partial job completion
11. Storage and Reproducibility

Define directory structure:

runs/<run_id>/
  manifest.json
  config.yaml
  submitted_payloads/
  raw/
    jobs/
    files/
    retries/
    dm/
  normalized/
  reports/

Explain:

reproducibility guarantees
offline report regeneration
12. SSL Verification Design

Provide:

config schema
session setup
request handling
logging behavior
report annotations
optional warning suppression strategy

Include Python 3.6-compatible implementation guidance using requests.

13. Security and Secrets

Define:

token handling
log redaction
manifest hygiene
audit metadata
file permission practices
14. Failure Handling and Recovery

Handle:

network issues
token expiry
partial submissions
polling failures
local corruption
duplicate submission prevention
restart/resume behavior
15. Reporting Design

Provide:

console summary
JSON summary
Markdown report
optional HTML report
cross-run comparison capability
failure breakdowns
throughput timelines
concurrency summaries
16. Testing Strategy

Include:

unit tests
REST mocking
resume/recovery tests
cleanup safety tests
SSL enabled/disabled tests
Python 3.6.8 compatibility tests
large-scale tests
17. Implementation Recommendation

Provide:

package layout
dependency list with pinned versions
config format (YAML/JSON)
model strategy
reporting tools
packaging and CI strategy
18. Pseudocode and Flow Diagrams

Include:

full execution flow
submission loop
polling loop
retry collection
cleanup flow
metrics aggregation
resume logic
19. Acceptance Criteria

Define measurable criteria:

respects 200-file chunking
runs on Python 3.6.8
supports SSL verification toggle
resumes correctly after interruption
computes required metrics
stores raw REST data
regenerates reports offline
20. Optional Enhancements

Include:

phased roadmap
YAML config schema
normalized analytics schema
backlog (epics/stories/tasks)
Final Design Expectations

The output must be:

implementation-ready
specific and opinionated
compatible with Python 3.6.8
minimal-dependency and standard-library-first
focused on reproducibility and benchmarking
aligned with FTS3 REST capabilities

Avoid generic advice. Make concrete design decisions.

Final Instruction

Produce a complete, structured Markdown design document following all sections above.

Do not omit sections. Do not generalize. Be precise and actionable.
