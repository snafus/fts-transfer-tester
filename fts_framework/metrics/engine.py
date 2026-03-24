"""
fts_framework.metrics.engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pure metric computation from ``FileRecord`` and ``RetryRecord`` lists.

This module performs no I/O.  It receives normalised records from the
collector, computes all metrics, and returns a ``MetricsSnapshot`` dict.

Throughput source priority
--------------------------
1. ``file["throughput"]`` — agent-reported, highest fidelity.
2. ``file["filesize"] / file["tx_duration"]`` — wire throughput, used when
   primary is zero or absent.
3. ``file["filesize"] / wall_duration`` — wall throughput (always computed
   but not used as fallback; stored for reference only).

A file is excluded from aggregate throughput statistics if:
- Its final ``file_state`` is not ``"FINISHED"``
- ``filesize == 0``
- Both primary and wire throughput resolve to ``None`` or zero
- ``start_time`` or ``finish_time`` is missing (cannot compute wall)

Success rate
------------
``eligible = total - not_used - staging_unsupported``
``success_rate = finished / eligible  (or 0.0 if eligible == 0)``

Standard library only
---------------------
Percentiles use a linear-interpolation formula (no numpy/scipy).
The ``statistics`` module provides mean and median where appropriate.

Usage::

    from fts_framework.metrics.engine import compute
    snapshot = compute(file_records, retry_records, config, run_id)
"""

import logging
import statistics

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Timestamp formats attempted when parsing FTS3 ISO8601 strings.
# FTS3 may omit the trailing 'Z' or include fractional seconds.
_TS_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
]


def compute(file_records, retry_records, config, run_id):
    # type: (list, list, dict, str) -> dict
    """Compute and return a ``MetricsSnapshot`` dict.

    This is the single public entry point.  All metric values are derived
    exclusively from *file_records* and *retry_records*.

    Args:
        file_records (list[dict]): Normalised ``FileRecord`` dicts from
            ``collector.harvest_all()``.  The dicts are updated in-place
            with computed per-file metrics.
        retry_records (list[dict]): Normalised ``RetryRecord`` dicts.
        config (dict): Validated framework config dict.
        run_id (str): Unique run identifier.

    Returns:
        dict: ``MetricsSnapshot`` dict.
    """
    test_label = config["run"]["test_label"]
    min_threshold = config.get("retry", {}).get("min_success_threshold", 0.95)

    # --- Step 1: compute per-file metrics, update records in-place ---
    _compute_file_metrics(file_records)

    # --- Step 2: partition by state ---
    finished = [f for f in file_records if f["file_state"] == "FINISHED"]
    failed = [f for f in file_records if f["file_state"] == "FAILED"]
    canceled = [f for f in file_records if f["file_state"] == "CANCELED"]
    not_used = [f for f in file_records if f["file_state"] == "NOT_USED"]
    # Files from STAGING_UNSUPPORTED jobs retain file_state="STAGING" (as
    # returned by FTS3); the snapshot key is "staging_unsupported" for clarity.
    staging = [f for f in file_records if f["file_state"] == "STAGING"]

    total = len(file_records)
    n_finished = len(finished)
    n_failed = len(failed)
    n_canceled = len(canceled)
    n_not_used = len(not_used)
    n_staging = len(staging)

    eligible = total - n_not_used - n_staging
    success_rate = float(n_finished) / eligible if eligible > 0 else 0.0
    failure_rate = float(n_failed + n_canceled) / eligible if eligible > 0 else 0.0
    threshold_passed = success_rate >= min_threshold

    logger.info(
        "Metrics: total=%d finished=%d failed=%d canceled=%d "
        "not_used=%d staging=%d success_rate=%.3f",
        total, n_finished, n_failed, n_canceled, n_not_used, n_staging, success_rate,
    )

    # --- Step 3: throughput stats (finished files with valid throughput only) ---
    throughput_vals = [
        f["throughput"] for f in finished
        if f["throughput"] and f["throughput"] > 0
    ]
    # Fall back to wire throughput for files where primary is missing
    for f in finished:
        if (not f["throughput"] or f["throughput"] == 0) and f["throughput_wire"] > 0:
            throughput_vals.append(f["throughput_wire"])

    if throughput_vals:
        throughput_mean = statistics.mean(throughput_vals)
        throughput_p50 = _percentile(throughput_vals, 50)
        throughput_p90 = _percentile(throughput_vals, 90)
        throughput_p95 = _percentile(throughput_vals, 95)
        throughput_p99 = _percentile(throughput_vals, 99)
        throughput_max = max(throughput_vals)
    else:
        throughput_mean = throughput_p50 = throughput_p90 = None
        throughput_p95 = throughput_p99 = throughput_max = None

    agg_tp = _aggregate_throughput(finished)

    # --- Step 4: duration stats ---
    duration_vals = [
        f["wall_duration_s"] for f in finished
        if f["wall_duration_s"] > 0
    ]
    if duration_vals:
        duration_mean = statistics.mean(duration_vals)
        duration_p50 = _percentile(duration_vals, 50)
        duration_p90 = _percentile(duration_vals, 90)
        duration_p95 = _percentile(duration_vals, 95)
    else:
        duration_mean = duration_p50 = duration_p90 = duration_p95 = None

    # --- Step 5: retry stats ---
    total_retries = len(retry_records)
    files_with_retries = len(set(r["file_id"] for r in retry_records))
    # retry_rate denominator is total (all file records, including NOT_USED and
    # STAGING) so the rate reflects the overall campaign, not just eligible files.
    retry_rate = float(files_with_retries) / total if total > 0 else 0.0
    retry_dist = _retry_distribution(retry_records)

    # --- Step 6: concurrency ---
    concurrency_timeline = _estimate_concurrency(finished)
    if concurrency_timeline:
        peak_concurrency = max(b["active"] for b in concurrency_timeline)
        active_vals = [b["active"] for b in concurrency_timeline]
        mean_concurrency = statistics.mean(active_vals) if active_vals else 0.0
    else:
        peak_concurrency = 0
        mean_concurrency = 0.0

    # --- Step 7: failure categorisation ---
    failure_reasons = _categorise_failures(failed + canceled)

    # --- Step 8: throughput/concurrency timeseries ---
    bucket_width_s = config.get("output", {}).get("timeseries_bucket_s", 60)
    timeseries = _compute_timeseries(finished, bucket_width_s)

    return {
        "run_id": run_id,
        "test_label": test_label,
        "generated_at": _now_iso(),

        # Counts
        "total_files": total,
        "finished": n_finished,
        "failed": n_failed,
        "canceled": n_canceled,
        "not_used": n_not_used,
        "staging_unsupported": n_staging,

        # Rates
        "success_rate": success_rate,
        "failure_rate": failure_rate,
        "threshold_passed": threshold_passed,

        # Throughput
        "throughput_mean": throughput_mean,
        "throughput_p50": throughput_p50,
        "throughput_p90": throughput_p90,
        "throughput_p95": throughput_p95,
        "throughput_p99": throughput_p99,
        "throughput_max": throughput_max,
        "aggregate_throughput_bytes_per_s": agg_tp,

        # Duration
        "duration_mean_s": duration_mean,
        "duration_p50_s": duration_p50,
        "duration_p90_s": duration_p90,
        "duration_p95_s": duration_p95,

        # Retries
        "total_retries": total_retries,
        "files_with_retries": files_with_retries,
        "retry_rate": retry_rate,
        "retry_distribution": retry_dist,

        # Concurrency
        "peak_concurrency": peak_concurrency,
        "mean_concurrency": mean_concurrency,
        "concurrency_timeline": concurrency_timeline,

        # Failures
        "failure_reasons": failure_reasons,

        # Throughput timeline (placeholder — populated if needed by reporting)
        "throughput_timeline": [],

        # Timeseries (per-bucket throughput and concurrency)
        "timeseries": timeseries,

        # SSL warning — caller sets this if ssl_verify=False
        "ssl_verify_disabled": False,
    }


# ---------------------------------------------------------------------------
# Per-file metric computation
# ---------------------------------------------------------------------------

def _compute_file_metrics(file_records):
    # type: (list) -> None
    """Update each FileRecord in-place with computed throughput and duration.

    Only modifies ``throughput_wire``, ``throughput_wall``, and
    ``wall_duration_s``.  The original ``throughput`` field (agent-reported)
    is never overwritten.

    Args:
        file_records (list[dict]): FileRecord dicts to update.
    """
    for f in file_records:
        filesize = f.get("filesize") or 0
        tx_duration = f.get("tx_duration") or 0.0
        start_ts = f.get("start_time") or ""
        finish_ts = f.get("finish_time") or ""

        # Wire throughput: filesize / tx_duration
        if filesize > 0 and tx_duration > 0:
            f["throughput_wire"] = float(filesize) / float(tx_duration)
        else:
            f["throughput_wire"] = 0.0

        # Wall duration and wall throughput
        start_dt = _parse_iso(start_ts) if start_ts else None
        finish_dt = _parse_iso(finish_ts) if finish_ts else None

        if start_dt is not None and finish_dt is not None:
            wall_s = (finish_dt - start_dt).total_seconds()
            f["wall_duration_s"] = max(wall_s, 0.0)
            if filesize > 0 and wall_s > 0:
                f["throughput_wall"] = float(filesize) / wall_s
            else:
                f["throughput_wall"] = 0.0
        else:
            f["wall_duration_s"] = 0.0
            f["throughput_wall"] = 0.0


# ---------------------------------------------------------------------------
# Aggregate throughput
# ---------------------------------------------------------------------------

def _aggregate_throughput(finished_files):
    # type: (list) -> object
    """Compute campaign aggregate throughput: total_bytes / campaign_wall_time.

    Args:
        finished_files (list[dict]): FileRecord dicts with ``file_state ==
            "FINISHED"``.  Only files with valid timestamps contribute to
            the campaign wall-time window.  ``total_bytes`` is summed from
            *all* finished files so that no transferred data is silently
            excluded from the byte count.

    Returns:
        float or None: Bytes per second, or ``None`` if it cannot be computed.
    """
    timed = [
        f for f in finished_files
        if f.get("start_time") and f.get("finish_time")
        and _parse_iso(f["start_time"]) is not None
        and _parse_iso(f["finish_time"]) is not None
    ]
    if not timed:
        return None

    start_dts = [_parse_iso(f["start_time"]) for f in timed]
    finish_dts = [_parse_iso(f["finish_time"]) for f in timed]

    campaign_start = min(start_dts)
    campaign_end = max(finish_dts)
    wall_s = (campaign_end - campaign_start).total_seconds()

    if wall_s <= 0:
        return None

    total_bytes = sum(f.get("filesize") or 0 for f in finished_files)
    return float(total_bytes) / wall_s


# ---------------------------------------------------------------------------
# Concurrency estimation
# ---------------------------------------------------------------------------

def _estimate_concurrency(finished_files, bucket_width_s=1):
    # type: (list, int) -> list
    """Estimate per-second transfer concurrency, throughput, and bytes in-flight.

    Each bucket represents a ``bucket_width_s``-second interval.  Three
    quantities are tracked using difference-array / prefix-sum (O(N + T)):

    - ``active``: number of transfers with ``start_epoch <= t < finish_epoch``.
    - ``bytes_in_flight``: sum of ``filesize`` for active transfers.
    - ``throughput_bytes_s``: aggregate bytes/s using the constant-rate model
      (each transfer contributes ``filesize / wall_duration`` uniformly across
      the seconds it spans).

    Args:
        finished_files (list[dict]): FINISHED FileRecord dicts.
        bucket_width_s (int): Bucket width in seconds.  Default 1.

    Returns:
        list[dict]: Timeline as
            ``[{"t": epoch_int, "active": int,
                "bytes_in_flight": int, "throughput_bytes_s": float}]``.
            Empty if no files have valid timestamps.
    """
    # Collect (start_epoch, finish_epoch, filesize, rate) per file.
    # rate is None for files with zero wall duration (excluded from throughput).
    timed = []
    for f in finished_files:
        s = _parse_iso(f.get("start_time") or "")
        e = _parse_iso(f.get("finish_time") or "")
        if s is None or e is None:
            continue
        s_i = int(_epoch(s))
        e_i = int(_epoch(e))
        filesize = int(f.get("filesize") or 0)
        wall_s = e_i - s_i
        rate = float(filesize) / wall_s if (wall_s > 0 and filesize > 0) else None
        timed.append((s_i, e_i, filesize, rate))

    if not timed:
        return []

    t_min = min(s for s, _, _, _ in timed)
    t_max = max(e for _, e, _, _ in timed)

    span = t_max - t_min + 1
    sentinel = span + 1

    diff_active = [0] * sentinel
    diff_bytes = [0] * sentinel
    diff_rate = [0.0] * sentinel

    for s_i, e_i, filesize, rate in timed:
        lo = s_i - t_min
        hi = e_i - t_min
        diff_active[lo] += 1
        if hi < sentinel:
            diff_active[hi] -= 1
        if filesize > 0:
            diff_bytes[lo] += filesize
            if hi < sentinel:
                diff_bytes[hi] -= filesize
        if rate is not None:
            diff_rate[lo] += rate
            if hi < sentinel:
                diff_rate[hi] -= rate

    # Prefix sums
    active_arr = [0] * span
    bytes_arr = [0] * span
    rate_arr = [0.0] * span
    a = b = r = 0
    for i in range(span):
        a += diff_active[i]
        b += diff_bytes[i]
        r += diff_rate[i]
        active_arr[i] = a
        bytes_arr[i] = b
        rate_arr[i] = r

    buckets = []
    i = 0
    while i < span:
        buckets.append({
            "t": int(t_min + i),
            "active": active_arr[i],
            "bytes_in_flight": bytes_arr[i],
            "throughput_bytes_s": round(rate_arr[i], 2),
        })
        i += bucket_width_s

    return buckets


# ---------------------------------------------------------------------------
# Throughput / concurrency timeseries
# ---------------------------------------------------------------------------

def _compute_timeseries(finished_files, bucket_width_s):
    # type: (list, int) -> list
    """Compute a per-bucket throughput and concurrency timeseries.

    Each bucket covers ``bucket_width_s`` seconds.  For each finished file,
    a constant transfer rate is assumed: ``rate = filesize / wall_duration``.
    The file's contribution to each bucket it overlaps is
    ``rate * overlap_seconds``.  Aggregate throughput for the bucket is the
    total bytes attributed to it divided by ``bucket_width_s``.

    Files with zero filesize, zero/negative wall duration, or unparseable
    timestamps are excluded.

    Args:
        finished_files (list[dict]): FINISHED FileRecord dicts with computed
            ``wall_duration_s`` (set by ``_compute_file_metrics``).
        bucket_width_s (int): Bucket width in seconds.

    Returns:
        list[dict]: One dict per bucket with keys ``bucket_start`` (ISO8601),
            ``bucket_end`` (ISO8601), ``active_transfers`` (int), and
            ``aggregate_throughput_bytes_s`` (float).  Empty list if no
            eligible files exist.
    """
    W = float(bucket_width_s)

    # Build list of (start_epoch, end_epoch, rate_bytes_per_s)
    timed = []
    for f in finished_files:
        s = _parse_iso(f.get("start_time") or "")
        e = _parse_iso(f.get("finish_time") or "")
        if s is None or e is None:
            continue
        wall_s = (e - s).total_seconds()
        if wall_s <= 0:
            continue
        filesize = f.get("filesize") or 0
        if filesize <= 0:
            continue
        timed.append((_epoch(s), _epoch(e), float(filesize) / wall_s))

    if not timed:
        return []

    t_min = min(s for s, _, _ in timed)
    t_max = max(e for _, e, _ in timed)

    n_buckets = int((t_max - t_min) / W) + 1

    agg_bytes = [0.0] * n_buckets
    concurrency = [0] * n_buckets

    for file_start, file_end, rate in timed:
        b_first = int((file_start - t_min) / W)
        b_last = min(int((file_end - t_min) / W), n_buckets - 1)
        for b in range(b_first, b_last + 1):
            bucket_t0 = t_min + b * W
            bucket_t1 = bucket_t0 + W
            overlap = min(file_end, bucket_t1) - max(file_start, bucket_t0)
            if overlap > 0:
                agg_bytes[b] += rate * overlap
                concurrency[b] += 1

    # Convert epoch bucket boundaries to ISO strings
    epoch_base = datetime(1970, 1, 1)
    buckets = []
    for b in range(n_buckets):
        bucket_t0 = t_min + b * W
        bucket_t1 = bucket_t0 + W
        dt0 = epoch_base + timedelta(seconds=bucket_t0)
        dt1 = epoch_base + timedelta(seconds=bucket_t1)
        buckets.append({
            "bucket_start": dt0.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "bucket_end": dt1.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "active_transfers": concurrency[b],
            "aggregate_throughput_bytes_s": agg_bytes[b] / W,
        })
    return buckets


# ---------------------------------------------------------------------------
# Retry distribution
# ---------------------------------------------------------------------------

def _retry_distribution(retry_records):
    # type: (list) -> dict
    """Count retry records by file (how many retries each file had).

    Returns:
        dict: Mapping of retry-count-as-string → number-of-files with that
            count.  E.g. ``{"0": 5, "1": 2, "2": 1}`` means 5 files had
            0 retries (absent from retry_records), 2 had 1, 1 had 2.

            Note: files with 0 retries are not in *retry_records* so the
            ``"0"`` key is omitted from this dict; the caller can infer it.
    """
    counts = {}  # type: dict
    for rec in retry_records:
        fid = rec["file_id"]
        counts[fid] = counts.get(fid, 0) + 1

    dist = {}  # type: dict
    for count in counts.values():
        key = str(count)
        dist[key] = dist.get(key, 0) + 1
    return dist


# ---------------------------------------------------------------------------
# Failure categorisation
# ---------------------------------------------------------------------------

def _categorise_failures(failed_files):
    # type: (list) -> dict
    """Group failure reasons into a summary dict.

    The raw ``reason`` field from FTS3 is used as-is; empty reasons become
    ``"UNKNOWN"``.

    Args:
        failed_files (list[dict]): Failed or canceled FileRecord dicts.

    Returns:
        dict: ``{reason_string: count}``
    """
    categories = {}  # type: dict
    for f in failed_files:
        reason = (f.get("reason") or "").strip() or "UNKNOWN"
        categories[reason] = categories.get(reason, 0) + 1
    return categories


# ---------------------------------------------------------------------------
# Percentile (stdlib, no numpy)
# ---------------------------------------------------------------------------

def _percentile(data, p):
    # type: (list, float) -> float
    """Compute the *p*-th percentile of *data* using linear interpolation.

    Args:
        data (list[float]): Non-empty sequence of numeric values.
        p (float): Percentile in [0, 100].

    Returns:
        float: Interpolated percentile value.
    """
    if not data:
        return 0.0
    sorted_data = sorted(data)
    n = len(sorted_data)
    if n == 1:
        return float(sorted_data[0])
    k = (n - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, n - 1)
    frac = k - lo
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * frac


# ---------------------------------------------------------------------------
# Timestamp utilities
# ---------------------------------------------------------------------------

def _parse_iso(ts):
    # type: (str) -> object
    """Parse an ISO8601 timestamp string as returned by FTS3.

    Returns:
        datetime or None: Parsed datetime, or None if parsing fails.
    """
    if not ts:
        return None
    ts = ts.strip()
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    logger.debug("Cannot parse timestamp %r — excluding from time-based metrics", ts)
    return None


def _epoch(dt):
    # type: (datetime) -> float
    """Return seconds since the Unix epoch for *dt* (naive, UTC assumed)."""
    epoch = datetime(1970, 1, 1)
    return (dt - epoch).total_seconds()


def _now_iso():
    # type: () -> str
    """Return current UTC time as an ISO8601 string."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
