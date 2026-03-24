"""
Unit tests for fts_framework.metrics.engine.

No I/O — all inputs are constructed in-memory.
"""

import pytest

from fts_framework.metrics.engine import (
    compute,
    _compute_file_metrics,
    _aggregate_throughput,
    _estimate_concurrency,
    _compute_timeseries,
    _retry_distribution,
    _categorise_failures,
    _percentile,
    _parse_iso,
    _epoch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(test_label="test-run", min_threshold=0.95):
    return {
        "run": {"test_label": test_label},
        "retry": {"min_success_threshold": min_threshold},
    }


def _file(file_id=1, file_state="FINISHED", filesize=1024, throughput=500.0,
          tx_duration=2.0, start_time="2026-01-01T00:00:00",
          finish_time="2026-01-01T00:00:02", reason=""):
    return {
        "file_id": file_id,
        "file_state": file_state,
        "filesize": filesize,
        "throughput": throughput,
        "tx_duration": tx_duration,
        "start_time": start_time,
        "finish_time": finish_time,
        "reason": reason,
        # computed fields initialised to zero (as collector sets them)
        "throughput_wire": 0.0,
        "throughput_wall": 0.0,
        "wall_duration_s": 0.0,
    }


def _retry(file_id, attempt=0):
    return {
        "file_id": file_id,
        "job_id": "job-1",
        "attempt": attempt,
        "datetime": "2026-01-01T00:00:01",
        "reason": "timeout",
        "transfer_host": "worker1",
    }


# ---------------------------------------------------------------------------
# _parse_iso
# ---------------------------------------------------------------------------

class TestParseIso:
    def test_basic_iso_no_z(self):
        dt = _parse_iso("2026-01-01T00:00:00")
        assert dt is not None
        assert dt.year == 2026

    def test_iso_with_z(self):
        dt = _parse_iso("2026-01-01T12:30:00Z")
        assert dt is not None
        assert dt.hour == 12

    def test_iso_with_fractional(self):
        dt = _parse_iso("2026-01-01T00:00:00.123456")
        assert dt is not None

    def test_iso_with_fractional_and_z(self):
        dt = _parse_iso("2026-01-01T00:00:00.123456Z")
        assert dt is not None

    def test_space_separator(self):
        dt = _parse_iso("2026-01-01 12:00:00")
        assert dt is not None

    def test_space_separator_fractional(self):
        dt = _parse_iso("2026-01-01 12:00:00.000001")
        assert dt is not None

    def test_empty_string_returns_none(self):
        assert _parse_iso("") is None

    def test_none_returns_none(self):
        assert _parse_iso(None) is None

    def test_garbage_returns_none(self):
        assert _parse_iso("not-a-date") is None

    def test_whitespace_stripped(self):
        dt = _parse_iso("  2026-01-01T00:00:00  ")
        assert dt is not None


# ---------------------------------------------------------------------------
# _epoch
# ---------------------------------------------------------------------------

class TestEpoch:
    def test_unix_epoch_is_zero(self):
        from datetime import datetime
        epoch = datetime(1970, 1, 1)
        assert _epoch(epoch) == 0.0

    def test_known_timestamp(self):
        from datetime import datetime
        dt = datetime(2026, 1, 1, 0, 0, 0)
        e = _epoch(dt)
        assert e > 0
        # 2026-01-01 is well past the epoch
        assert e > 1_700_000_000


# ---------------------------------------------------------------------------
# _percentile
# ---------------------------------------------------------------------------

class TestPercentile:
    def test_single_element(self):
        assert _percentile([42.0], 50) == 42.0

    def test_p0_is_minimum(self):
        assert _percentile([1.0, 2.0, 3.0], 0) == 1.0

    def test_p100_is_maximum(self):
        assert _percentile([1.0, 2.0, 3.0], 100) == 3.0

    def test_p50_two_elements(self):
        result = _percentile([1.0, 3.0], 50)
        assert result == pytest.approx(2.0)

    def test_p50_odd_count(self):
        result = _percentile([1.0, 2.0, 3.0], 50)
        assert result == pytest.approx(2.0)

    def test_p90_sorted(self):
        data = [float(i) for i in range(1, 11)]  # 1..10
        result = _percentile(data, 90)
        assert result == pytest.approx(9.1)

    def test_empty_returns_zero(self):
        assert _percentile([], 50) == 0.0

    def test_unsorted_input_handled(self):
        result = _percentile([3.0, 1.0, 2.0], 50)
        assert result == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# _compute_file_metrics
# ---------------------------------------------------------------------------

class TestComputeFileMetrics:
    def test_wire_throughput_computed(self):
        f = _file(filesize=1000, tx_duration=2.0)
        _compute_file_metrics([f])
        assert f["throughput_wire"] == pytest.approx(500.0)

    def test_wall_duration_computed(self):
        f = _file(start_time="2026-01-01T00:00:00", finish_time="2026-01-01T00:00:10")
        _compute_file_metrics([f])
        assert f["wall_duration_s"] == pytest.approx(10.0)

    def test_wall_throughput_computed(self):
        f = _file(filesize=100, start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10")
        _compute_file_metrics([f])
        assert f["throughput_wall"] == pytest.approx(10.0)

    def test_zero_filesize_wire_throughput_zero(self):
        f = _file(filesize=0, tx_duration=2.0)
        _compute_file_metrics([f])
        assert f["throughput_wire"] == 0.0

    def test_zero_tx_duration_wire_throughput_zero(self):
        f = _file(filesize=1000, tx_duration=0.0)
        _compute_file_metrics([f])
        assert f["throughput_wire"] == 0.0

    def test_missing_start_time_wall_duration_zero(self):
        f = _file(start_time="", finish_time="2026-01-01T00:00:02")
        _compute_file_metrics([f])
        assert f["wall_duration_s"] == 0.0

    def test_missing_finish_time_wall_duration_zero(self):
        f = _file(start_time="2026-01-01T00:00:00", finish_time="")
        _compute_file_metrics([f])
        assert f["wall_duration_s"] == 0.0

    def test_agent_throughput_not_overwritten(self):
        f = _file(throughput=999.0)
        _compute_file_metrics([f])
        assert f["throughput"] == 999.0

    def test_finish_before_start_wall_duration_zero(self):
        # finish before start → negative → clamped to 0
        f = _file(start_time="2026-01-01T00:00:10", finish_time="2026-01-01T00:00:00")
        _compute_file_metrics([f])
        assert f["wall_duration_s"] == 0.0

    def test_multiple_files_all_updated(self):
        files = [_file(file_id=1, filesize=100, tx_duration=1.0),
                 _file(file_id=2, filesize=200, tx_duration=2.0)]
        _compute_file_metrics(files)
        assert files[0]["throughput_wire"] == pytest.approx(100.0)
        assert files[1]["throughput_wire"] == pytest.approx(100.0)

    def test_finish_before_start_throughput_wall_zero(self):
        # wall_s < 0 → clamped; throughput_wall must also be 0.0
        f = _file(filesize=1000,
                  start_time="2026-01-01T00:00:10",
                  finish_time="2026-01-01T00:00:00")
        _compute_file_metrics([f])
        assert f["wall_duration_s"] == 0.0
        assert f["throughput_wall"] == 0.0


# ---------------------------------------------------------------------------
# _aggregate_throughput
# ---------------------------------------------------------------------------

class TestAggregateThroughput:
    def test_single_file(self):
        f = _file(filesize=1000, start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10")
        result = _aggregate_throughput([f])
        assert result == pytest.approx(100.0)

    def test_campaign_window_spans_all_files(self):
        # file1: 0s-5s, file2: 3s-8s → campaign 0s-8s = 8s; total_bytes = 800
        f1 = _file(file_id=1, filesize=400, start_time="2026-01-01T00:00:00",
                   finish_time="2026-01-01T00:00:05")
        f2 = _file(file_id=2, filesize=400, start_time="2026-01-01T00:00:03",
                   finish_time="2026-01-01T00:00:08")
        result = _aggregate_throughput([f1, f2])
        assert result == pytest.approx(800.0 / 8.0)

    def test_empty_list_returns_none(self):
        assert _aggregate_throughput([]) is None

    def test_no_valid_timestamps_returns_none(self):
        f = _file(start_time="", finish_time="")
        assert _aggregate_throughput([f]) is None

    def test_same_start_and_finish_returns_none(self):
        f = _file(filesize=100, start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:00")
        assert _aggregate_throughput([f]) is None

    def test_zero_filesize_with_valid_timestamps_returns_zero(self):
        # all bytes are 0 → 0.0 bytes/s (not None)
        f = _file(filesize=0, start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10")
        result = _aggregate_throughput([f])
        assert result == pytest.approx(0.0)

    def test_untimed_bytes_included_in_total(self):
        # f1 has timestamps, f2 does not; both bytes count
        f1 = _file(file_id=1, filesize=500,
                   start_time="2026-01-01T00:00:00",
                   finish_time="2026-01-01T00:00:10")
        f2 = _file(file_id=2, filesize=500, start_time="", finish_time="")
        result = _aggregate_throughput([f1, f2])
        # wall = 10s, bytes = 1000 (both files)
        assert result == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# _estimate_concurrency
# ---------------------------------------------------------------------------

class TestEstimateConcurrency:
    def test_empty_returns_empty(self):
        assert _estimate_concurrency([]) == []

    def test_no_valid_timestamps_returns_empty(self):
        f = _file(start_time="", finish_time="")
        assert _estimate_concurrency([f]) == []

    def test_single_file_active_at_start(self):
        f = _file(start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:05")
        timeline = _estimate_concurrency([f])
        assert len(timeline) > 0
        # file is active at its start epoch
        first = timeline[0]
        assert first["active"] == 1

    def test_two_overlapping_files_peak_concurrency_two(self):
        f1 = _file(file_id=1, start_time="2026-01-01T00:00:00",
                   finish_time="2026-01-01T00:00:10")
        f2 = _file(file_id=2, start_time="2026-01-01T00:00:03",
                   finish_time="2026-01-01T00:00:07")
        timeline = _estimate_concurrency([f1, f2])
        peak = max(b["active"] for b in timeline)
        assert peak == 2

    def test_sequential_files_peak_concurrency_one(self):
        f1 = _file(file_id=1, start_time="2026-01-01T00:00:00",
                   finish_time="2026-01-01T00:00:05")
        f2 = _file(file_id=2, start_time="2026-01-01T00:00:05",
                   finish_time="2026-01-01T00:00:10")
        timeline = _estimate_concurrency([f1, f2])
        peak = max(b["active"] for b in timeline)
        assert peak == 1

    def test_bucket_keys_present(self):
        f = _file(start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:02")
        timeline = _estimate_concurrency([f])
        for bucket in timeline:
            assert "t" in bucket
            assert "active" in bucket
            assert "bytes_in_flight" in bucket
            assert "throughput_bytes_s" in bucket

    def test_t_values_are_integers(self):
        f = _file(start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:03")
        timeline = _estimate_concurrency([f])
        for bucket in timeline:
            assert isinstance(bucket["t"], int)

    def test_bucket_width_non_default(self):
        # bucket_width_s=5 → fewer buckets than default
        f = _file(start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10")
        tl_default = _estimate_concurrency([f], bucket_width_s=1)
        tl_wide = _estimate_concurrency([f], bucket_width_s=5)
        assert len(tl_wide) < len(tl_default)

    def test_zero_duration_files_all_buckets_zero_active(self):
        # start == finish → half-open interval means no bucket has active > 0
        f = _file(start_time="2026-01-01T00:00:05",
                  finish_time="2026-01-01T00:00:05")
        timeline = _estimate_concurrency([f])
        # Only one bucket at t_min == t_max; active = 0 (s <= t < e fails when s==e)
        assert all(b["active"] == 0 for b in timeline)

    # --- bytes_in_flight ---

    def test_bytes_in_flight_single_file(self):
        # 1000-byte file active for seconds [0, 5)
        f = _file(filesize=1000,
                  start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:05")
        timeline = _estimate_concurrency([f])
        # All seconds in [0,5) should have bytes_in_flight == 1000
        active_buckets = [b for b in timeline if b["active"] > 0]
        assert all(b["bytes_in_flight"] == 1000 for b in active_buckets)

    def test_bytes_in_flight_two_overlapping_files(self):
        # f1: 500 bytes, [0,10); f2: 300 bytes, [3,7)
        # seconds [3,7): both active → bytes_in_flight == 800
        f1 = _file(file_id=1, filesize=500,
                   start_time="2026-01-01T00:00:00",
                   finish_time="2026-01-01T00:00:10")
        f2 = _file(file_id=2, filesize=300,
                   start_time="2026-01-01T00:00:03",
                   finish_time="2026-01-01T00:00:07")
        timeline = _estimate_concurrency([f1, f2])
        # Second at relative offset 3 (both active)
        by_t = {b["t"]: b for b in timeline}
        t_base = min(by_t)
        assert by_t[t_base + 3]["bytes_in_flight"] == 800
        # Second at relative offset 0 (only f1 active)
        assert by_t[t_base]["bytes_in_flight"] == 500

    def test_bytes_in_flight_zero_filesize_not_counted(self):
        f = _file(filesize=0,
                  start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:05")
        timeline = _estimate_concurrency([f])
        assert all(b["bytes_in_flight"] == 0 for b in timeline)

    # --- throughput_bytes_s ---

    def test_throughput_single_file_constant_rate(self):
        # 100-byte file, 10 s wall → rate = 10 B/s
        f = _file(filesize=100,
                  start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10")
        timeline = _estimate_concurrency([f])
        active_buckets = [b for b in timeline if b["active"] > 0]
        for b in active_buckets:
            assert b["throughput_bytes_s"] == pytest.approx(10.0)

    def test_throughput_zero_for_inactive_seconds(self):
        f = _file(filesize=100,
                  start_time="2026-01-01T00:00:02",
                  finish_time="2026-01-01T00:00:05")
        timeline = _estimate_concurrency([f])
        # No bucket should have throughput > 0 when active == 0
        for b in timeline:
            if b["active"] == 0:
                assert b["throughput_bytes_s"] == pytest.approx(0.0)

    def test_throughput_two_concurrent_files_additive(self):
        # f1: 60 B / 10 s = 6 B/s; f2: 40 B / 10 s = 4 B/s
        # Together: 10 B/s while both active
        f1 = _file(file_id=1, filesize=60,
                   start_time="2026-01-01T00:00:00",
                   finish_time="2026-01-01T00:00:10")
        f2 = _file(file_id=2, filesize=40,
                   start_time="2026-01-01T00:00:00",
                   finish_time="2026-01-01T00:00:10")
        timeline = _estimate_concurrency([f1, f2])
        active_buckets = [b for b in timeline if b["active"] == 2]
        assert len(active_buckets) > 0
        for b in active_buckets:
            assert b["throughput_bytes_s"] == pytest.approx(10.0)

    def test_throughput_zero_duration_file_excluded(self):
        f = _file(filesize=1000,
                  start_time="2026-01-01T00:00:05",
                  finish_time="2026-01-01T00:00:05")
        timeline = _estimate_concurrency([f])
        assert all(b["throughput_bytes_s"] == pytest.approx(0.0) for b in timeline)


# ---------------------------------------------------------------------------
# _retry_distribution
# ---------------------------------------------------------------------------

class TestRetryDistribution:
    def test_empty_returns_empty_dict(self):
        assert _retry_distribution([]) == {}

    def test_single_file_one_retry(self):
        dist = _retry_distribution([_retry(file_id=1)])
        assert dist == {"1": 1}

    def test_single_file_two_retries(self):
        dist = _retry_distribution([_retry(file_id=1), _retry(file_id=1)])
        assert dist == {"2": 1}

    def test_two_files_one_retry_each(self):
        dist = _retry_distribution([_retry(file_id=1), _retry(file_id=2)])
        assert dist == {"1": 2}

    def test_mixed_retry_counts(self):
        records = [
            _retry(file_id=1), _retry(file_id=1),  # file 1: 2 retries
            _retry(file_id=2),                       # file 2: 1 retry
        ]
        dist = _retry_distribution(records)
        assert dist["2"] == 1
        assert dist["1"] == 1


# ---------------------------------------------------------------------------
# _categorise_failures
# ---------------------------------------------------------------------------

class TestCategoriseFailures:
    def test_empty_returns_empty_dict(self):
        assert _categorise_failures([]) == {}

    def test_single_failure_with_reason(self):
        f = _file(file_state="FAILED", reason="checksum mismatch")
        result = _categorise_failures([f])
        assert result == {"checksum mismatch": 1}

    def test_empty_reason_becomes_unknown(self):
        f = _file(file_state="FAILED", reason="")
        result = _categorise_failures([f])
        assert result == {"UNKNOWN": 1}

    def test_none_reason_becomes_unknown(self):
        f = _file(file_state="FAILED")
        f["reason"] = None
        result = _categorise_failures([f])
        assert result == {"UNKNOWN": 1}

    def test_reason_whitespace_stripped(self):
        f = _file(file_state="FAILED", reason="  timeout  ")
        result = _categorise_failures([f])
        assert "timeout" in result

    def test_multiple_same_reason_counted(self):
        files = [_file(file_state="FAILED", reason="timeout")] * 3
        result = _categorise_failures(files)
        assert result["timeout"] == 3

    def test_multiple_different_reasons(self):
        files = [
            _file(file_id=1, file_state="FAILED", reason="timeout"),
            _file(file_id=2, file_state="FAILED", reason="network error"),
            _file(file_id=3, file_state="CANCELED", reason="timeout"),
        ]
        result = _categorise_failures(files)
        assert result["timeout"] == 2
        assert result["network error"] == 1


# ---------------------------------------------------------------------------
# compute — integration-level
# ---------------------------------------------------------------------------

class TestCompute:
    def test_returns_dict_with_required_keys(self):
        files = [_file(file_id=1)]
        snap = compute(files, [], _config(), "run-001")
        required = [
            "run_id", "test_label", "generated_at",
            "total_files", "finished", "failed", "canceled",
            "not_used", "staging_unsupported",
            "success_rate", "failure_rate", "threshold_passed",
            "throughput_mean", "throughput_p50", "throughput_p90",
            "throughput_p95", "throughput_p99", "throughput_max",
            "aggregate_throughput_bytes_per_s",
            "duration_mean_s", "duration_p50_s", "duration_p90_s", "duration_p95_s",
            "total_retries", "files_with_retries", "retry_rate", "retry_distribution",
            "peak_concurrency", "mean_concurrency", "concurrency_timeline",
            "failure_reasons", "throughput_timeline", "ssl_verify_disabled",
        ]
        for key in required:
            assert key in snap, "Missing key: {}".format(key)

    def test_run_id_and_label_set(self):
        snap = compute([_file()], [], _config(test_label="perf-test"), "run-xyz")
        assert snap["run_id"] == "run-xyz"
        assert snap["test_label"] == "perf-test"

    def test_counts_correct(self):
        files = [
            _file(file_id=1, file_state="FINISHED"),
            _file(file_id=2, file_state="FAILED"),
            _file(file_id=3, file_state="CANCELED"),
            _file(file_id=4, file_state="NOT_USED"),
            _file(file_id=5, file_state="STAGING"),
        ]
        snap = compute(files, [], _config(), "r")
        assert snap["total_files"] == 5
        assert snap["finished"] == 1
        assert snap["failed"] == 1
        assert snap["canceled"] == 1
        assert snap["not_used"] == 1
        assert snap["staging_unsupported"] == 1

    def test_success_rate_calculation(self):
        # 3 finished, 1 failed, 1 not_used → eligible=4, rate=0.75
        files = [
            _file(file_id=i, file_state="FINISHED") for i in range(1, 4)
        ] + [
            _file(file_id=4, file_state="FAILED"),
            _file(file_id=5, file_state="NOT_USED"),
        ]
        snap = compute(files, [], _config(min_threshold=0.95), "r")
        assert snap["success_rate"] == pytest.approx(0.75)
        assert snap["threshold_passed"] is False

    def test_success_rate_all_finished(self):
        files = [_file(file_id=i, file_state="FINISHED") for i in range(1, 4)]
        snap = compute(files, [], _config(min_threshold=0.95), "r")
        assert snap["success_rate"] == pytest.approx(1.0)
        assert snap["threshold_passed"] is True

    def test_success_rate_zero_when_no_eligible(self):
        files = [_file(file_state="NOT_USED")]
        snap = compute(files, [], _config(), "r")
        assert snap["success_rate"] == 0.0

    def test_throughput_stats_populated_for_finished(self):
        files = [_file(file_id=i, file_state="FINISHED", throughput=float(i * 100))
                 for i in range(1, 6)]
        snap = compute(files, [], _config(), "r")
        assert snap["throughput_mean"] is not None
        assert snap["throughput_p50"] is not None
        assert snap["throughput_max"] is not None

    def test_throughput_none_when_no_finished(self):
        files = [_file(file_state="FAILED", throughput=0.0)]
        snap = compute(files, [], _config(), "r")
        assert snap["throughput_mean"] is None

    def test_wire_throughput_fallback(self):
        # throughput=0, but filesize/tx_duration available
        f = _file(file_state="FINISHED", throughput=0.0, filesize=1000, tx_duration=2.0)
        snap = compute([f], [], _config(), "r")
        # wire throughput = 500.0 — should be included in stats
        assert snap["throughput_mean"] is not None
        assert snap["throughput_mean"] == pytest.approx(500.0)

    def test_duration_stats_populated(self):
        files = [
            _file(file_id=1, file_state="FINISHED",
                  start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10"),
            _file(file_id=2, file_state="FINISHED",
                  start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:20"),
        ]
        snap = compute(files, [], _config(), "r")
        assert snap["duration_mean_s"] is not None
        assert snap["duration_p50_s"] is not None

    def test_duration_none_when_no_valid_timestamps(self):
        f = _file(file_state="FINISHED", start_time="", finish_time="")
        snap = compute([f], [], _config(), "r")
        assert snap["duration_mean_s"] is None

    def test_retry_stats(self):
        files = [_file(file_id=1, file_state="FINISHED"),
                 _file(file_id=2, file_state="FINISHED")]
        retries = [_retry(file_id=1), _retry(file_id=1)]  # 2 retries on file 1
        snap = compute(files, retries, _config(), "r")
        assert snap["total_retries"] == 2
        assert snap["files_with_retries"] == 1
        assert snap["retry_rate"] == pytest.approx(0.5)

    def test_retry_stats_no_retries(self):
        snap = compute([_file()], [], _config(), "r")
        assert snap["total_retries"] == 0
        assert snap["files_with_retries"] == 0
        assert snap["retry_rate"] == 0.0

    def test_concurrency_timeline_populated(self):
        f = _file(file_state="FINISHED", start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:05")
        snap = compute([f], [], _config(), "r")
        assert isinstance(snap["concurrency_timeline"], list)
        assert len(snap["concurrency_timeline"]) > 0

    def test_peak_concurrency_set(self):
        files = [
            _file(file_id=1, file_state="FINISHED",
                  start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10"),
            _file(file_id=2, file_state="FINISHED",
                  start_time="2026-01-01T00:00:02",
                  finish_time="2026-01-01T00:00:08"),
        ]
        snap = compute(files, [], _config(), "r")
        assert snap["peak_concurrency"] >= 2

    def test_failure_reasons_populated(self):
        files = [
            _file(file_id=1, file_state="FAILED", reason="timeout"),
            _file(file_id=2, file_state="CANCELED", reason=""),
        ]
        snap = compute(files, [], _config(), "r")
        assert snap["failure_reasons"]["timeout"] == 1
        assert snap["failure_reasons"]["UNKNOWN"] == 1

    def test_ssl_verify_disabled_default_false(self):
        snap = compute([_file()], [], _config(), "r")
        assert snap["ssl_verify_disabled"] is False

    def test_throughput_timeline_empty_list(self):
        snap = compute([_file()], [], _config(), "r")
        assert snap["throughput_timeline"] == []

    def test_file_records_updated_in_place(self):
        f = _file(filesize=1000, tx_duration=4.0)
        compute([f], [], _config(), "r")
        # _compute_file_metrics called inside compute — wire should be set
        assert f["throughput_wire"] == pytest.approx(250.0)

    def test_empty_file_records(self):
        snap = compute([], [], _config(), "r")
        assert snap["total_files"] == 0
        assert snap["success_rate"] == 0.0

    def test_failure_rate_calculation(self):
        files = [
            _file(file_id=1, file_state="FINISHED"),
            _file(file_id=2, file_state="FAILED"),
            _file(file_id=3, file_state="CANCELED"),
        ]
        snap = compute(files, [], _config(), "r")
        # eligible=3, failed+canceled=2
        assert snap["failure_rate"] == pytest.approx(2.0 / 3.0)

    def test_generated_at_is_string(self):
        snap = compute([_file()], [], _config(), "r")
        assert isinstance(snap["generated_at"], str)
        assert "T" in snap["generated_at"]

    def test_config_without_retry_section(self):
        cfg = {"run": {"test_label": "t"}}
        snap = compute([_file()], [], cfg, "r")
        assert snap["threshold_passed"] is True  # default threshold 0.95, 1 finished

    def test_min_threshold_boundary_passed(self):
        files = [_file(file_id=i, file_state="FINISHED") for i in range(95)]
        files += [_file(file_id=i + 95, file_state="FAILED") for i in range(5)]
        snap = compute(files, [], _config(min_threshold=0.95), "r")
        assert snap["success_rate"] == pytest.approx(0.95)
        assert snap["threshold_passed"] is True

    def test_aggregate_throughput_included(self):
        f = _file(file_state="FINISHED", filesize=1000,
                  start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10")
        snap = compute([f], [], _config(), "r")
        assert snap["aggregate_throughput_bytes_per_s"] == pytest.approx(100.0)

    def test_retry_rate_denominator_is_total_not_eligible(self):
        # 1 NOT_USED, 1 FINISHED with 1 retry → total=2, rate=0.5 not 1.0
        files = [
            _file(file_id=1, file_state="FINISHED"),
            _file(file_id=2, file_state="NOT_USED"),
        ]
        retries = [_retry(file_id=1)]
        snap = compute(files, retries, _config(), "r")
        assert snap["files_with_retries"] == 1
        assert snap["retry_rate"] == pytest.approx(0.5)

    def test_peak_concurrency_zero_for_zero_duration_files(self):
        # Files where start == finish: half-open interval → 0 active in all buckets
        f = _file(file_state="FINISHED",
                  start_time="2026-01-01T00:00:05",
                  finish_time="2026-01-01T00:00:05")
        snap = compute([f], [], _config(), "r")
        assert snap["peak_concurrency"] == 0


# ---------------------------------------------------------------------------
# _compute_timeseries
# ---------------------------------------------------------------------------

class TestComputeTimeseries:
    def _f(self, start, finish, filesize=1000000):
        """Build a minimal finished-file dict for timeseries input."""
        return {
            "file_state": "FINISHED",
            "filesize": filesize,
            "start_time": start,
            "finish_time": finish,
        }

    def test_empty_input_returns_empty(self):
        assert _compute_timeseries([], 60) == []

    def test_single_file_single_bucket(self):
        # 100-byte file transferred in 10 s → rate = 10 B/s
        # Fits entirely in one 60 s bucket → agg = 10*10/60 ≈ 1.667 B/s
        f = self._f("2026-01-01T00:00:00", "2026-01-01T00:00:10", filesize=100)
        buckets = _compute_timeseries([f], 60)
        assert len(buckets) == 1
        b = buckets[0]
        assert b["active_transfers"] == 1
        assert b["aggregate_throughput_bytes_s"] == pytest.approx(100.0 / 60.0)

    def test_bucket_start_end_iso_format(self):
        f = self._f("2026-01-01T00:00:00", "2026-01-01T00:00:10", filesize=60)
        buckets = _compute_timeseries([f], 60)
        assert len(buckets) == 1
        assert "T" in buckets[0]["bucket_start"]
        assert buckets[0]["bucket_start"].endswith("Z")
        assert "T" in buckets[0]["bucket_end"]
        assert buckets[0]["bucket_end"].endswith("Z")

    def test_two_files_non_overlapping_two_buckets(self):
        # File 1: 00:00:00 → 00:00:30 (30 s, 300 B → rate 10 B/s)
        # File 2: 00:01:10 → 00:01:40 (30 s, 300 B → rate 10 B/s)
        # Both fit in separate 60 s buckets; no overlap
        f1 = self._f("2026-01-01T00:00:00", "2026-01-01T00:00:30", filesize=300)
        f2 = self._f("2026-01-01T00:01:10", "2026-01-01T00:01:40", filesize=300)
        buckets = _compute_timeseries([f1, f2], 60)
        # There should be at least 2 buckets
        assert len(buckets) >= 2
        active_counts = [b["active_transfers"] for b in buckets]
        assert max(active_counts) == 1  # never concurrent

    def test_two_files_overlapping_same_bucket(self):
        # Both files run simultaneously; concurrency should be 2
        f1 = self._f("2026-01-01T00:00:00", "2026-01-01T00:00:30", filesize=300)
        f2 = self._f("2026-01-01T00:00:00", "2026-01-01T00:00:30", filesize=300)
        buckets = _compute_timeseries([f1, f2], 60)
        assert len(buckets) == 1
        assert buckets[0]["active_transfers"] == 2
        # Each contributes 300/30 B/s * 30 s = 300 B; total 600 B / 60 s = 10 B/s
        assert buckets[0]["aggregate_throughput_bytes_s"] == pytest.approx(10.0)

    def test_file_spanning_two_buckets(self):
        # Anchor t_min with an early file so that f2 straddles a bucket boundary.
        # f1: 00:00:00 → 00:00:10 (t_min anchor, small)
        # f2: 00:00:50 → 00:01:20 (straddles the 60 s boundary)
        #   rate = 300 B / 30 s = 10 B/s
        #   bucket 0 overlap: 60-50 = 10 s → 10*10/60 ≈ 1.667 B/s
        #   bucket 1 overlap: 80-60 = 20 s → 10*20/60 ≈ 3.333 B/s
        f1 = self._f("2026-01-01T00:00:00", "2026-01-01T00:00:10", filesize=10)
        f2 = self._f("2026-01-01T00:00:50", "2026-01-01T00:01:20", filesize=300)
        buckets = _compute_timeseries([f1, f2], 60)
        assert len(buckets) >= 2
        assert buckets[0]["active_transfers"] >= 1
        assert buckets[1]["active_transfers"] == 1
        assert buckets[1]["aggregate_throughput_bytes_s"] == pytest.approx(300.0 / 30 * 20 / 60.0)

    def test_zero_filesize_excluded(self):
        f = self._f("2026-01-01T00:00:00", "2026-01-01T00:00:10", filesize=0)
        assert _compute_timeseries([f], 60) == []

    def test_zero_duration_excluded(self):
        f = self._f("2026-01-01T00:00:05", "2026-01-01T00:00:05", filesize=1000)
        assert _compute_timeseries([f], 60) == []

    def test_missing_start_time_excluded(self):
        f = {"file_state": "FINISHED", "filesize": 1000,
             "start_time": None, "finish_time": "2026-01-01T00:00:10"}
        assert _compute_timeseries([f], 60) == []

    def test_missing_finish_time_excluded(self):
        f = {"file_state": "FINISHED", "filesize": 1000,
             "start_time": "2026-01-01T00:00:00", "finish_time": None}
        assert _compute_timeseries([f], 60) == []

    def test_bucket_width_respected(self):
        # 90-second file, 90 B → rate = 1 B/s
        # With 30 s buckets: spans 3 full buckets (0-30, 30-60, 60-90), each gets 1 B/s
        # n_buckets formula may add an empty trailing bucket; check at least 3 active ones
        f = self._f("2026-01-01T00:00:00", "2026-01-01T00:01:30", filesize=90)
        buckets = _compute_timeseries([f], 30)
        assert len(buckets) >= 3
        active = [b for b in buckets if b["active_transfers"] > 0]
        assert len(active) == 3
        for b in active:
            assert b["aggregate_throughput_bytes_s"] == pytest.approx(1.0)

    def test_compute_returns_timeseries_key(self):
        cfg = _config()
        cfg["output"] = {"timeseries_bucket_s": 60}
        f = _file(file_state="FINISHED",
                  start_time="2026-01-01T00:00:00",
                  finish_time="2026-01-01T00:00:10")
        snap = compute([f], [], cfg, "r")
        assert "timeseries" in snap
        assert isinstance(snap["timeseries"], list)
