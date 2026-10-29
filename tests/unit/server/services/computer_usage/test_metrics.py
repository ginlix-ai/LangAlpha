"""The incremental fold of provider samples into a run's metric columns.

A run is folded every cycle for as long as it runs, so every aggregate has to
come out the same whether its samples arrive in one batch or many, and a
batch folded twice must count nothing twice.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ptc_agent.core.sandbox.runtime import MetricSample
from src.server.services.computer_usage.metrics import (
    CPU_BUCKETS,
    RunMetrics,
    cpu_bucket,
    fold_samples,
    histogram_percentile,
)

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)
GIB = 1024**3


def _sample(i: int, cpu: float = 10.0, **kw) -> MetricSample:
    return MetricSample(timestamp=T0 + timedelta(seconds=5 * i), cpu_used_pct=cpu, **kw)


def test_no_samples_leaves_the_run_as_it_was():
    current = RunMetrics()
    assert fold_samples(current, []) is current


def test_one_batch_sets_every_aggregate():
    samples = [
        _sample(1, 10.0, mem_used_bytes=2 * GIB, mem_cache_bytes=GIB,
                mem_total_bytes=4 * GIB, disk_used_bytes=3 * GIB,
                disk_total_bytes=10 * GIB),
        _sample(2, 30.0, mem_used_bytes=3 * GIB, mem_cache_bytes=2 * GIB,
                mem_total_bytes=4 * GIB, disk_used_bytes=2 * GIB,
                disk_total_bytes=10 * GIB),
    ]

    folded = fold_samples(RunMetrics(), samples)

    assert folded.sample_count == 2
    assert folded.cpu_avg_pct == pytest.approx(20.0)
    assert folded.cpu_max_pct == 30.0
    assert folded.mem_peak_bytes == 3 * GIB
    # The peak of used less cache, not used peak less cache peak.
    assert folded.mem_peak_excl_cache_bytes == GIB
    assert folded.mem_total_bytes == 4 * GIB
    assert folded.disk_peak_bytes == 3 * GIB
    assert folded.disk_total_bytes == 10 * GIB
    assert folded.metrics_through == samples[-1].timestamp


def test_many_batches_fold_to_what_one_batch_does():
    samples = [_sample(i, cpu=float((i * 37) % 101), mem_used_bytes=i * 1000)
               for i in range(1, 41)]

    whole = fold_samples(RunMetrics(), samples)
    piecewise = RunMetrics()
    for start in range(0, 40, 7):
        piecewise = fold_samples(piecewise, samples[start:start + 7])

    assert piecewise.sample_count == whole.sample_count == 40
    assert piecewise.cpu_avg_pct == pytest.approx(whole.cpu_avg_pct)
    assert piecewise.cpu_pct_histogram == whole.cpu_pct_histogram
    assert piecewise.cpu_p95_pct == whole.cpu_p95_pct
    assert piecewise.mem_peak_bytes == whole.mem_peak_bytes
    assert piecewise.metrics_through == whole.metrics_through


def test_a_batch_folded_again_counts_nothing_twice():
    batch = [_sample(1, 50.0), _sample(2, 70.0)]
    once = fold_samples(RunMetrics(), batch)

    assert fold_samples(once, batch) == once
    # An overlapping batch counts only what is past the cursor.
    overlapped = fold_samples(once, [_sample(2, 70.0), _sample(3, 90.0)])
    assert overlapped.sample_count == 3
    assert overlapped.cpu_avg_pct == pytest.approx(70.0)


def test_folding_resumes_from_a_stored_row():
    first = fold_samples(RunMetrics(), [_sample(1, 20.0), _sample(2, 40.0)])
    row = first.as_columns()
    # Postgres hands the histogram back as a list.
    assert isinstance(row["cpu_pct_histogram"], list)

    resumed = fold_samples(RunMetrics.from_row(row), [_sample(3, 60.0)])

    assert resumed.sample_count == 3
    assert resumed.cpu_avg_pct == pytest.approx(40.0)
    assert resumed.cpu_max_pct == 60.0


def test_a_reading_the_provider_did_not_give_keeps_its_column_null():
    """An E2B template built before envd 0.2.4 reports no disk at all."""
    folded = fold_samples(RunMetrics(), [_sample(1, mem_used_bytes=GIB)])

    assert folded.disk_peak_bytes is None
    assert folded.disk_total_bytes is None
    assert folded.mem_total_bytes is None
    # With no cache reported, all of the memory in use counts.
    assert folded.mem_peak_excl_cache_bytes == GIB


def test_cache_larger_than_use_reads_as_zero_not_negative():
    folded = fold_samples(
        RunMetrics(), [_sample(1, mem_used_bytes=GIB, mem_cache_bytes=2 * GIB)]
    )
    assert folded.mem_peak_excl_cache_bytes == 0


def test_a_later_sample_that_reads_lower_never_lowers_a_peak():
    folded = fold_samples(
        RunMetrics(),
        [_sample(1, 90.0, disk_used_bytes=5 * GIB),
         _sample(2, 5.0, disk_used_bytes=GIB)],
    )
    assert folded.cpu_max_pct == 90.0
    assert folded.disk_peak_bytes == 5 * GIB


@pytest.mark.parametrize(
    "pct, bucket",
    [(0.0, 0), (4.99, 0), (5.0, 1), (94.9, 18), (95.0, 19), (100.0, 19),
     (180.0, 19), (-1.0, 0)],
)
def test_cpu_readings_land_in_five_point_buckets(pct, bucket):
    assert cpu_bucket(pct) == bucket


def test_p95_is_the_upper_edge_of_the_bucket_holding_the_95th_sample():
    # 95 samples at 10% and 5 at 80%: the 95th sample is still in 10-15.
    samples = [_sample(i, 10.0) for i in range(95)] + [
        _sample(100 + i, 80.0) for i in range(5)
    ]
    folded = fold_samples(RunMetrics(), samples)

    assert folded.cpu_p95_pct == 15.0
    # One more busy sample moves the 95th into the 80-85 bucket.
    busier = fold_samples(folded, [_sample(200, 80.0)])
    assert busier.cpu_p95_pct == 85.0


def test_an_empty_histogram_has_no_percentile():
    assert histogram_percentile((0,) * CPU_BUCKETS, 0.95) is None
