"""Folding provider samples into a run's aggregates.

The fold is incremental, so a run's whole history never has to be fetched
again: every aggregate is one that a batch of new samples can update from the
stored value alone. The 95th percentile is the exception, which an exact
answer needs every sample for, so it is read off a histogram of 5-point CPU
buckets kept on the run instead, and overstates by less than one bucket.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Optional

from ptc_agent.core.sandbox.runtime import MetricSample

CPU_BUCKET_PCT = 5
CPU_BUCKETS = 100 // CPU_BUCKET_PCT
CPU_PERCENTILE = 0.95


@dataclass(frozen=True)
class RunMetrics:
    """A run's metric columns, as stored."""

    sample_count: int = 0
    cpu_avg_pct: Optional[float] = None
    cpu_p95_pct: Optional[float] = None
    cpu_max_pct: Optional[float] = None
    cpu_pct_histogram: Optional[tuple[int, ...]] = None
    mem_peak_bytes: Optional[int] = None
    mem_peak_excl_cache_bytes: Optional[int] = None
    mem_total_bytes: Optional[int] = None
    disk_peak_bytes: Optional[int] = None
    disk_total_bytes: Optional[int] = None
    metrics_through: Optional[datetime] = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "RunMetrics":
        histogram = row.get("cpu_pct_histogram")
        return cls(
            sample_count=int(row.get("sample_count") or 0),
            cpu_avg_pct=row.get("cpu_avg_pct"),
            cpu_p95_pct=row.get("cpu_p95_pct"),
            cpu_max_pct=row.get("cpu_max_pct"),
            cpu_pct_histogram=tuple(histogram) if histogram is not None else None,
            mem_peak_bytes=row.get("mem_peak_bytes"),
            mem_peak_excl_cache_bytes=row.get("mem_peak_excl_cache_bytes"),
            mem_total_bytes=row.get("mem_total_bytes"),
            disk_peak_bytes=row.get("disk_peak_bytes"),
            disk_total_bytes=row.get("disk_total_bytes"),
            metrics_through=row.get("metrics_through"),
        )

    def as_columns(self) -> dict[str, Any]:
        columns = {
            name: getattr(self, name) for name in self.__dataclass_fields__
        }
        if self.cpu_pct_histogram is not None:
            columns["cpu_pct_histogram"] = list(self.cpu_pct_histogram)
        return columns


def cpu_bucket(pct: float) -> int:
    """The histogram bucket of a CPU reading; the last bucket holds 95 to 100 and over."""
    return min(max(int(pct // CPU_BUCKET_PCT), 0), CPU_BUCKETS - 1)


def histogram_percentile(histogram: tuple[int, ...], q: float) -> Optional[float]:
    """The upper edge of the bucket holding the q-th sample, None with no samples."""
    total = sum(histogram)
    if total == 0:
        return None
    rank = q * total
    seen = 0
    for i, count in enumerate(histogram):
        seen += count
        if seen >= rank:
            return float((i + 1) * CPU_BUCKET_PCT)
    return 100.0


def _peak(current: Optional[int], new: Optional[int]) -> Optional[int]:
    if new is None:
        return current
    return new if current is None else max(current, new)


def fold_samples(current: RunMetrics, samples: Iterable[MetricSample]) -> RunMetrics:
    """The run's metrics with *samples* folded in.

    A sample at or before ``metrics_through`` is one a fold already counted
    and is skipped, so folding the same batch twice changes nothing.
    """
    fresh = [
        s
        for s in samples
        if current.metrics_through is None or s.timestamp > current.metrics_through
    ]
    if not fresh:
        return current

    histogram = list(current.cpu_pct_histogram or (0,) * CPU_BUCKETS)
    count = current.sample_count
    cpu_total = (current.cpu_avg_pct or 0.0) * count
    cpu_max = current.cpu_max_pct
    mem_peak = current.mem_peak_bytes
    mem_excl = current.mem_peak_excl_cache_bytes
    mem_total = current.mem_total_bytes
    disk_peak = current.disk_peak_bytes
    disk_total = current.disk_total_bytes
    through = current.metrics_through

    for s in fresh:
        count += 1
        cpu_total += s.cpu_used_pct
        cpu_max = s.cpu_used_pct if cpu_max is None else max(cpu_max, s.cpu_used_pct)
        histogram[cpu_bucket(s.cpu_used_pct)] += 1
        mem_peak = _peak(mem_peak, s.mem_used_bytes)
        if s.mem_used_bytes is not None:
            excl = max(s.mem_used_bytes - (s.mem_cache_bytes or 0), 0)
            mem_excl = _peak(mem_excl, excl)
        mem_total = _peak(mem_total, s.mem_total_bytes)
        disk_peak = _peak(disk_peak, s.disk_used_bytes)
        disk_total = _peak(disk_total, s.disk_total_bytes)
        through = s.timestamp if through is None else max(through, s.timestamp)

    frozen = tuple(histogram)
    return replace(
        current,
        sample_count=count,
        cpu_avg_pct=cpu_total / count,
        cpu_p95_pct=histogram_percentile(frozen, CPU_PERCENTILE),
        cpu_max_pct=cpu_max,
        cpu_pct_histogram=frozen,
        mem_peak_bytes=mem_peak,
        mem_peak_excl_cache_bytes=mem_excl,
        mem_total_bytes=mem_total,
        disk_peak_bytes=disk_peak,
        disk_total_bytes=disk_total,
        metrics_through=through,
    )
