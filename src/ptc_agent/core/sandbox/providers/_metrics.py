"""Metric-sample normalization, shared by the providers that keep history.

Daytona and E2B both fill what they did not measure with 0 and hand back
whatever window they like, so the same four fixes apply to either: a missing
size becomes None, a timestamp becomes tz-aware UTC, the window is enforced
here rather than trusted to the SDK, and the result is ordered.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from ptc_agent.core.sandbox.runtime import MetricSample


def _size(value: Any) -> int | None:
    # 0 is how both SDKs say "not reported" for a total, a count or disk use
    # (a disk is never empty); memory in use and cache keep a real 0.
    return int(value) if value else None


def _reading(value: Any) -> int | None:
    return None if value is None else int(value)


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def normalize_samples(
    raw: Iterable[Any],
    since: datetime,
    until: datetime | None,
    *,
    used_excludes_cache: bool = False,
) -> list[MetricSample]:
    """*raw* (SDK metric objects) as samples with ``since < timestamp <= until``.

    The lower bound is strict even though both SDKs take it inclusive or
    floored to whole seconds; a caller that resumes from the last sample it
    kept must not see that sample again. *used_excludes_cache* is for a
    provider whose memory in use leaves the page cache out, which is added
    back so every provider's ``mem_used_bytes`` means the same.
    """
    samples = []
    for r in raw:
        ts = _utc(r.timestamp)
        if ts <= since or (until is not None and ts > until):
            continue
        cpu = float(r.cpu_used_pct)
        if not math.isfinite(cpu):
            # The fold buckets every reading; one NaN would stop the run's folds.
            continue
        mem_used = _reading(r.mem_used)
        mem_cache = _reading(r.mem_cache)
        if used_excludes_cache and mem_used is not None:
            mem_used += mem_cache or 0
        samples.append(
            MetricSample(
                timestamp=ts,
                cpu_used_pct=cpu,
                cpu_count=_size(r.cpu_count),
                mem_used_bytes=mem_used,
                mem_cache_bytes=mem_cache,
                mem_total_bytes=_size(r.mem_total),
                disk_used_bytes=_size(r.disk_used),
                disk_total_bytes=_size(r.disk_total),
            )
        )
    return sorted(samples, key=lambda s: s.timestamp)
