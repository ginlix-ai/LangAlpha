"""ObservableGauge callbacks reporting Redis pool occupancy at export time.

An exhausted pool is only diagnosable in hindsight if something was recording
occupancy while it filled — the errors name the victims, never the holder. The
three pools saturate independently (a healthy cache pool says nothing about the
reader or pub/sub pools), so every series carries which pool it came from.

Occupancy lives in private redis-py bookkeeping, so each read is guarded: a
rename in a future redis-py must cost a missing series, never a broken export.
"""

from __future__ import annotations

from typing import Iterable

from opentelemetry.metrics import CallbackOptions, Observation


def _live_pools() -> Iterable[tuple[str, object]]:
    """(label, pool) for each pool that already exists — never builds one.

    Imports are local: the pool modules pull in server services, and this
    module is imported by ``metrics``, which is imported almost everywhere.
    """
    from src.server.services.workspace_status_pubsub import peek_status_pubsub_pool
    from src.utils.cache.redis_cache import peek_cache_pool
    from src.utils.cache.stream_pool import peek_stream_reader_pool

    for label, peek in (
        ("cache", peek_cache_pool),
        ("reader", peek_stream_reader_pool),
        ("pubsub", peek_status_pubsub_pool),
    ):
        try:
            pool = peek()
        except Exception:
            continue
        if pool is not None:
            yield label, pool


def pool_in_use_observe(options: CallbackOptions) -> Iterable[Observation]:
    for label, pool in _live_pools():
        in_use = getattr(pool, "_in_use_connections", None)
        if in_use is None:
            continue
        yield Observation(len(in_use), {"pool": label})


def pool_max_observe(options: CallbackOptions) -> Iterable[Observation]:
    """Capacity, so a dashboard can read saturation without knowing our config."""
    for label, pool in _live_pools():
        cap = getattr(pool, "max_connections", None)
        if isinstance(cap, int):
            yield Observation(cap, {"pool": label})
