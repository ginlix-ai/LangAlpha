"""Redis pool-occupancy gauge: labelling, absence, and the private-attr guard."""

from __future__ import annotations

from unittest.mock import patch

from src.observability.redis_pool_callbacks import (
    pool_in_use_observe,
    pool_max_observe,
)


class FakePool:
    def __init__(self, in_use: int, max_connections: int = 150):
        self._in_use_connections = {object() for _ in range(in_use)}
        self.max_connections = max_connections


def _peeks(cache=None, reader=None, pubsub=None):
    return (
        patch(
            "src.utils.cache.redis_cache.peek_cache_pool",
            return_value=cache,
        ),
        patch(
            "src.utils.cache.stream_pool.peek_stream_reader_pool",
            return_value=reader,
        ),
        patch(
            "src.server.services.workspace_status_pubsub.peek_status_pubsub_pool",
            return_value=pubsub,
        ),
    )


def _observe(callback, **pools) -> dict:
    p1, p2, p3 = _peeks(**pools)
    with p1, p2, p3:
        return {o.attributes["pool"]: o.value for o in callback(None)}


def test_every_pool_reports_under_its_own_label():
    values = _observe(
        pool_in_use_observe,
        cache=FakePool(4),
        reader=FakePool(9),
        pubsub=FakePool(2),
    )
    assert values == {"cache": 4, "reader": 9, "pubsub": 2}


def test_capacity_is_reported_per_pool():
    values = _observe(
        pool_max_observe,
        cache=FakePool(0, 150),
        reader=FakePool(0, 100),
        pubsub=FakePool(0, 150),
    )
    assert values == {"cache": 150, "reader": 100, "pubsub": 150}


def test_a_pool_that_does_not_exist_yet_emits_no_series():
    """The gauge must never be the thing that builds a pool."""
    assert _observe(pool_in_use_observe) == {}
    assert _observe(pool_in_use_observe, reader=FakePool(3)) == {"reader": 3}


def test_a_renamed_redis_py_attribute_drops_the_series_not_the_export():
    pool = FakePool(5)
    del pool._in_use_connections
    values = _observe(pool_in_use_observe, cache=pool, reader=FakePool(1))
    assert values == {"reader": 1}


def test_a_failing_peek_does_not_break_the_other_pools():
    with patch(
        "src.utils.cache.redis_cache.peek_cache_pool",
        side_effect=RuntimeError("boom"),
    ):
        p1, p2, p3 = _peeks(reader=FakePool(7))
        with p2, p3:
            values = {o.attributes["pool"]: o.value for o in pool_in_use_observe(None)}
    assert values == {"reader": 7}
