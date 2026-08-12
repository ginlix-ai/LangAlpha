"""`is_pool_exhaustion` must recognize every spelling of "no pool slot".

The detector previously matched only `MaxConnectionsError` / "Too many
connections". `BlockingConnectionPool` reports an expired acquire timeout as a
plain `ConnectionError("No connection available.")`, so moving the hot pools to
the blocking class would have silently blinded the exhaustion path — the exact
signal the 0060 incident was diagnosed from.
"""

import redis.exceptions as redis_exceptions

from src.utils.cache.redis_cache import is_pool_exhaustion


def test_max_connections_error_is_exhaustion():
    exc = redis_exceptions.MaxConnectionsError("Too many connections")
    assert is_pool_exhaustion(exc)


def test_blocking_pool_timeout_is_exhaustion():
    # Raised verbatim by BlockingConnectionPool.get_connection on timeout.
    assert is_pool_exhaustion(
        redis_exceptions.ConnectionError("No connection available.")
    )


def test_bare_parent_with_legacy_message_is_exhaustion():
    assert is_pool_exhaustion(
        redis_exceptions.ConnectionError("Too many connections")
    )


def test_unrelated_redis_errors_are_not_exhaustion():
    assert not is_pool_exhaustion(
        redis_exceptions.TimeoutError("Timeout reading from 10.0.0.1:6379")
    )
    assert not is_pool_exhaustion(
        redis_exceptions.ConnectionError("Connection closed by server")
    )
    assert not is_pool_exhaustion(RuntimeError("boom"))


def test_loading_is_not_exhaustion_but_is_a_connection_error():
    """The boundary ``stream_append`` relies on to classify LOADING.

    ``BusyLoadingError`` is a ``ConnectionError`` subclass, not a
    ``ResponseError``, so it reaches the append policy's ambiguous branch by
    default and has to be named there explicitly. It is not pool exhaustion —
    the message shares no wording with any exhaustion spelling.
    """
    exc = redis_exceptions.BusyLoadingError("Redis is loading the dataset in memory")
    assert isinstance(exc, redis_exceptions.ConnectionError)
    assert not isinstance(exc, redis_exceptions.ResponseError)
    assert not is_pool_exhaustion(exc)
