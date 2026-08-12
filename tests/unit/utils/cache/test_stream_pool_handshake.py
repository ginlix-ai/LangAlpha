"""The stream-reader connect budget must cover the handshake, not just the dial.

``socket_connect_timeout`` bounds ``asyncio.open_connection`` and stops there;
the AUTH / CLIENT SETINFO / SELECT round trips that follow are each bounded
separately by ``socket_timeout``, so they sum well past the callers' XREAD
deadline. These lock the explicit cap and — just as load-bearing — the
exception *type* it fails with: the three SSE readers route a bare
``asyncio.TimeoutError`` into an immediate keepalive retry and everything else
into a backoff, so a handshake stall surfacing as a timeout would hot-loop the
redial storm the pool split exists to prevent.
"""

import asyncio
import time
import types

import pytest
import redis.exceptions as redis_exceptions

# Imported by value on purpose: tests/unit/conftest.py monkeypatches the
# accessor on the module to alias the reader onto each test's fake cache
# client. Binding at import time keeps these tests on the real builder.
from src.utils.cache.redis_cache import is_pool_exhaustion
from src.utils.cache.stream_pool import (
    _bounded_handshake,
    _CONNECT_TIMEOUT_S,
    close_stream_reader_pool,
    get_stream_reader_client,
)

# The tightest of the three reader deadlines: ``wait_for(block_ms/1000 + 2.0)``
# with the default 4s block. Nothing in connect may approach it.
_CALLER_WAIT_FOR_S = 6.0


class _StalledConnection:
    """Stands in for a server that accepts the TCP dial and then goes quiet."""

    def __init__(self):
        self.calls = []

    async def on_connect_check_health(self, check_health: bool = True) -> None:
        self.calls.append(check_health)
        await asyncio.sleep(3600)


@pytest.mark.asyncio
async def test_stalled_handshake_fails_at_the_connect_budget():
    started = time.monotonic()
    with pytest.raises(redis_exceptions.ConnectionError):
        await _bounded_handshake(_StalledConnection())
    elapsed = time.monotonic() - started

    assert elapsed == pytest.approx(_CONNECT_TIMEOUT_S, abs=0.5)
    assert elapsed < _CALLER_WAIT_FOR_S / 2


@pytest.mark.asyncio
async def test_handshake_timeout_is_not_a_bare_timeout_error():
    with pytest.raises(redis_exceptions.ConnectionError) as excinfo:
        await _bounded_handshake(_StalledConnection())

    exc = excinfo.value
    # RedisError is what triggers connect_check_health's disconnect() cleanup.
    assert isinstance(exc, redis_exceptions.RedisError)
    # asyncio.TimeoutError is the readers' immediate-retry branch — staying off
    # it is the whole point.
    assert not isinstance(exc, asyncio.TimeoutError)
    assert isinstance(exc.__cause__, asyncio.TimeoutError)
    # A slow handshake is not a starved pool: it must take the ordinary error
    # backoff, not the longer exhaustion one.
    assert not is_pool_exhaustion(exc)


@pytest.mark.asyncio
async def test_successful_handshake_passes_through():
    conn = _StalledConnection()

    async def _ok(check_health: bool = True) -> None:
        conn.calls.append(check_health)

    conn.on_connect_check_health = _ok
    assert await _bounded_handshake(conn) is None
    assert conn.calls == [True]


@pytest.mark.asyncio
async def test_handshake_failure_leaves_no_half_open_socket(monkeypatch):
    """The full redis-py path: connect() must clean up and surface ConnectionError."""
    from redis.asyncio.connection import Connection

    monkeypatch.setattr("src.utils.cache.stream_pool._CONNECT_TIMEOUT_S", 0.05)

    closed = asyncio.Event()

    class _Writer:
        def close(self):
            closed.set()

        async def wait_closed(self):
            return None

    conn = Connection(
        host="192.0.2.1",
        port=6379,
        socket_timeout=5.0,
        socket_connect_timeout=1.0,
        redis_connect_func=_bounded_handshake,
    )

    async def _fake_dial():
        conn._reader = types.SimpleNamespace()
        conn._writer = _Writer()

    monkeypatch.setattr(conn, "_connect", _fake_dial)

    async def _stall(check_health: bool = True) -> None:
        await asyncio.sleep(3600)

    monkeypatch.setattr(conn, "on_connect_check_health", _stall)

    with pytest.raises(redis_exceptions.ConnectionError):
        await conn.connect()

    assert closed.is_set(), "half-open socket was left behind"
    assert not conn.is_connected


@pytest.mark.asyncio
async def test_reader_pool_installs_the_bounded_handshake():
    """Without the kwarg the cap is silently inert — pin the wiring."""
    cache = types.SimpleNamespace(enabled=True, url="redis://127.0.0.1:6379/0")
    try:
        client = await get_stream_reader_client(cache)
        assert client is not None
        kwargs = client.connection_pool.connection_kwargs
        assert kwargs["redis_connect_func"] is _bounded_handshake
        assert kwargs["socket_connect_timeout"] == _CONNECT_TIMEOUT_S
    finally:
        await close_stream_reader_pool()
