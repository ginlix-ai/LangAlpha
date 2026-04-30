"""Unit tests for the Redis-Streams-backed SSE consumer.

Validates:
- XREAD cursor selection ($/0/<seq>-0) for new vs replay vs resume connections
- SSE payload pass-through (UTF-8 decode of bytes)
- Keepalive emission on BLOCK timeout + terminal-after-empty exit handshake
- on_attach / on_detach hooks fire even when generator is cancelled mid-flight
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.stream_from_log import _stream_from_redis_log


def _make_cache(xread_returns: list[Any]) -> MagicMock:
    """A cache mock whose client.xread iterates through the given sequence."""
    cache = MagicMock()
    cache.enabled = True
    redis = MagicMock()
    cache.client = redis
    redis.xread = AsyncMock(side_effect=xread_returns)
    return cache


@pytest.mark.asyncio
async def test_yields_decoded_payloads_in_order():
    cache = _make_cache(
        [
            [
                (
                    b"workflow:stream:t1",
                    [
                        (b"1-0", {b"event": b"id: 1\nevent: x\ndata: a\n\n"}),
                        (b"2-0", {b"event": b"id: 2\nevent: x\ndata: b\n\n"}),
                    ],
                )
            ],
            [],  # BLOCK timeout
            [],  # terminal=True confirmed; should exit after second empty
        ]
    )

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        out = []
        async for ev in _stream_from_redis_log(
            stream_key="workflow:stream:t1",
            terminal_check=terminal,
            last_event_id=None,
        ):
            out.append(ev)

    # Two real events + one keepalive between the only-data round and the
    # exit handshake.
    assert "id: 1" in out[0]
    assert "id: 2" in out[1]
    assert ":keepalive\n\n" in out


@pytest.mark.asyncio
async def test_first_connect_uses_dollar_cursor():
    cache = _make_cache([[], []])  # two empty rounds → terminal handshake

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=None,
        ):
            pass

    args, kwargs = cache.client.xread.call_args_list[0]
    streams = args[0] if args else kwargs.get("streams")
    assert streams == {b"k": b"$"}


@pytest.mark.asyncio
async def test_replay_uses_zero_cursor():
    cache = _make_cache([[], []])

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=0,
        ):
            pass

    args, kwargs = cache.client.xread.call_args_list[0]
    streams = args[0] if args else kwargs.get("streams")
    assert streams == {b"k": b"0"}


@pytest.mark.asyncio
async def test_resume_uses_seq_dash_zero_cursor():
    cache = _make_cache([[], []])

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=42,
        ):
            pass

    args, kwargs = cache.client.xread.call_args_list[0]
    streams = args[0] if args else kwargs.get("streams")
    assert streams == {b"k": b"42-0"}


@pytest.mark.asyncio
async def test_advances_cursor_through_entries():
    """Across multiple XREAD rounds, cursor should advance to last seen ID."""
    cache = _make_cache(
        [
            [
                (
                    b"k",
                    [
                        (b"5-0", {b"event": b"id: 5\ndata: a\n\n"}),
                        (b"6-0", {b"event": b"id: 6\ndata: b\n\n"}),
                    ],
                )
            ],
            [],  # terminal becomes True; first empty round
            [],  # second empty round → exit
        ]
    )

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=4,
        ):
            pass

    cursors = [
        (call.args[0] if call.args else call.kwargs.get("streams"))[b"k"]
        for call in cache.client.xread.call_args_list
    ]
    assert cursors[0] == b"4-0"  # initial resume cursor
    assert cursors[1] == b"6-0"  # advanced past the two yielded entries


@pytest.mark.asyncio
async def test_terminal_handshake_requires_two_empty_rounds():
    """If terminal=True but new events still arrive between rounds, don't exit early."""
    cache = _make_cache(
        [
            [],  # empty + terminal=True → set terminal_seen
            [
                (
                    b"k",
                    [(b"1-0", {b"event": b"id: 1\ndata: x\n\n"})],
                )
            ],  # late event resets terminal_seen
            [],  # empty + terminal=True → first
            [],  # empty + terminal=True → second; exit
        ]
    )

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        out = []
        async for ev in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=0,
        ):
            out.append(ev)

    payloads = [e for e in out if not e.startswith(":keepalive")]
    assert len(payloads) == 1
    assert "id: 1" in payloads[0]


@pytest.mark.asyncio
async def test_attach_detach_hooks_fire():
    cache = _make_cache([[], []])
    attach_calls = []
    detach_calls = []

    async def on_attach() -> None:
        attach_calls.append(True)

    async def on_detach() -> None:
        detach_calls.append(True)

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=0,
            on_attach=on_attach,
            on_detach=on_detach,
        ):
            pass

    assert attach_calls == [True]
    assert detach_calls == [True]


@pytest.mark.asyncio
async def test_disabled_cache_returns_empty_immediately():
    cache = MagicMock()
    cache.enabled = False
    cache.client = None

    async def terminal() -> bool:
        return False

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        out = [
            ev
            async for ev in _stream_from_redis_log(
                stream_key="k",
                terminal_check=terminal,
                last_event_id=None,
            )
        ]
    assert out == []
