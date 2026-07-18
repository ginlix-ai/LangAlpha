"""Locks the mux active-stream gap contract (§Retention).

A resume cursor whose successor was trimmed away on an ACTIVE stream gets
``resync_required`` (the caller closes the channel) — never the old
gap-and-continue advisory. With the write-side quota this state is
unreachable for post-quota streams; the frame exists for legacy or
externally-trimmed state.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.server.handlers.chat.thread_stream_mux import _attach_gap_probe, _Chan


def _chan(cursor: bytes) -> _Chan:
    return _Chan(
        task_id="abc123",
        epoch="run-1",
        stream_key=b"subagent:stream:t-1:abc123",
        cursor=cursor,
    )


def _cache_with_head(payload: bytes | None):
    cache = MagicMock()
    entries = [] if payload is None else [(b"50-0", {b"event": payload})]
    cache.client.xrange = AsyncMock(return_value=entries)
    return cache


@pytest.mark.asyncio
async def test_trimmed_head_returns_resync_required():
    cache = _cache_with_head(b"id: 50\nevent: message_chunk\ndata: {}\n\n")
    frame = await _attach_gap_probe(cache, _chan(b"10-0"))
    assert frame is not None
    assert frame.startswith("event: resync_required\n")
    assert '"expected_from": 11' in frame
    assert '"first_available": 50' in frame
    assert '"chan": "task:abc123"' in frame


@pytest.mark.asyncio
async def test_contiguous_head_returns_none():
    cache = _cache_with_head(b"id: 11\nevent: message_chunk\ndata: {}\n\n")
    assert await _attach_gap_probe(cache, _chan(b"10-0")) is None


@pytest.mark.asyncio
async def test_auto_id_cursor_carries_no_logical_position():
    cache = _cache_with_head(b"id: 50\nevent: message_chunk\ndata: {}\n\n")
    # ms-timestamp major (auto-ID) — no seq comparison possible.
    assert await _attach_gap_probe(cache, _chan(b"1752800000000-0")) is None


@pytest.mark.asyncio
async def test_empty_stream_returns_none():
    cache = _cache_with_head(None)
    assert await _attach_gap_probe(cache, _chan(b"10-0")) is None
