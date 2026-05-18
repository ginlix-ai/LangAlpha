"""Regression coverage for the secretary's active-thread Redis reader.

After PR A the workflow path stopped writing the legacy
``workflow:events:{tid}`` List entirely; the only durable store for
in-flight events is ``workflow:stream:{tid}``. ``_extract_from_redis``
must now read from the Stream or it will return empty text for every
active thread the secretary touches.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.tools.secretary.utils import _extract_from_redis


def _sse(seq: int, text: str) -> bytes:
    data = json.dumps({"content_type": "text", "content": text})
    body = (
        f"id: {seq}\n"
        "event: message_chunk\n"
        f"data: {data}\n\n"
    )
    return body.encode("utf-8")


@pytest.mark.asyncio
async def test_reads_text_chunks_from_stream_in_chronological_order():
    """XREVRANGE returns newest-first; reader must reverse to chronological."""
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    # Most-recent first (XREVRANGE order). Each entry is (entry_id, fields).
    cache.client.xrevrange = AsyncMock(
        return_value=[
            (b"3-0", {b"event": _sse(3, "!")}),
            (b"2-0", {b"event": _sse(2, "world")}),
            (b"1-0", {b"event": _sse(1, "hello ")}),
        ]
    )

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        text = await _extract_from_redis("thread-1")

    assert text == "hello world!"
    cache.client.xrevrange.assert_awaited_once_with(
        "workflow:stream:thread-1", count=500
    )


@pytest.mark.asyncio
async def test_skips_entries_without_event_field():
    """Sentinel-style entries (no ``b"event"``) must not crash the reader."""
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.xrevrange = AsyncMock(
        return_value=[
            (b"2-0", {b"record": b'{"seq": 2}'}),  # no b"event" — skip
            (b"1-0", {b"event": _sse(1, "kept")}),
        ]
    )

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        text = await _extract_from_redis("thread-1")

    assert text == "kept"


@pytest.mark.asyncio
async def test_filters_non_text_events():
    """Only ``message_chunk`` with ``content_type=text`` contributes."""
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.xrevrange = AsyncMock(
        return_value=[
            (b"2-0", {b"event": b"id: 2\nevent: tool_call\ndata: {}\n\n"}),
            (b"1-0", {b"event": _sse(1, "only this")}),
        ]
    )

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        text = await _extract_from_redis("thread-1")

    assert text == "only this"


@pytest.mark.asyncio
async def test_cache_disabled_returns_empty():
    cache = MagicMock()
    cache.enabled = False
    cache.client = None

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        text = await _extract_from_redis("thread-1")

    assert text == ""


@pytest.mark.asyncio
async def test_xrevrange_failure_returns_empty():
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.xrevrange = AsyncMock(side_effect=RuntimeError("redis down"))

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        text = await _extract_from_redis("thread-1")

    assert text == ""
