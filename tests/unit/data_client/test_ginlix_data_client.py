"""Pagination + sort behavior for the ginlix-data aggregates client.

The client defaults to ``sort=desc`` so upstream pagination walks newest →
oldest; truncation then drops the *oldest* bars instead of the recent tail
(the previous asc behavior silently served 3-week-old bars for any wide
intraday window). Results are sorted back to ascending before return so
downstream code — cache watermark comparisons, delta-merge, lightweight-
charts — continues to receive ascending bars.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from src.data_client.ginlix_data.client import GinlixDataClient


class _FakeTransport(httpx.AsyncBaseTransport):
    """Feeds scripted pages to the client and records query params per call."""

    def __init__(self, pages: list[dict[str, Any]]):
        self._pages = pages
        self._idx = 0
        self.captured_params: list[dict[str, str]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.captured_params.append(dict(request.url.params))
        page = self._pages[min(self._idx, len(self._pages) - 1)]
        self._idx += 1
        return httpx.Response(200, json=page)


async def _client_with(pages: list[dict[str, Any]]) -> tuple[GinlixDataClient, _FakeTransport]:
    transport = _FakeTransport(pages)
    client = GinlixDataClient(base_url="http://ginlix-data.test")
    # Swap the HTTP layer for our scripted transport.
    client.http = httpx.AsyncClient(
        base_url="http://ginlix-data.test",
        transport=transport,
        timeout=1.0,
    )
    return client, transport


@pytest.mark.asyncio
async def test_default_sort_is_desc():
    # Single page, no cursor — verifies the sort= param that gets sent upstream.
    pages = [{"results": [{"time": 3, "close": 30.0}], "next_cursor": None}]
    client, transport = await _client_with(pages)
    bars, truncated = await client.get_aggregates(
        market="stock", symbol="NVDA", timespan="minute", multiplier=5,
        from_date="2026-03-23", to_date="2026-04-22",
    )
    assert truncated is False
    # Sent as query param to ginlix-data
    assert transport.captured_params[0]["sort"] == "desc"


@pytest.mark.asyncio
async def test_desc_pages_reversed_to_ascending():
    # Three pages of desc-ordered bars. Client should return them ascending.
    pages = [
        {"results": [{"time": 30, "close": 3.0}, {"time": 29, "close": 2.9}], "next_cursor": "c1"},
        {"results": [{"time": 20, "close": 2.0}, {"time": 19, "close": 1.9}], "next_cursor": "c2"},
        {"results": [{"time": 10, "close": 1.0}], "next_cursor": None},
    ]
    client, _ = await _client_with(pages)
    bars, truncated = await client.get_aggregates(
        market="stock", symbol="NVDA", timespan="minute", multiplier=1,
    )
    assert truncated is False
    assert [b["time"] for b in bars] == [10, 19, 20, 29, 30]


@pytest.mark.asyncio
async def test_page_ceiling_drops_oldest_not_recent_under_desc():
    # Ten pages of 2 bars each, desc order. Eleventh page would have older
    # bars but we stop at _MAX_PAGES=10. The *recent* bars (20..1) must be
    # preserved; the older bars that would have been on page 11+ are the
    # ones dropped. (Proves the screenshot symptom can't recur.)
    pages = []
    for page_idx in range(10):
        high = 20 - page_idx * 2
        pages.append({
            "results": [{"time": high, "close": 1.0}, {"time": high - 1, "close": 1.0}],
            "next_cursor": f"c{page_idx}",
        })
    client, _ = await _client_with(pages)
    bars, truncated = await client.get_aggregates(
        market="stock", symbol="NVDA", timespan="minute", multiplier=1,
    )
    assert truncated is True
    times = [b["time"] for b in bars]
    # 20 bars, sorted ascending, most-recent bar (time=20) is present.
    assert len(times) == 20
    assert times[-1] == 20  # recent tail preserved
    assert times == sorted(times)


@pytest.mark.asyncio
async def test_explicit_sort_asc_is_passed_through_and_not_reversed():
    # Backward-compat: a caller that asks for asc gets asc, no reverse.
    pages = [{"results": [{"time": 1, "close": 1.0}, {"time": 2, "close": 2.0}], "next_cursor": None}]
    client, transport = await _client_with(pages)
    bars, _truncated = await client.get_aggregates(
        market="stock", symbol="NVDA", timespan="minute", multiplier=1, sort="asc",
    )
    assert transport.captured_params[0]["sort"] == "asc"
    assert [b["time"] for b in bars] == [1, 2]
