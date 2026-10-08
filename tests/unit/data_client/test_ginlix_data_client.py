"""Pagination + sort behavior for the ginlix-data aggregates client.

The client defaults to ``sort=desc`` so upstream pagination walks newest →
oldest; truncation then drops the *oldest* bars instead of the recent tail
(the previous asc behavior silently served 3-week-old bars for any wide
intraday window). Results are sorted back to ascending before return so
downstream code — cache watermark comparisons, delta-merge, lightweight-
charts — continues to receive ascending bars.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from src.data_client.ginlix_data.client import SERVICE_USER_ID, GinlixDataClient
from src.data_client.ginlix_data.v2_routes import MAX_FUNDAMENTAL_ROWS, MAX_KEYS_PER_REQUEST


class _FakeTransport(httpx.AsyncBaseTransport):
    """Feeds scripted pages to the client and records query params per call."""

    def __init__(self, pages: list[dict[str, Any]]):
        self._pages = pages
        self._idx = 0
        self.captured_params: list[dict[str, str]] = []
        self.captured_headers: list[httpx.Headers] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.captured_params.append(dict(request.url.params))
        self.captured_headers.append(request.headers)
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


@pytest.mark.asyncio
async def test_get_snapshots_rejects_path_smuggling_asset_type():
    """asset_type is interpolated into the request path next to the service
    token — anything outside the known asset types must be refused outright."""
    client = GinlixDataClient(base_url="http://ginlix-data.test")
    with pytest.raises(ValueError):
        await client.get_snapshots("../../admin", ["AAPL"])


@pytest.mark.asyncio
async def test_get_snapshots_accepts_options_asset_type():
    """The options-chain pricing path calls get_snapshots("options", ...) —
    the whitelist must not reject it (regression: pre-landing review P1)."""
    pages = [{"results": [{"ticker": "O:AAPL260117C00200000", "price": 1.23}]}]
    client, _ = await _client_with(pages)
    snaps = await client.get_snapshots("options", ["O:AAPL260117C00200000"])
    assert snaps == [{"ticker": "O:AAPL260117C00200000", "price": 1.23}]


@pytest.mark.asyncio
async def test_userless_call_authenticates_as_the_service():
    # ginlix-data 401s a service token without a user id; directory, fundamentals
    # and feed reads have no user, so they must name the service instead.
    client, transport = await _client_with([{"instruments": {}}, {"quotes": {}}])
    await client.get_instruments_v2(["600519.SH"])
    await client.get_quotes_v2(["600519.SH"], user_id="u-1")
    assert transport.captured_headers[0]["X-User-Id"] == SERVICE_USER_ID
    assert transport.captured_headers[1]["X-User-Id"] == "u-1"


@pytest.mark.asyncio
async def test_repeated_cursor_stops_and_dedupes():
    # A cursor that does not advance re-reads the same page: stop, flag the
    # window truncated, and keep one bar per timestamp.
    page = {"results": [{"time": 20, "close": 2.0}, {"time": 19, "close": 1.9}], "next_cursor": "same"}
    client, transport = await _client_with([page, page, page])
    bars, truncated = await client.get_aggregates(
        market="stock", symbol="NVDA", timespan="hour", multiplier=1,
        from_date="2026-03-01", to_date="2026-03-20",
    )
    assert truncated is True
    assert len(transport.captured_params) == 2
    assert [b["time"] for b in bars] == [19, 20]


class _PathTransport(httpx.AsyncBaseTransport):
    """Records each request's raw path and answers with one scripted response."""

    def __init__(self, response: httpx.Response):
        self._response = response
        self.raw_paths: list[bytes] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.raw_paths.append(request.url.raw_path)
        return httpx.Response(
            self._response.status_code,
            headers=self._response.headers,
            content=self._response.content,
        )


def _client_answering(response: httpx.Response) -> tuple[GinlixDataClient, _PathTransport]:
    transport = _PathTransport(response)
    client = GinlixDataClient(base_url="http://ginlix-data.test")
    client.http = httpx.AsyncClient(
        base_url="http://ginlix-data.test", transport=transport, timeout=1.0
    )
    return client, transport


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["AAPL?.SH", "600519.SH#x", "a/../../admin", ".."])
async def test_a_symbol_is_one_path_segment(key):
    # Raw, `?` and `#` cut the path short (AAPL?.SH would fetch AAPL) and `/`
    # or a dot segment walks out of the route.
    client, transport = _client_answering(httpx.Response(200, json={"rows": [], "bars": []}))

    await client.get_fundamentals_v2(key, "profile")
    await client.get_options_bars_v2(key, market="cn", from_date=None, to_date=None)
    await client.get_option_contract(key)
    await client.get_aggregates(market="stock", symbol=key)

    prefixes = [
        b"/api/v2/data/fundamentals/",
        b"/api/v2/data/options/bars/",
        b"/api/v1/data/options/contracts/",
        b"/api/v1/data/aggregates/stock/",
    ]
    for prefix, raw in zip(prefixes, transport.raw_paths, strict=True):
        query = raw.find(b"?")
        path = raw if query < 0 else raw[:query]
        assert path.startswith(prefix), raw
        segment = path[len(prefix):]
        assert segment and b"/" not in segment and b"#" not in segment, raw
        assert httpx.URL("http://x.test/" + segment.decode()).path == "/" + key


@pytest.mark.asyncio
async def test_a_non_json_2xx_is_one_typed_error(caplog):
    from src.data_client.ginlix_data.v2_routes import NonJsonResponse

    page = httpx.Response(200, text="<!doctype html><html></html>",
                          headers={"content-type": "text/html; charset=utf-8"})
    client, _ = _client_answering(page)

    with caplog.at_level("ERROR"), pytest.raises(NonJsonResponse) as exc:
        await client.get_fundamentals_v2("600519.XSHG", "profile")

    # An httpx.HTTPError, so the soft-miss handlers that absorb an outage absorb it too.
    assert isinstance(exc.value, httpx.HTTPError)
    assert "non-JSON" in str(exc.value)
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    message = errors[0].getMessage()
    assert "/api/v2/data/fundamentals/600519.XSHG" in message and "text/html" in message


@pytest.mark.asyncio
async def test_news_route_missing_is_an_empty_window():
    client, _ = _client_answering(httpx.Response(404, json={"detail": "Not Found"}))

    assert await client.get_news_v2(market="cn", limit=1000) == []


class _KeyedTransport(httpx.AsyncBaseTransport):
    """Answers a keyed batch route as ginlix-data does, 422 past its key cap.

    Every request waits at a barrier sized to the requests the test expects,
    so chunks sent one after another never get past the first.
    """

    def __init__(self, field: str, known: set[str], *, requests: int, fail_on: str | None = None):
        self._field = field
        self._known = known
        self._fail_on = fail_on
        self._barrier = asyncio.Barrier(requests)
        self.batches: list[list[str]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        keys = request.url.params["keys"].split(",")
        self.batches.append(keys)
        if len(keys) > MAX_KEYS_PER_REQUEST:
            return httpx.Response(422, json={"detail": "too many keys"})
        await asyncio.wait_for(self._barrier.wait(), timeout=2.0)
        if self._fail_on in keys:
            return httpx.Response(503, json={"detail": "unavailable"})
        return httpx.Response(200, json={
            self._field: {k: {"instrument_key": k} for k in keys if k in self._known},
            "missing": [k for k in keys if k not in self._known],
            "invalid": [],
        })


def _client_over(transport: httpx.AsyncBaseTransport) -> GinlixDataClient:
    client = GinlixDataClient(base_url="http://ginlix-data.test")
    client.http = httpx.AsyncClient(
        base_url="http://ginlix-data.test", transport=transport, timeout=5.0
    )
    return client


_BATCH_ROUTES = [("get_quotes_v2", "quotes"), ("get_instruments_v2", "instruments")]


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "field"), _BATCH_ROUTES)
async def test_a_batch_over_the_key_cap_is_split_concurrently_and_merged(method, field):
    keys = [f"{600000 + i}.XSHG" for i in range(2 * MAX_KEYS_PER_REQUEST + 1)]
    known = set(keys[::2])
    transport = _KeyedTransport(field, known, requests=3)

    # Blanks and repeats count against the cap, so they go before it is applied.
    got = await getattr(_client_over(transport), method)([*keys, " ", "", keys[0]])

    assert sorted(len(b) for b in transport.batches) == [1, MAX_KEYS_PER_REQUEST, MAX_KEYS_PER_REQUEST]
    assert sorted(k for b in transport.batches for k in b) == keys
    assert got == {k: {"instrument_key": k} for k in known}


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "field"), _BATCH_ROUTES)
async def test_one_failed_chunk_fails_the_batch(method, field):
    keys = [f"{600000 + i}.XSHG" for i in range(MAX_KEYS_PER_REQUEST + 1)]
    transport = _KeyedTransport(field, set(keys), requests=2, fail_on=keys[-1])

    with pytest.raises(httpx.HTTPStatusError) as exc:
        await getattr(_client_over(transport), method)(keys)
    assert exc.value.response.status_code == 503
    assert sorted(len(b) for b in transport.batches) == [1, MAX_KEYS_PER_REQUEST]


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "field"), _BATCH_ROUTES)
async def test_a_key_holding_a_comma_is_dropped_and_its_chunk_still_answers(method, field):
    # Upstream it splits in two: a listing nobody asked for, and a full chunk
    # pushed past the cap, which would fail every key beside it.
    keys = [f"{600000 + i}.XSHG" for i in range(MAX_KEYS_PER_REQUEST - 1)]
    transport = _KeyedTransport(field, {*keys, "AAPL", "MSFT"}, requests=1)

    got = await getattr(_client_over(transport), method)([*keys[:5], "AAPL,MSFT", *keys[5:]])

    assert [len(b) for b in transport.batches] == [MAX_KEYS_PER_REQUEST - 1]
    assert got == {k: {"instrument_key": k} for k in keys}


@pytest.mark.asyncio
async def test_a_fundamentals_limit_past_the_route_cap_is_clamped():
    # ginlix-data answers limit > 500 with a 422, which hands the name to the
    # default source; no listing has that many rows.
    client, transport = await _client_with([{"rows": []}])

    await client.get_fundamentals_v2("600519.XSHG", "income", period="quarter", limit=1000)

    assert transport.captured_params[0]["limit"] == str(MAX_FUNDAMENTAL_ROWS)
