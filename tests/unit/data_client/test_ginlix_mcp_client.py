"""Error-sanitization guard for the sandbox-side ginlix-data client.

``GinlixMCPClient.fetch_short_data`` runs inside a Daytona sandbox and returns
its errors straight into agent-visible tool output. A stringified httpx error
embeds the request URL (internal host + query params), so the client routes
every non-HTTP failure through ``_error_dict``, which surfaces only the action
label plus the exception type — never the URL.
"""

import json
import os
from unittest.mock import AsyncMock

import httpx
import pytest

from src.data_client.ginlix_data import mcp_client
from src.data_client.ginlix_data.mcp_client import GinlixMCPClient

# Made-up internal endpoint — a stand-in whose host/path must never surface.
_LEAKY_URL = "https://data.internal.example:8005/api/v1/data/stocks/short-interest?ticker=AAPL"


def _no_url_leaked(msg: str) -> None:
    assert "https://" not in msg
    assert "data.internal.example" not in msg
    assert "/api/v1/data/" not in msg


class TestFetchShortDataErrorSanitization:
    @pytest.mark.asyncio
    async def test_both_connect_errors_omit_url(self):
        client = GinlixMCPClient()
        client.ensure = AsyncMock(return_value=True)
        client.request = AsyncMock(
            side_effect=httpx.ConnectError(f"connection refused to {_LEAKY_URL}")
        )

        result = await client.fetch_short_data("AAPL", data_type="both")

        assert result["symbol"] == "AAPL"
        assert result["source"] == "ginlix-data"
        # Only the action + exception type, no URL.
        assert result["short_interest_error"] == "Short interest fetch failed (ConnectError)"
        assert result["short_volume_error"] == "Short volume fetch failed (ConnectError)"
        _no_url_leaked(result["short_interest_error"])
        _no_url_leaked(result["short_volume_error"])
        # Failed fetches never populate the success keys.
        assert "short_interest" not in result
        assert "short_volume" not in result

    @pytest.mark.asyncio
    async def test_single_short_volume_connect_error_omits_url(self):
        client = GinlixMCPClient()
        client.ensure = AsyncMock(return_value=True)
        client.request = AsyncMock(
            side_effect=httpx.ConnectError(f"connection refused to {_LEAKY_URL}")
        )

        result = await client.fetch_short_data("AAPL", data_type="short_volume")

        assert result["short_volume_error"] == "Short volume fetch failed (ConnectError)"
        _no_url_leaked(result["short_volume_error"])
        # short_interest branch was never entered for this data_type.
        assert "short_interest_error" not in result


class TestV2OverTheSandboxTransport:
    @pytest.mark.asyncio
    async def test_non_json_page_is_an_http_error_not_a_decode_error(self, caplog):
        """A 200 HTML page (a route that never reached ginlix-data) reads as an
        outage: one ERROR line, and the CN source raises the httpx error every
        CN caller already treats as a miss."""
        from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

        client = GinlixMCPClient()
        client.ensure = AsyncMock(return_value=True)
        client.request = AsyncMock(return_value=httpx.Response(
            200, text="<!doctype html>", headers={"content-type": "text/html"},
            request=httpx.Request("GET", "http://ginlix-data.test/api/v2/data/fundamentals/x"),
        ))

        with caplog.at_level("INFO"), pytest.raises(httpx.HTTPError):
            await GinlixDataCnFinancialSource(client).get_company_profile("600519.SH")

        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(errors) == 1 and "text/html" in errors[0].getMessage()
        assert not any(r.exc_info for r in caplog.records)

    @pytest.mark.asyncio
    async def test_contract_ticker_is_one_path_segment(self):
        client = GinlixMCPClient()
        client.ensure = AsyncMock(return_value=True)
        client.request = AsyncMock(return_value=httpx.Response(
            200, json={"results": []},
            request=httpx.Request("GET", "http://ginlix-data.test/"),
        ))

        await client.fetch_options_prices("O:SPY?x/../y", interval="1day")

        url = client.request.await_args.args[1]
        assert url == "/api/v1/data/aggregates/option/O%3ASPY%3Fx%2F..%2Fy"


def _write_token(path, token: str) -> None:
    """Replace the file whole, as the server does."""
    staged = path.with_suffix(".tmp")
    staged.write_text(
        json.dumps({"access_token": token, "ginlix_data_url": "http://ginlix-data.test"})
    )
    os.replace(staged, path)


class TestTokenFile:
    """The server writes every token; the client only reads, so a new one
    reaches it by the file changing."""

    @pytest.fixture
    def token_file(self, tmp_path, monkeypatch):
        path = tmp_path / ".mcp_tokens.json"
        monkeypatch.setattr(mcp_client, "TOKEN_FILE", path)
        return path

    @staticmethod
    def _client(handler) -> GinlixMCPClient:
        client = GinlixMCPClient()
        client._http = httpx.AsyncClient(
            base_url="http://ginlix-data.test", transport=httpx.MockTransport(handler)
        )
        return client

    @pytest.mark.asyncio
    async def test_a_replaced_file_is_read_before_the_next_request(self, token_file):
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers["Authorization"])
            return httpx.Response(200, json={})

        client = self._client(handler)
        try:
            _write_token(token_file, "one")
            await client.request("GET", "/api/v2/data/quotes")
            await client.request("GET", "/api/v2/data/quotes")
            _write_token(token_file, "two")
            await client.request("GET", "/api/v2/data/quotes")
        finally:
            await client.close()

        assert seen == ["Bearer one", "Bearer one", "Bearer two"]

    @pytest.mark.asyncio
    async def test_a_401_retries_once_with_a_token_written_meanwhile(self, token_file):
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers["Authorization"])
            if request.headers["Authorization"] == "Bearer fresh":
                return httpx.Response(200, json={})
            # The server's write lands while this request is in flight.
            _write_token(token_file, "fresh")
            return httpx.Response(401, json={"detail": "expired"})

        client = self._client(handler)
        try:
            _write_token(token_file, "stale")
            resp = await client.request("GET", "/api/v2/data/quotes")
        finally:
            await client.close()

        assert resp.status_code == 200
        assert seen == ["Bearer stale", "Bearer fresh"]

    @pytest.mark.asyncio
    async def test_a_401_with_no_newer_token_is_not_retried(self, token_file):
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers["Authorization"])
            return httpx.Response(401, json={"detail": "revoked"})

        client = self._client(handler)
        try:
            _write_token(token_file, "revoked")
            resp = await client.request("GET", "/api/v2/data/quotes")
        finally:
            await client.close()

        assert resp.status_code == 401
        assert seen == ["Bearer revoked"]


class TestFetchStockDataStatus:
    @pytest.mark.parametrize(
        ("status", "reported"),
        [(401, True), (403, True), (400, True), (404, True), (429, False), (503, False)],
    )
    @pytest.mark.asyncio
    async def test_a_4xx_reaches_the_caller_with_its_status(self, status, reported):
        """price_data reads the status to decide whether FMP may still answer,
        so a 401 no newer token answered has to arrive as one."""
        client = GinlixMCPClient()
        client._http = httpx.AsyncClient(
            base_url="http://ginlix-data.test",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(status, json={"detail": "nope"})
            ),
        )
        try:
            result = await client.fetch_stock_data("AAPL", "1day", "2026-10-01", "2026-10-08")
        finally:
            await client.close()

        if reported:
            assert result == {"error": f"ginlix-data error ({status}): nope", "status": status}
        else:
            assert result is None


class TestFetchStockDataWindow:
    @pytest.mark.parametrize(
        ("start", "end", "kept"),
        [
            # A window across days keeps the evening of the first and the
            # morning of the last, not the times of day both share.
            ("2026-10-15 15:00", "2026-10-16 10:00", ["15:00", "09:30", "10:00"]),
            # A bare end spans its whole day.
            ("2026-10-16", "2026-10-16", ["09:30", "10:00", "15:30"]),
            # Midnight is a time, not a bare day.
            ("2026-10-16 00:00", "2026-10-16 09:30", ["09:30"]),
        ],
    )
    @pytest.mark.asyncio
    async def test_intraday_bounds_are_instants(self, start, end, kept):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        new_york = ZoneInfo("America/New_York")
        opens = ["2026-10-15 13:00", "2026-10-15 15:00", "2026-10-16 09:30",
                 "2026-10-16 10:00", "2026-10-16 15:30"]
        bars = [
            {"time": int(datetime.fromisoformat(o).replace(tzinfo=new_york).timestamp() * 1000),
             "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
            for o in opens
        ]
        client = GinlixMCPClient()
        client._http = httpx.AsyncClient(
            base_url="http://ginlix-data.test",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"results": bars})
            ),
        )
        try:
            rows = await client.fetch_stock_data("AAPL", "5min", start, end)
        finally:
            await client.close()

        assert [r["date"][11:16] for r in reversed(rows)] == kept

    @pytest.mark.asyncio
    async def test_offset_bounds_ask_for_the_exchange_day(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        # 02:00 to 04:00 in UTC+8 is 14:00 to 16:00 the day before in New York.
        new_york = ZoneInfo("America/New_York")
        opens = ["2026-10-15 13:00", "2026-10-15 14:00", "2026-10-15 16:00",
                 "2026-10-15 16:30"]
        bars = [
            {"time": int(datetime.fromisoformat(o).replace(tzinfo=new_york).timestamp() * 1000),
             "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
            for o in opens
        ]
        asked = []

        def answer(request):
            asked.append(dict(request.url.params))
            return httpx.Response(200, json={"results": bars})

        client = GinlixMCPClient()
        client._http = httpx.AsyncClient(
            base_url="http://ginlix-data.test", transport=httpx.MockTransport(answer)
        )
        try:
            rows = await client.fetch_stock_data(
                "AAPL", "5min", "2026-10-16 02:00+08:00", "2026-10-16T04:00+08:00"
            )
        finally:
            await client.close()

        assert (asked[0]["from"], asked[0]["to"]) == ("2026-10-15", "2026-10-15")
        assert [r["date"] for r in reversed(rows)] == [
            "2026-10-15 14:00:00", "2026-10-15 16:00:00"
        ]

    @pytest.mark.parametrize(
        ("interval", "start", "end"),
        [
            ("1day", "2026-10-16", "2026-10-15"),
            ("1day", "2026/10/15", "2026-10-16"),
            ("5min", "2026-10-16 15:00", "2026-10-16 10:00"),
            ("5min", "2026-10-16 9:30", "2026-10-16 10:00"),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_window_it_cannot_read_falls_back(self, interval, start, end):
        # The FMP read that follows refuses it in the hosted words.
        asked = []
        client = GinlixMCPClient()
        client._http = httpx.AsyncClient(
            base_url="http://ginlix-data.test",
            transport=httpx.MockTransport(
                lambda request: asked.append(request) or httpx.Response(200, json={"results": []})
            ),
        )
        try:
            assert await client.fetch_stock_data("AAPL", interval, start, end) is None
        finally:
            await client.close()
        assert asked == []
