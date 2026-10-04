"""Error-sanitization guard for the sandbox-side ginlix-data client.

``GinlixMCPClient.fetch_short_data`` runs inside a Daytona sandbox and returns
its errors straight into agent-visible tool output. A stringified httpx error
embeds the request URL (internal host + query params), so the client routes
every non-HTTP failure through ``_error_dict``, which surfaces only the action
label plus the exception type — never the URL.
"""

import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest

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


class TestTokenRefresh:
    @pytest.mark.asyncio
    async def test_concurrent_expiries_refresh_once_and_every_caller_retries(self):
        """The refresh token rotates on use, so a second refresh racing the
        first spends a dead token and its caller fails on the 401."""
        def handler(request: httpx.Request) -> httpx.Response:
            if request.headers["Authorization"] == "Bearer fresh":
                return httpx.Response(200, json={"quotes": {}})
            return httpx.Response(401, json={"detail": "expired"})

        client = GinlixMCPClient()
        client._http = httpx.AsyncClient(
            base_url="http://ginlix-data.test", transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer stale"},
        )
        refreshes = 0

        async def refresh() -> str | None:
            nonlocal refreshes
            refreshes += 1
            first = refreshes == 1
            await asyncio.sleep(0.01)
            return "fresh" if first else None  # the rotated token is spent

        client._refresh_access_token = refresh
        try:
            responses = await asyncio.gather(
                *(client.request("GET", "/api/v2/data/quotes") for _ in range(3))
            )
        finally:
            await client.close()

        assert [r.status_code for r in responses] == [200, 200, 200]
        assert refreshes == 1
