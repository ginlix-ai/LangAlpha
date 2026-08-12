"""Integration tests for price_data_mcp_server — hits real APIs.

Run with:  uv run python -m pytest tests/integration/ -m integration -v
Requires:  FMP_API_KEY (for OHLCV), GINLIX_DATA_URL (for short data)
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_has_fmp = bool(os.getenv("FMP_API_KEY"))
_has_ginlix = bool(os.getenv("GINLIX_DATA_URL"))


# ---------------------------------------------------------------------------
# get_stock_data
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_fmp, reason="FMP_API_KEY not set")
class TestGetStockDataLive:
    async def test_daily(self):
        from mcp_servers.price_data_mcp_server import get_stock_data

        result = await get_stock_data("AAPL", interval="1day")
        assert "error" not in result, result.get("error")
        assert result["symbol"] == "AAPL"
        assert result["interval"] == "1day"
        assert result["currency"] == "USD"
        assert result["count"] > 0
        data = result["data"]
        assert result["count"] == len(data)
        row = data[0]
        assert all(k in row for k in ("date", "open", "high", "low", "close", "volume"))
        # Ascending order (oldest first)
        if len(data) > 1:
            assert data[0]["date"] <= data[1]["date"]

    async def test_intraday_5min(self):
        from mcp_servers.price_data_mcp_server import get_stock_data

        result = await get_stock_data(
            "AAPL", interval="5min",
            start_date="2025-03-03", end_date="2025-03-07",
        )
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    async def test_unsupported_interval(self):
        from mcp_servers.price_data_mcp_server import get_stock_data

        result = await get_stock_data("AAPL", interval="2min")
        assert "error" in result
        assert result["error"] == "unsupported_interval"


# ---------------------------------------------------------------------------
# get_asset_data
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_fmp, reason="FMP_API_KEY not set")
class TestGetAssetDataLive:
    async def test_commodity(self):
        from mcp_servers.price_data_mcp_server import get_asset_data

        result = await get_asset_data("GCUSD", asset_type="commodity")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    async def test_crypto(self):
        from mcp_servers.price_data_mcp_server import get_asset_data

        result = await get_asset_data("BTCUSD", asset_type="crypto")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    async def test_forex(self):
        from mcp_servers.price_data_mcp_server import get_asset_data

        result = await get_asset_data("EURUSD", asset_type="forex")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0


# ---------------------------------------------------------------------------
# get_short_data
# ---------------------------------------------------------------------------

async def _init_ginlix():
    """Ensure the ginlix-data MCP client is initialized for the current event loop.

    The module-level ``_ginlix`` is a :class:`GinlixMCPClient` whose HTTP
    client is lazily created via ``ensure()``.  In strict asyncio mode each
    test gets its own event loop, so we must recreate the httpx client every
    time to avoid "Event loop is closed" errors.
    """
    import httpx
    import mcp_servers.price_data_mcp_server as mod

    client = mod._ginlix

    # Always recreate the httpx client to bind to the current event loop
    if client._http is not None:
        try:
            await client._http.aclose()
        except Exception:
            pass
        client._http = None

    # Try the normal ensure() path first (reads token file)
    if not await client.ensure():
        # Fall back to env-var-based initialization
        url = os.getenv("GINLIX_DATA_URL", "")
        token = os.getenv("INTERNAL_SERVICE_TOKEN", "")
        if not url:
            pytest.skip("GINLIX_DATA_URL not set")
        headers: dict[str, str] = {}
        if token:
            headers["X-Service-Token"] = token
            headers["X-User-Id"] = "integration-test"
        client._http = httpx.AsyncClient(
            base_url=url.rstrip("/"), headers=headers, timeout=30.0,
        )
    return mod


@pytest.mark.skipif(not _has_ginlix, reason="GINLIX_DATA_URL not set")
class TestGetShortDataLive:
    async def test_both(self):
        mod = await _init_ginlix()
        result = await mod.get_short_data("AAPL")
        assert "error" not in result, result.get("error")
        assert result["source"] == "ginlix-data"
        assert result["data_type"] == "both"
        # Sections live under the standard `data` payload (a dict of lists).
        data = result["data"]
        assert "short_interest" in data
        assert "short_volume" in data
        assert len(data["short_interest"]) > 0
        assert len(data["short_volume"]) > 0
        # `count` is a plain int totalling every record across sections.
        assert result["count"] == len(data["short_interest"]) + len(data["short_volume"])

        # Short data is newest-first (per the tool docstring).
        si = data["short_interest"]
        if len(si) > 1:
            assert si[0]["settlement_date"] >= si[1]["settlement_date"]
        sv = data["short_volume"]
        if len(sv) > 1:
            assert sv[0]["date"] >= sv[1]["date"]

        # Verify vendor-native fields.
        si_row = si[0]
        assert "short_interest" in si_row
        assert "settlement_date" in si_row
        assert "days_to_cover" in si_row

        sv_row = sv[0]
        assert "short_volume" in sv_row
        assert "short_volume_ratio" in sv_row
        assert "total_volume" in sv_row

    async def test_short_interest_only_with_date_filter(self):
        mod = await _init_ginlix()
        result = await mod.get_short_data(
            "AAPL", data_type="short_interest",
            from_date="2025-01-01", to_date="2025-12-31", limit=5,
        )
        assert "error" not in result, result.get("error")
        data = result["data"]
        assert "short_interest" in data
        assert "short_volume" not in data
        assert len(data["short_interest"]) <= 5
        for row in data["short_interest"]:
            assert row["settlement_date"] >= "2025-01-01"

    async def test_short_volume_only(self):
        mod = await _init_ginlix()
        result = await mod.get_short_data("TSLA", data_type="short_volume", limit=3)
        assert "error" not in result, result.get("error")
        data = result["data"]
        assert "short_volume" in data
        assert "short_interest" not in data
        assert len(data["short_volume"]) <= 3
