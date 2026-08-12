"""Tests for price_data_mcp_server: envelope conformance, ordering, errors.

Exercises the standard agent-facing envelope (AGENT_CONTRACT.md): canonical
`symbol`/`interval`, `currency`/`timezone`, ascending `data`, machine-code
errors, and GBX→major-unit price conversion. Upstream fetches are mocked.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from .conftest import assert_error, assert_ok_envelope

_MOD = "mcp_servers.price_data_mcp_server"

# ---------------------------------------------------------------------------
# Canned data
# ---------------------------------------------------------------------------

# Raw FMP rows (exchange-local date strings; the server sorts + formats).
_RAW_ROWS = [
    {"date": "2025-01-02", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000},
    {"date": "2025-01-03", "open": 103, "high": 108, "low": 102, "close": 107, "volume": 1200},
]

# ginlix-data returns already-normalized display rows, DESCENDING (newest first).
_GINLIX_ROWS_DESC = [
    {"date": "2025-01-03", "open": 103, "high": 108, "low": 102, "close": 107, "volume": 1200},
    {"date": "2025-01-02", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000},
]

_SHORT_INTEREST_ROW = {
    "ticker": "AAPL", "settlement_date": "2025-03-14", "short_interest": 133_000_000,
    "avg_daily_volume": 59_000_000, "days_to_cover": 2.25,
}
_SHORT_VOLUME_ROW = {
    "ticker": "AAPL", "date": "2025-03-25", "short_volume": 181_219,
    "total_volume": 574_084, "short_volume_ratio": 31.57,
}


def _fmp_client(**overrides) -> AsyncMock:
    client = AsyncMock()
    client.get_stock_price = AsyncMock(return_value=overrides.get("stock_price", _RAW_ROWS))
    client.get_intraday_chart = AsyncMock(return_value=overrides.get("intraday", _RAW_ROWS))
    client.get_commodity_price = AsyncMock(return_value=overrides.get("commodity", _RAW_ROWS))
    client.get_crypto_price = AsyncMock(return_value=overrides.get("crypto", _RAW_ROWS))
    client.get_forex_price = AsyncMock(return_value=overrides.get("forex", _RAW_ROWS))
    client.get_commodity_intraday_chart = AsyncMock(return_value=overrides.get("commodity_intra", _RAW_ROWS))
    client.get_crypto_intraday_chart = AsyncMock(return_value=overrides.get("crypto_intra", _RAW_ROWS))
    client.get_forex_intraday_chart = AsyncMock(return_value=overrides.get("forex_intra", _RAW_ROWS))
    return client


def _force_fmp_path(mod):
    """Make the ginlix (US) client report 'no data' so the FMP path runs."""
    return patch.object(mod._ginlix, "fetch_stock_data", new=AsyncMock(return_value=None))


# ---------------------------------------------------------------------------
# get_stock_data
# ---------------------------------------------------------------------------

class TestGetStockData:
    @pytest.mark.asyncio
    async def test_daily_fmp_envelope(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with _force_fmp_path(mod), patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data("AAPL", interval="1day")

        assert_ok_envelope(
            result, symbol="AAPL", interval="1day", currency="USD",
            timezone="America/New_York", count=2, source="fmp",
        )
        assert "rows" not in result
        # Ascending, oldest-first.
        assert result["data"][0]["date"] == "2025-01-02"
        assert result["data"][1]["date"] == "2025-01-03"
        client.get_stock_price.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ginlix_source_flipped_to_ascending(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with patch.object(mod._ginlix, "fetch_stock_data",
                          new=AsyncMock(return_value=list(_GINLIX_ROWS_DESC))), \
             patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data(
                "AAPL", interval="1day", start_date="2025-01-01", end_date="2025-01-05",
            )

        assert_ok_envelope(result, source="ginlix-data")
        assert result["data"][0]["date"] == "2025-01-02"
        assert result["data"][-1]["date"] == "2025-01-03"
        client.get_stock_price.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_intraday_fmp(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with _force_fmp_path(mod), patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data(
                "AAPL", interval="5min", start_date="2025-01-01", end_date="2025-01-07",
            )

        assert_ok_envelope(result, interval="5min")
        client.get_intraday_chart.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_interval_alias_normalized(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with _force_fmp_path(mod), patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data("AAPL", interval="daily")

        assert_ok_envelope(result, interval="1day")

    @pytest.mark.asyncio
    async def test_gbx_converted_to_pounds(self):
        """VOD.L quotes arrive in pence (GBX) → returned in pounds with GBP."""
        import mcp_servers.price_data_mcp_server as mod

        raw = [{"date": "2025-01-02", "open": 10000, "high": 10500,
                "low": 9900, "close": 10300, "volume": 5000}]
        client = _fmp_client(stock_price=raw)
        with patch.object(mod._ginlix, "fetch_stock_data", new=AsyncMock(return_value=None)), \
             patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data("VOD.L", interval="1day")

        assert_ok_envelope(
            result, symbol="VOD.L", currency="GBP", timezone="Europe/London",
        )
        row = result["data"][0]
        assert row["open"] == 100.0   # 10000 pence → 100 pounds
        assert row["close"] == 103.0
        assert row["volume"] == 5000  # share count, unscaled
        # FMP is queried with the canonical legacy spelling (positional or kwarg).
        call = client.get_stock_price.await_args
        passed_symbol = call.kwargs.get("symbol", call.args[0] if call.args else None)
        assert passed_symbol == "VOD.L"

    @pytest.mark.asyncio
    async def test_unsupported_interval(self):
        import mcp_servers.price_data_mcp_server as mod

        result = await mod.get_stock_data("AAPL", interval="2min")
        assert_error(result, "unsupported_interval")
        assert "1day" in result["supported"]
        assert result["interval"] == "2min"

    @pytest.mark.asyncio
    async def test_one_second_falls_through_to_unsupported(self):
        """The dead 1s branch is gone; 1s is now a plain unsupported interval."""
        import mcp_servers.price_data_mcp_server as mod

        result = await mod.get_stock_data(
            "AAPL", interval="1s", start_date="2025-01-01", end_date="2025-01-02",
        )
        assert_error(result, "unsupported_interval")

    @pytest.mark.asyncio
    async def test_weekly_unsupported_for_stock(self):
        import mcp_servers.price_data_mcp_server as mod

        result = await mod.get_stock_data("AAPL", interval="1week")
        assert_error(result, "unsupported_interval")

    @pytest.mark.asyncio
    async def test_intraday_missing_dates(self):
        import mcp_servers.price_data_mcp_server as mod

        result = await mod.get_stock_data("AAPL", interval="5min")
        assert_error(result, "invalid_argument")

    @pytest.mark.asyncio
    async def test_fmp_init_error(self):
        import mcp_servers.price_data_mcp_server as mod

        with _force_fmp_path(mod), \
             patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await mod.get_stock_data("AAPL")

        assert_error(result, "client_unavailable")

    @pytest.mark.asyncio
    async def test_api_error(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        client.get_stock_price = AsyncMock(side_effect=Exception("timeout"))
        with _force_fmp_path(mod), patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data("AAPL")

        assert_error(result, "upstream_error")

    @pytest.mark.asyncio
    async def test_ginlix_404_maps_to_not_found(self):
        import mcp_servers.price_data_mcp_server as mod

        err = {"error": "ginlix-data error (404): symbol not found"}
        with patch.object(mod._ginlix, "fetch_stock_data", new=AsyncMock(return_value=err)):
            result = await mod.get_stock_data(
                "AAPL", interval="1day", start_date="2025-01-01", end_date="2025-01-02",
            )

        assert_error(result, "not_found")

    @pytest.mark.asyncio
    async def test_empty_rows(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client(stock_price=[])
        with _force_fmp_path(mod), patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data("AAPL")

        assert_ok_envelope(result, count=0)

    @pytest.mark.asyncio
    async def test_ohlcv_normalization(self):
        import mcp_servers.price_data_mcp_server as mod

        raw = [{"date": "2025-01-01", "open": "10", "high": None, "low": 9, "close": 10, "volume": 100}]
        client = _fmp_client(stock_price=raw)
        with _force_fmp_path(mod), patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data("AAPL")

        row = result["data"][0]
        assert row["open"] == 10.0   # string → float
        assert row["high"] is None   # None preserved
        assert row["date"] == "2025-01-01"


# ---------------------------------------------------------------------------
# get_asset_data
# ---------------------------------------------------------------------------

class TestGetAssetData:
    @pytest.mark.asyncio
    async def test_commodity_daily(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_asset_data("GCUSD", asset_type="commodity")

        assert_ok_envelope(result, source="fmp", currency="USD", count=2)
        assert result["asset_type"] == "commodity"
        assert result["data"][0]["date"] == "2025-01-02"  # ascending
        client.get_commodity_price.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_crypto_intraday_canonical_symbol(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_asset_data(
                "BTCUSD", asset_type="crypto", interval="5min",
                from_date="2025-01-01", to_date="2025-01-07",
            )

        assert_ok_envelope(
            result, symbol="BTC-USD", interval="5min", currency="USD", timezone="UTC",
        )
        client.get_crypto_intraday_chart.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_forex_daily(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_asset_data("EURUSD", asset_type="forex")

        assert_ok_envelope(result, symbol="EUR-USD")
        client.get_forex_price.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stock_routes_through_stock_path(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with _force_fmp_path(mod), patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_asset_data("AAPL", asset_type="stock", interval="1day")

        assert_ok_envelope(result, source="fmp")
        assert result["asset_type"] == "stock"
        assert result["data"][0]["date"] == "2025-01-02"

    @pytest.mark.asyncio
    async def test_invalid_asset_type(self):
        import mcp_servers.price_data_mcp_server as mod

        result = await mod.get_asset_data("X", asset_type="bond")
        assert_error(result, "invalid_argument")
        assert "supported" in result

    @pytest.mark.asyncio
    async def test_unsupported_intraday_for_commodity(self):
        import mcp_servers.price_data_mcp_server as mod

        client = _fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_asset_data("GCUSD", asset_type="commodity", interval="30min")

        assert_error(result, "unsupported_interval")

    @pytest.mark.asyncio
    async def test_intraday_missing_dates(self):
        import mcp_servers.price_data_mcp_server as mod

        result = await mod.get_asset_data("BTCUSD", asset_type="crypto", interval="5min")
        assert_error(result, "invalid_argument")


# ---------------------------------------------------------------------------
# get_short_data
# ---------------------------------------------------------------------------

class TestGetShortData:
    @pytest.mark.asyncio
    async def test_no_ginlix_client(self):
        import mcp_servers.price_data_mcp_server as mod

        with patch.object(mod._ginlix, "ensure", new=AsyncMock(return_value=False)):
            result = await mod.get_short_data("AAPL")

        assert_error(result, "client_unavailable")

    @pytest.mark.asyncio
    async def test_both_sections(self):
        import mcp_servers.price_data_mcp_server as mod

        canned = {
            "symbol": "AAPL", "source": "ginlix-data",
            "short_interest": [_SHORT_INTEREST_ROW],
            "short_volume": [_SHORT_VOLUME_ROW],
        }
        with patch.object(mod._ginlix, "ensure", new=AsyncMock(return_value=True)), \
             patch.object(mod._ginlix, "fetch_short_data", new=AsyncMock(return_value=canned)):
            result = await mod.get_short_data("AAPL")

        assert_ok_envelope(
            result, symbol="AAPL", source="ginlix-data",
            timezone="America/New_York", count=2,
        )
        assert result["data_type"] == "both"
        assert "currency" not in result  # short data carries no price fields
        assert result["data"]["short_interest"][0]["short_interest"] == 133_000_000
        assert result["data"]["short_volume"][0]["short_volume_ratio"] == 31.57

    @pytest.mark.asyncio
    async def test_short_interest_only_forwarded(self):
        import mcp_servers.price_data_mcp_server as mod

        canned = {"symbol": "AAPL", "source": "ginlix-data", "short_interest": [_SHORT_INTEREST_ROW]}
        fetch = AsyncMock(return_value=canned)
        with patch.object(mod._ginlix, "ensure", new=AsyncMock(return_value=True)), \
             patch.object(mod._ginlix, "fetch_short_data", new=fetch):
            result = await mod.get_short_data("AAPL", data_type="short_interest")

        assert "short_interest" in result["data"]
        assert "short_volume" not in result["data"]
        assert fetch.await_args.kwargs["data_type"] == "short_interest"

    @pytest.mark.asyncio
    async def test_partial_error_is_annotated_not_fatal(self):
        import mcp_servers.price_data_mcp_server as mod

        canned = {
            "symbol": "AAPL", "source": "ginlix-data",
            "short_interest": [_SHORT_INTEREST_ROW],
            "short_volume_error": "boom",
        }
        with patch.object(mod._ginlix, "ensure", new=AsyncMock(return_value=True)), \
             patch.object(mod._ginlix, "fetch_short_data", new=AsyncMock(return_value=canned)):
            result = await mod.get_short_data("AAPL")

        assert_ok_envelope(result)  # still a success envelope
        assert "short_interest" in result["data"]
        assert result["errors"]["short_volume"] == "boom"

    @pytest.mark.asyncio
    async def test_all_sections_failed(self):
        import mcp_servers.price_data_mcp_server as mod

        canned = {
            "symbol": "AAPL", "source": "ginlix-data",
            "short_interest_error": "x", "short_volume_error": "y",
        }
        with patch.object(mod._ginlix, "ensure", new=AsyncMock(return_value=True)), \
             patch.object(mod._ginlix, "fetch_short_data", new=AsyncMock(return_value=canned)):
            result = await mod.get_short_data("AAPL")

        assert_error(result, "upstream_error")


# ---------------------------------------------------------------------------
# normalize_bars — the descending helper the server flips to ascending
# ---------------------------------------------------------------------------

class TestNormalization:
    def test_normalize_ohlcv_descending(self):
        from data_client.normalize import normalize_bars

        rows = [
            {"date": "2025-01-01", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100},
            {"date": "2025-01-03", "open": 2, "high": 3, "low": 1, "close": 2.5, "volume": 200},
        ]
        result = normalize_bars(rows, "AAPL")
        assert result[0]["date"] == "2025-01-03"
        assert result[1]["date"] == "2025-01-01"

    def test_as_float_handles_edge_cases(self):
        from data_client.normalize import _as_float

        assert _as_float(None) is None
        assert _as_float("10.5") == 10.5
        assert _as_float("not_a_number") is None
        assert _as_float(42) == 42.0
