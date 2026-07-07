"""Tests for options_mcp_server: envelope conformance, ordering, errors.

Covers the standard agent-facing envelope (AGENT_CONTRACT.md): canonical
`symbol`/`interval`, currency/timezone derived from the UNDERLYING instrument,
ascending `data`, interval validation, and machine-code errors. Upstream
ginlix-data fetches are mocked — no live network.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from .conftest import assert_error, assert_ok_envelope

_MOD = "mcp_servers.options_mcp_server"

_OPT = "O:AAPL260618C00220000"

_CONTRACT = {
    "ticker": _OPT,
    "underlying_ticker": "AAPL",
    "contract_type": "call",
    "exercise_style": "american",
    "expiration_date": "2026-06-18",
    "strike_price": 220.0,
    "shares_per_contract": 100,
    "primary_exchange": "BATO",
}

# fetch_options_prices returns already-normalized display rows, DESCENDING.
_OPT_ROWS_DESC = [
    {"date": "2025-01-03", "open": 5.1, "high": 5.5, "low": 5.0, "close": 5.4, "volume": 300},
    {"date": "2025-01-02", "open": 5.0, "high": 5.3, "low": 4.9, "close": 5.1, "volume": 250},
]

_SNAP = {
    "ticker": _OPT,
    "name": "AAPL 260618 C 220",
    "market_status": "open",
    "session": {"open": 5.0, "high": 5.5, "low": 4.9, "close": 5.4, "volume": 1200},
    "last_quote": {"bid": 5.3, "ask": 5.5, "midpoint": 5.4},
    "last_trade": {"price": 5.4, "size": 3},
}


def _ensure(mod, value: bool):
    return patch.object(mod._ginlix, "ensure", new=AsyncMock(return_value=value))


# ---------------------------------------------------------------------------
# get_options_chain
# ---------------------------------------------------------------------------

class TestGetOptionsChain:
    @pytest.mark.asyncio
    async def test_no_ginlix_client(self):
        import mcp_servers.options_mcp_server as mod

        with _ensure(mod, False):
            result = await mod.get_options_chain("AAPL")

        assert_error(result, "client_unavailable", symbol="AAPL")

    @pytest.mark.asyncio
    async def test_success(self):
        import mcp_servers.options_mcp_server as mod

        fetch = AsyncMock(return_value={"results": [_CONTRACT]})
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_chain", new=fetch):
            result = await mod.get_options_chain("AAPL", contract_type="call")

        assert_ok_envelope(
            result, symbol="AAPL", currency="USD",
            timezone="America/New_York", count=1, source="ginlix-data",
        )
        assert result["underlying_ticker"] == "AAPL"
        assert result["data"][0]["ticker"] == _OPT

    @pytest.mark.asyncio
    async def test_upstream_error(self):
        import mcp_servers.options_mcp_server as mod

        fetch = AsyncMock(return_value={"error": "Failed to fetch options chain: boom"})
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_chain", new=fetch):
            result = await mod.get_options_chain("AAPL")

        assert_error(result, "upstream_error")

    @pytest.mark.asyncio
    async def test_not_found_from_status_code(self):
        import mcp_servers.options_mcp_server as mod

        fetch = AsyncMock(return_value={"error": "ginlix-data error (404): unknown ticker"})
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_chain", new=fetch):
            result = await mod.get_options_chain("AAPL")

        assert_error(result, "not_found")


# ---------------------------------------------------------------------------
# get_options_prices
# ---------------------------------------------------------------------------

class TestGetOptionsPrices:
    @pytest.mark.asyncio
    async def test_unsupported_interval(self):
        import mcp_servers.options_mcp_server as mod

        result = await mod.get_options_prices(_OPT, "2025-01-01", "2025-01-31", interval="3min")
        assert_error(result, "unsupported_interval", symbol=_OPT)
        assert "1day" in result["supported"]

    @pytest.mark.asyncio
    async def test_no_ginlix_client(self):
        import mcp_servers.options_mcp_server as mod

        with _ensure(mod, False):
            result = await mod.get_options_prices(_OPT, "2025-01-01", "2025-01-31")

        assert_error(result, "client_unavailable")

    @pytest.mark.asyncio
    async def test_success_ascending_and_underlying_currency(self):
        import mcp_servers.options_mcp_server as mod

        fetch = AsyncMock(return_value=list(_OPT_ROWS_DESC))
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_prices", new=fetch):
            result = await mod.get_options_prices(_OPT, "2025-01-01", "2025-01-31", interval="1day")

        assert_ok_envelope(
            result, symbol=_OPT, interval="1day", currency="USD",
            timezone="America/New_York", count=2, source="ginlix-data",
        )
        # currency is derived from the AAPL underlying.
        # Descending client rows → ascending, oldest-first.
        assert result["data"][0]["date"] == "2025-01-02"
        assert result["data"][-1]["date"] == "2025-01-03"

    @pytest.mark.asyncio
    async def test_interval_alias_normalized(self):
        import mcp_servers.options_mcp_server as mod

        fetch = AsyncMock(return_value=list(_OPT_ROWS_DESC))
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_prices", new=fetch):
            result = await mod.get_options_prices(_OPT, "2025-01-01", "2025-01-31", interval="1d")

        assert_ok_envelope(result, interval="1day")
        assert fetch.await_args.kwargs["interval"] == "1day"

    @pytest.mark.asyncio
    async def test_weekly_supported(self):
        """Options prices serve the full canonical vocab (incl. 1week)."""
        import mcp_servers.options_mcp_server as mod

        fetch = AsyncMock(return_value=list(_OPT_ROWS_DESC))
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_prices", new=fetch):
            result = await mod.get_options_prices(_OPT, "2025-01-01", "2025-03-31", interval="1week")

        assert_ok_envelope(result, interval="1week")

    @pytest.mark.asyncio
    async def test_error_dict_maps_to_rate_limited(self):
        import mcp_servers.options_mcp_server as mod

        fetch = AsyncMock(return_value={"error": "ginlix-data error (429): slow down"})
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_prices", new=fetch):
            result = await mod.get_options_prices(_OPT, "2025-01-01", "2025-01-31")

        assert_error(result, "rate_limited")


# ---------------------------------------------------------------------------
# get_options_snapshot
# ---------------------------------------------------------------------------

class TestGetOptionsSnapshot:
    @pytest.mark.asyncio
    async def test_no_tickers(self):
        import mcp_servers.options_mcp_server as mod

        result = await mod.get_options_snapshot("   ")
        assert_error(result, "invalid_argument")

    @pytest.mark.asyncio
    async def test_no_ginlix_client(self):
        import mcp_servers.options_mcp_server as mod

        with _ensure(mod, False):
            result = await mod.get_options_snapshot(_OPT)

        assert_error(result, "client_unavailable")

    @pytest.mark.asyncio
    async def test_success_single_underlying(self):
        import mcp_servers.options_mcp_server as mod

        canned = {"count": 1, "data": [_SNAP], "source": "ginlix-data"}
        fetch = AsyncMock(return_value=canned)
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_snapshot", new=fetch):
            result = await mod.get_options_snapshot(_OPT)

        assert_ok_envelope(
            result, source="ginlix-data", currency="USD",
            timezone="America/New_York", count=1,
        )
        assert result["data"][0]["ticker"] == _OPT
        assert "symbol" not in result  # batch payload has no single symbol

    @pytest.mark.asyncio
    async def test_currency_omitted_when_underlying_unresolvable(self):
        import mcp_servers.options_mcp_server as mod

        canned = {"count": 0, "data": [], "source": "ginlix-data"}
        fetch = AsyncMock(return_value=canned)
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_snapshot", new=fetch):
            result = await mod.get_options_snapshot("JUNK1,JUNK2")

        assert_ok_envelope(result, source="ginlix-data")
        assert "currency" not in result
        assert "timezone" not in result

    @pytest.mark.asyncio
    async def test_upstream_error(self):
        import mcp_servers.options_mcp_server as mod

        fetch = AsyncMock(return_value={"error": "Failed to fetch options snapshot: boom"})
        with _ensure(mod, True), patch.object(mod._ginlix, "fetch_options_snapshot", new=fetch):
            result = await mod.get_options_snapshot(_OPT)

        assert_error(result, "upstream_error")
