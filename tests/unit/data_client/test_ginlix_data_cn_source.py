"""Wire mapping for the CN market-data source over ginlix-data's v2 routes.

The fake transport answers with wire that ``market_protocol`` builds itself,
behind the real route contracts, so a drift between what ginlix-data serves
and what this source reads fails here rather than in a chart.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest

from market_protocol import PriceTreatment, Quote, Tier, build_series, to_canonical
from src.data_client.ginlix_data.cn_source import GinlixDataCnSource
from src.data_client.ginlix_data.v2_routes import GinlixDataV2Routes
from src.data_client.market_data_provider import MarketDataProvider, ProviderEntry
from src.data_client.normalize import snapshot_tier

_SHANGHAI = timezone(timedelta(hours=8))


def _ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=_SHANGHAI).timestamp() * 1000)


class _Routes(GinlixDataV2Routes):
    """The real route contracts over a scripted transport."""

    def __init__(self, status: int = 200, body: Any = None) -> None:
        self.status = status
        self.body = body
        self.calls: list[tuple[str, dict[str, Any], str | None]] = []

    async def _v2_get(self, path, params=None, *, user_id=None) -> httpx.Response:
        self.calls.append((path, dict(params or {}), user_id))
        request = httpx.Request("GET", f"http://ginlix-data.test{path}")
        return httpx.Response(self.status, json=self.body, request=request)


def _bars_wire() -> dict[str, Any]:
    rows = [
        {"time": _ms(2026, 3, 2), "open": 1500.0, "high": 1520.0, "low": 1490.0,
         "close": 1510.0, "volume": 1200.0},
        {"time": _ms(2026, 3, 3), "open": 1510.0, "high": 1530.0, "low": 1500.0,
         "close": 1525.0, "volume": 900.0},
    ]
    return build_series(
        rows, ref=to_canonical("600519.SH"), schema="ohlcv-1d", publisher="tushare",
        price_treatment=PriceTreatment.DIVIDEND_ADJUSTED, tier=Tier.DELAYED_15M,
    ).to_wire()


def _quote_wire(**overrides: Any) -> dict[str, Any]:
    quote = Quote(
        instrument_key="600519.XSHG", symbol="600519.SH", name="Kweichow Moutai",
        name_local="贵州茅台", price=1525.0, previous_close=1510.0, volume=900.0,
        currency="CNY", tier=Tier.REALTIME, publisher="tushare", as_of=_ms(2026, 3, 3, 14),
    ).model_dump(mode="json")
    quote.update(overrides)
    return quote


@pytest.mark.asyncio
async def test_daily_bars_map_the_series_records():
    wire = _bars_wire()
    routes = _Routes(body=wire)

    bars = await GinlixDataCnSource(routes).get_daily(
        "600519.SH", "2026-03-02", "2026-03-03", user_id="u-1"
    )

    assert [b["time"] for b in bars] == [r["ts_event"] for r in wire["records"]]
    assert bars[0] == {"time": _ms(2026, 3, 2), "open": 1500.0, "high": 1520.0,
                       "low": 1490.0, "close": 1510.0, "volume": 1200.0}
    path, params, user_id = routes.calls[0]
    assert path == "/api/v2/data/bars/600519.XSHG"
    assert user_id == "u-1"
    # Whole Shanghai days: the end bound is the next midnight less one ms.
    assert params == {"schema": "ohlcv-1d", "start": _ms(2026, 3, 2),
                      "end": _ms(2026, 3, 4) - 1}


@pytest.mark.asyncio
async def test_intraday_asks_for_the_minute_schema():
    routes = _Routes(body=_bars_wire())

    await GinlixDataCnSource(routes).get_intraday("600519.SH", "5min")

    assert routes.calls[0][1] == {"schema": "ohlcv-5m"}


@pytest.mark.asyncio
async def test_uncovered_series_is_empty():
    routes = _Routes(status=404, body={"detail": "no vendor covers 600519.XSHG"})

    assert await GinlixDataCnSource(routes).get_daily("600519.SH") == []


@pytest.mark.asyncio
async def test_a_record_with_a_null_price_fails_the_series_so_the_chain_falls_back():
    wire = _bars_wire()
    wire["records"][1]["close"] = None

    with pytest.raises(ValueError, match="close"):
        await GinlixDataCnSource(_Routes(body=wire)).get_daily("600519.SH")


@pytest.mark.asyncio
async def test_outage_raises_so_the_chain_falls_back():
    # An empty answer here would be cached under the pinned publisher; only a
    # raise sends the series cache to the next provider.
    routes = _Routes(status=503, body={"detail": "no CN vendor available"})
    source = GinlixDataCnSource(routes)

    with pytest.raises(httpx.HTTPStatusError):
        await source.get_daily("600519.SH")
    with pytest.raises(httpx.HTTPStatusError):
        await source.get_snapshots(["600519.SH"])


@pytest.mark.asyncio
async def test_quotes_are_stamped_with_the_requested_spelling():
    routes = _Routes(body={"quotes": {"600519.XSHG": _quote_wire()}})

    rows = await GinlixDataCnSource(routes).get_snapshots(["600519.SS"], user_id="u-1")

    assert routes.calls[0] == ("/api/v2/data/quotes", {"keys": "600519.SS"}, "u-1")
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "600519.SS"
    assert (row["name"], row["name_local"]) == ("Kweichow Moutai", "贵州茅台")
    assert (row["price"], row["previous_close"], row["volume"]) == (1525.0, 1510.0, 900)
    assert row["tier"] == "realtime"
    assert row["as_of"] == _ms(2026, 3, 3, 14)


@pytest.mark.asyncio
async def test_a_quote_without_a_tier_takes_the_declared_one():
    quote = _quote_wire()
    del quote["tier"]
    source = GinlixDataCnSource(_Routes(body={"quotes": {"600519.XSHG": quote}}))

    (row,) = await source.get_snapshots(["600519.SH"])
    assert "tier" not in row

    chain = MarketDataProvider([ProviderEntry("tushare", source, {"all"})])
    (served,) = await chain.get_snapshots(["600519.SH"])
    assert served["tier"] == snapshot_tier("tushare", "XSHG").value


@pytest.mark.asyncio
async def test_daily_without_a_start_runs_back_two_years_from_its_end():
    # Without a start ginlix-data answers with the whole listed history, all of
    # which would land in the live daily cache entry.
    routes = _Routes(body=_bars_wire())

    await GinlixDataCnSource(routes).get_daily("600519.SH", to_date="2026-03-03")

    assert routes.calls[0][1]["start"] == _ms(2024, 3, 3)


@pytest.mark.asyncio
async def test_other_markets_are_never_asked():
    # ginlix-data's v2 routes serve US listings too; an answer from here would
    # be published as tushare's.
    routes = _Routes(body={"quotes": {"600519.XSHG": _quote_wire()}})
    source = GinlixDataCnSource(routes)

    assert await source.get_daily("AAPL", "2026-03-02", "2026-03-03") == []
    assert await source.get_intraday("AAPL", "5min") == []
    rows = await source.get_snapshots(["AAPL", "600519.SH"])

    assert [r["symbol"] for r in rows] == ["600519.SH"]
    assert routes.calls == [("/api/v2/data/quotes", {"keys": "600519.SH"}, None)]
