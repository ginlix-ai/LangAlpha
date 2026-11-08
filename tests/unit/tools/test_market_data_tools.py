"""Regression tests for market_data tool implementation functions."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytz
from market_protocol import InvalidSymbol

from src.tools.market_data._shared import _normalize_market_bars
from src.tools.market_data.company import _quote_heading, fetch_company_overview
from src.tools.market_data.display import resolve_ref
from src.tools.market_data.market_overview import (
    fetch_market_overview,
    fetch_sector_performance,
)
from src.tools.market_data.prices import (
    _calculate_price_statistics,
    _format_price_data_as_table,
    _format_price_summary,
    fetch_daily_prices,
)
from src.tools.market_data.quotes import (
    fetch_market_movers,
    fetch_options_chain,
    fetch_quote,
)
from src.tools.market_data.quote_format import stamp_quote
from src.tools.market_data.screener import fetch_stock_screener

_PRICES_MOD = "src.tools.market_data.prices"
_COMPANY_MOD = "src.tools.market_data.company"
_MKT_MOD = "src.tools.market_data.market_overview"
_SCREEN_MOD = "src.tools.market_data.screener"
_QUOTES_MOD = "src.tools.market_data.quotes"
_DAILY_MOD = "src.server.services.cache.quote_daily_fallback"
_QCS_MOD = "src.server.services.cache.quote_cache_service"
_ET = pytz.timezone("US/Eastern")
_FIXED_ET = _ET.localize(datetime(2026, 7, 1, 14, 32, 5))


@pytest.fixture(autouse=True)
def _stub_cn_display_name():
    # Keep the CN/HK display-name lookups off the network for every test;
    # name-specific tests override with their own patch.
    with patch(f"{_PRICES_MOD}.display_names", new=AsyncMock(return_value=(None, None))), \
            patch(f"{_COMPANY_MOD}.display_names", new=AsyncMock(return_value=(None, None))), \
            patch(f"{_QUOTES_MOD}.fill_quote_names", new=AsyncMock()):
        yield


@pytest.fixture(autouse=True)
def _no_daily_quote_fallback():
    # A quote the provider drops reads the daily bars while its venue is
    # closed, which would make a test's result depend on the hour it runs.
    with patch(f"{_DAILY_MOD}.daily_fallback_snapshot", new=AsyncMock(return_value=None)):
        yield


@pytest.fixture(autouse=True)
def _no_company_snapshot():
    # The overview's snapshot fetch would otherwise reach the live provider
    # chain, whose HTTP client is not stopped by the socket guard, and print
    # whatever the market is doing right now. Snapshot tests patch their own.
    mdp = AsyncMock()
    mdp.get_snapshots = AsyncMock(return_value=[])
    with patch(f"{_COMPANY_MOD}.get_market_data_provider", return_value=mdp):
        yield

# ---------------------------------------------------------------------------
# Helpers — canned data
# ---------------------------------------------------------------------------

def _frozen_clock(at: datetime):
    """Patch the session rule's clock (``src.data_client.normalize``) to read *at* as now."""

    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz) if tz else at

    return patch("src.data_client.normalize.datetime", _Frozen)


def _make_daily_records(n: int, symbol: str = "AAPL", base_price: float = 150.0):
    """Generate n canned daily OHLCV records (newest-first) in formatter format."""
    records = []
    for i in range(n):
        day_offset = n - 1 - i
        price = base_price + day_offset * 0.5
        records.append({
            "date": f"2025-01-{(day_offset + 1):02d}",
            "symbol": symbol,
            "open": price,
            "high": price + 2.0,
            "low": price - 1.0,
            "close": price + 1.0,
            "volume": 1_000_000 + i * 100_000,
            "change": 1.0,
            "changePercent": 0.67,
            "vwap": price + 0.5,
        })
    return records


def _make_provider_bars(n: int, base_price: float = 150.0):
    """Generate n canned OHLCV bars in MarketDataSource format {time, open, high, low, close, volume}."""
    from datetime import timedelta

    base_dt = datetime(2025, 1, 1, 14, 30, tzinfo=timezone.utc)
    bars = []
    for i in range(n):
        price = base_price + i * 0.5
        ts = int((base_dt + timedelta(days=i)).timestamp() * 1000)
        bars.append({
            "time": ts,
            "open": price,
            "high": price + 2.0,
            "low": price - 1.0,
            "close": price + 1.0,
            "volume": 1_000_000 + i * 100_000,
        })
    return bars


def _make_fake_market_provider(
    *, daily_bars=None, intraday_bars=None, source="fmp", intraday_source=None,
):
    """Build a mock MarketDataProvider.

    The tools take the ``*_with_source`` variants so the artifact can name the
    provider that served each series; the plain variants stay mocked for the
    other callers of this helper. ``intraday_source`` defaults to ``source``;
    pass it to model the split where one provider serves the daily table and
    another the finer-grained chart.
    """
    provider = AsyncMock()
    provider.get_daily = AsyncMock(return_value=daily_bars or [])
    provider.get_intraday = AsyncMock(return_value=intraday_bars or [])
    provider.get_daily_with_source = AsyncMock(
        return_value=(daily_bars or [], source, False)
    )
    provider.get_intraday_with_source = AsyncMock(
        return_value=(intraday_bars or [], intraday_source or source, False)
    )
    return provider


def _make_fake_financial_source(
    *,
    profile_data=None,
    income_stmt=None,
    earnings_calendar=None,
    price_change=None,
    key_metrics=None,
    ratios=None,
    price_target_consensus=None,
    grades_summary=None,
    product_data=None,
    geo_data=None,
    quote_data=None,
    cash_flow=None,
    screener_results=None,
):
    """Build a mock FinancialDataSource."""
    src = AsyncMock()
    src.get_company_profile = AsyncMock(return_value=profile_data)
    src.get_income_statements = AsyncMock(return_value=income_stmt or [])
    src.get_earnings_history = AsyncMock(return_value=earnings_calendar or [])
    src.get_price_performance = AsyncMock(return_value=price_change or [])
    src.get_key_metrics = AsyncMock(return_value=key_metrics or [])
    src.get_financial_ratios = AsyncMock(return_value=ratios or [])
    src.get_analyst_price_targets = AsyncMock(return_value=price_target_consensus or [])
    src.get_analyst_ratings = AsyncMock(return_value=grades_summary or [])
    src.get_revenue_by_segment = AsyncMock(return_value=product_data or [])
    src.get_realtime_quote = AsyncMock(return_value=quote_data or [])
    src.get_cash_flows = AsyncMock(return_value=cash_flow or [])
    src.screen_stocks = AsyncMock(return_value=screener_results or [])
    return src


def _make_fake_financial_provider(financial=None, intel=None):
    """Build a mock FinancialDataProvider composite."""
    provider = MagicMock()
    provider.financial = financial
    provider.intel = intel
    return provider


# ---------------------------------------------------------------------------
# Pure helper tests (no mocking needed)
# ---------------------------------------------------------------------------

class TestCalculatePriceStatistics:
    def test_empty_data_returns_empty(self):
        assert _calculate_price_statistics([]) == {}
        assert _calculate_price_statistics(None) == {}

    def test_single_record(self):
        data = _make_daily_records(1)
        stats = _calculate_price_statistics(data)
        assert stats["period_days"] == 1
        assert stats["symbol"] == "AAPL"
        assert stats["period_open"] is not None
        assert stats["period_close"] is not None
        assert stats["volatility"] is None  # need >=2 points

    def test_basic_stats_with_20_records(self):
        data = _make_daily_records(20)
        stats = _calculate_price_statistics(data)

        assert stats["period_days"] == 20
        assert stats["ma_20"] is not None
        assert stats["ma_50"] is None  # only 20 records
        assert stats["ma_200"] is None
        assert stats["volatility"] is not None
        assert stats["avg_volume"] is not None
        assert stats["total_volume"] is not None
        # Performance
        assert stats["period_change"] is not None
        assert stats["period_change_pct"] is not None

    def test_moving_averages_thresholds(self):
        data = _make_daily_records(50)
        stats = _calculate_price_statistics(data)
        assert stats["ma_20"] is not None
        assert stats["ma_50"] is not None
        assert stats["ma_200"] is None

    def test_period_high_low(self):
        data = [
            {"date": "2025-01-01", "open": 100, "high": 110, "low": 90, "close": 105, "volume": 1000},
            {"date": "2025-01-02", "open": 105, "high": 120, "low": 95, "close": 115, "volume": 2000},
        ]
        stats = _calculate_price_statistics(data)
        assert stats["period_high"] == 120
        assert stats["period_low"] == 90
        assert stats["min_close"] == 105
        assert stats["max_close"] == 115


class TestNanSafety:
    """Non-finite floats (a forming Yahoo bar) must never poison output."""

    def test_normalize_market_bars_nan_close_does_not_poison_change(self):
        import math

        base = {"open": 10.0, "high": 11.0, "low": 9.0, "volume": 100}
        bars = [
            {**base, "time": 1_000_000, "close": 10.0},
            {**base, "time": 2_000_000, "close": float("nan")},
            {**base, "time": 3_000_000, "close": 12.0},
        ]
        result = _normalize_market_bars(bars, "TEST")

        for row in result:
            for key in ("close", "change", "changePercent"):
                v = row[key]
                assert v is None or math.isfinite(v), f"{key} is non-finite: {v}"

        # Newest-first: result[0] is the third bar. Its change must be
        # computed against the last FINITE close (10.0), not NaN.
        assert result[0]["close"] == 12.0
        assert result[0]["change"] == pytest.approx(2.0)
        assert result[0]["changePercent"] == pytest.approx(20.0)
        # The NaN bar itself renders as missing data.
        assert result[1]["close"] is None
        assert result[1]["change"] is None

    def test_calculate_price_statistics_nan_tail_row(self):
        import math

        data = _make_daily_records(5)
        data.insert(0, {  # newest-first: NaN forming bar at the head
            "date": "2025-02-01",
            "symbol": "AAPL",
            "open": float("nan"),
            "high": float("nan"),
            "low": float("nan"),
            "close": float("nan"),
            "volume": float("nan"),
        })
        stats = _calculate_price_statistics(data)

        for key, v in stats.items():
            if isinstance(v, float):
                assert math.isfinite(v), f"stats[{key!r}] is non-finite: {v}"

    def test_format_percentage_nan_is_na(self):
        from src.tools.market_data.utils import format_percentage

        assert format_percentage(float("nan")) == format_percentage(None) == "N/A"
        assert format_percentage(float("inf")) == "N/A"

    def test_format_percentage_finite_unchanged(self):
        from src.tools.market_data.utils import format_percentage

        assert format_percentage(5.234) == "+5.23%"
        assert format_percentage(-2.15) == "-2.15%"


class TestFormatPriceDataAsTable:
    def test_empty_data(self):
        assert _format_price_data_as_table([]) == "No price data available."
        assert _format_price_data_as_table(None) == "No price data available."

    def test_single_record_table(self):
        data = _make_daily_records(1)
        result = _format_price_data_as_table(data)
        assert "AAPL" in result
        assert "Daily Prices" in result
        assert "| Date" in result
        assert "Total Volume" in result

    def test_table_contains_all_records(self):
        data = _make_daily_records(5)
        result = _format_price_data_as_table(data)
        # Should have header + separator + 5 data rows
        table_lines = [l for l in result.split("\n") if l.startswith("|")]
        assert len(table_lines) == 7  # header + separator + 5 rows


class TestFormatPriceSummary:
    def test_empty_stats(self):
        assert _format_price_summary({}) == "No data available for summary"
        assert _format_price_summary(None) == "No data available for summary"

    def test_with_full_stats(self):
        data = _make_daily_records(20)
        stats = _calculate_price_statistics(data)
        result = _format_price_summary(stats)
        assert "Period:" in result
        assert "trading days" in result
        assert "| Metric | Value |" in result
        assert "Period Open" in result
        assert "20-Day MA" in result


# ---------------------------------------------------------------------------
# fetch_daily_prices
# ---------------------------------------------------------------------------

class TestFetchDailyPrices:
    @pytest.mark.asyncio
    async def test_side_reads_record_no_fallback_the_answer_did_not_take(self):
        # The live stamp and the intraday chart are extras to the daily table:
        # their misses are not the answer's, so they stay out of its note.
        from src.data_client.fallbacks import Fallback, record, recording

        def missed(what, result):
            async def read(*_, **__):
                record(Fallback("AAPL", what, "ginlix-data", "ConnectError", None))
                return result
            return read

        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(30))
        provider.get_daily_with_source.side_effect = missed(
            "daily bars", (_make_provider_bars(30), "fmp", False)
        )
        provider.get_intraday_with_source.side_effect = missed("intraday bars", ([], "fmp", False))
        provider.get_snapshots = AsyncMock(side_effect=missed("quote", []))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
                recording() as taken:
            await fetch_daily_prices("AAPL")

        provider.get_intraday_with_source.assert_awaited()
        provider.get_snapshots.assert_awaited()
        assert [f.what for f in taken] == ["daily bars"]

    @pytest.mark.asyncio
    async def test_short_period_returns_table(self):
        """< 14 days should return markdown table format."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("AAPL", limit=5)

        assert "Daily Prices" in content
        assert "| Date" in content
        assert artifact["type"] == "stock_prices"
        assert artifact["symbol"] == "AAPL"
        assert len(artifact["ohlcv"]) == 5

    @pytest.mark.asyncio
    async def test_forming_session_is_marked_in_the_table(self):
        # Bars open 2025-01-01..03; the 01-03 session is still running.
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(3))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
                _frozen_clock(_ET.localize(datetime(2025, 1, 3, 11, 0))):
            content, _ = await fetch_daily_prices("AAPL", limit=3)

        assert "| 2025-01-03 (in progress) |" in content
        assert "| 2025-01-02 |" in content

    @pytest.mark.asyncio
    async def test_forming_session_is_marked_in_the_summary(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(17))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
                _frozen_clock(_ET.localize(datetime(2025, 1, 17, 11, 0))):
            content, _ = await fetch_daily_prices("AAPL", limit=17)

        assert "| Period Close (in progress) |" in content

    @pytest.mark.asyncio
    async def test_settled_session_is_not_marked(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(3))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
                _frozen_clock(_ET.localize(datetime(2025, 1, 3, 20, 1))):
            content, _ = await fetch_daily_prices("AAPL", limit=3)

        assert "(in progress)" not in content

    @pytest.mark.asyncio
    async def test_long_period_returns_summary(self):
        """>= 14 days should return formatted summary."""
        bars = _make_provider_bars(20)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("AAPL", limit=20)

        assert "| Metric | Value |" in content
        assert "Period Open" in content
        assert artifact["type"] == "stock_prices"
        assert artifact["stats"]["ma_20"] is not None

    @pytest.mark.asyncio
    async def test_empty_results(self):
        """No data should return 'No data available' message."""
        provider = _make_fake_market_provider(daily_bars=[])

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("FAKE")

        assert "No data available" in content
        assert artifact["type"] == "stock_prices"
        assert artifact["symbol"] == "FAKE"

    @pytest.mark.asyncio
    async def test_date_range_query(self):
        """Using start_date/end_date should call get_daily with dates."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices(
                "AAPL", start_date="2025-01-01", end_date="2025-01-05"
            )

        provider.get_daily_with_source.assert_called_once_with(
            "AAPL", from_date="2025-01-01", to_date="2025-01-05",
            is_index=False, user_id=None,
        )
        assert artifact["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_caret_symbol_routes_as_index(self):
        """Caret-prefixed symbols get index-market routing in the provider chain."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            await fetch_daily_prices(
                "^GSPC", start_date="2025-01-01", end_date="2025-01-05"
            )

        assert provider.get_daily_with_source.call_args.kwargs["is_index"] is True

    @pytest.mark.asyncio
    async def test_intraday_fetched_for_short_period(self):
        """Periods <= 60 days should attempt intraday data."""
        daily_bars = _make_provider_bars(5)
        intraday_bars = _make_provider_bars(50, base_price=150.0)
        provider = _make_fake_market_provider(
            daily_bars=daily_bars, intraday_bars=intraday_bars
        )

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("AAPL", limit=5)

        provider.get_intraday_with_source.assert_called_once()
        assert artifact["chart_interval"] == "5min"

    @pytest.mark.asyncio
    async def test_intraday_failure_falls_back_to_daily(self):
        """If intraday fetch fails, chart_ohlcv should use daily data."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)
        provider.get_intraday_with_source = AsyncMock(side_effect=Exception("API error"))

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("AAPL", limit=5)

        assert artifact["chart_interval"] == "daily"

    @pytest.mark.asyncio
    async def test_default_limit_applied(self):
        """No args should default to limit=60."""
        bars = _make_provider_bars(60)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("AAPL")

        # Should call with date range (limit logic converts to date range)
        provider.get_daily_with_source.assert_called_once()


class _RecordingClient:
    """Stands in for the LangSmith client: records the runs it would update."""

    def __init__(self) -> None:
        self.updated: list[dict] = []

    def create_run(self, **kwargs) -> None:
        return None

    def update_run(self, **kwargs) -> None:
        self.updated.append(kwargs)

    def flush(self) -> None:
        return None


class TestDailyPricesOutcome:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("symbol", "markets", "raised", "outcome", "error_class", "text"),
        [
            pytest.param(
                "0700.HK", {"us"}, RuntimeError("upstream 503"),
                "unavailable", "MarketNotCovered",
                "No data source configured for market of 0700.HK",
                id="market-not-covered",
            ),
            pytest.param(
                "AAPL", {"all"}, InvalidSymbol("Symbol 'A B' contains ' '"),
                "ok", None, "Symbol 'A B' contains ' '",
                id="invalid-symbol",
            ),
            pytest.param(
                "AAPL", {"all"}, RuntimeError("upstream 503"),
                "error", "RuntimeError", "upstream 503",
                id="source-failed",
            ),
            pytest.param(
                "AAPL", {"all"}, None,
                "unavailable", "no_data", "No price data available",
                id="no-bars",
            ),
        ],
    )
    async def test_only_a_source_failing_is_an_error(
        self, symbol, markets, raised, outcome, error_class, text
    ):
        """A market no source covers, a symbol nothing is spelled as, or a
        range the symbol has no bars for is the agent's ask, not a source
        failing, so it stays out of the tool's error rate; its text is
        unchanged."""
        from src.data_client.market_data_provider import (
            MarketDataProvider,
            ProviderEntry,
        )
        from src.observability.langsmith_tracer import NonRetainingTracer
        from src.tools.market_data.tool import get_daily_prices

        source = AsyncMock()
        source.get_daily = AsyncMock(side_effect=raised, return_value=[])
        provider = MarketDataProvider([ProviderEntry("ginlix", source, markets)])
        client = _RecordingClient()

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            msg = await get_daily_prices.ainvoke(
                {
                    "type": "tool_call",
                    "name": "get_daily_prices",
                    "args": {"symbol": symbol},
                    "id": "tc-1",
                },
                {"callbacks": [NonRetainingTracer(project_name="proj", client=client)]},
            )

        assert text in msg.content
        (update,) = [u for u in client.updated if u["run_type"] == "tool"]
        metadata = update["extra"]["metadata"]
        assert metadata["tool_outcome"] == outcome
        assert metadata.get("error_class") == error_class
        assert (update["error"] is not None) == (outcome == "error")


class TestChartSeriesProvenance:
    @pytest.mark.asyncio
    async def test_daily_chart_artifact_names_source_and_freshness(self):
        bars = _make_provider_bars(20)
        provider = _make_fake_market_provider(daily_bars=bars, source="tushare")

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("AAPL", limit=20)

        assert artifact["chart_interval"] == "daily"
        assert artifact["source"] == "tushare"
        assert artifact["freshness"]["interval"] == "1day"
        assert artifact["freshness"]["source"] == "tushare"
        assert artifact["freshness"]["measured"] is True
        # With no intraday series the chart keys restate the daily ones.
        assert artifact["chart_source"] == "tushare"
        assert artifact["chart_freshness"] == artifact["freshness"]
        # The freshness line is about intraday chart bars only.
        assert "Chart bars:" not in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "end_date, open_ended",
        [("2026-07-01", True), ("2026-07-01 10:30", False), ("2026-07-01 15:00", True)],
    )
    async def test_an_intraday_end_is_open_only_at_or_after_the_venue_clock(
        self, end_date, open_ended
    ):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        venue_now = datetime(2026, 7, 1, 14, 32)  # venue-local, naive is enough

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
             patch(f"{_PRICES_MOD}.venue_local_time", return_value=venue_now):
            _, artifact = await fetch_daily_prices(
                "AAPL", start_date="2026-06-01", end_date=end_date
            )

        assert ("freshness" in artifact) is open_ended

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "end_date, open_ended",
        [("2026-07-01T07:00:00Z", True), ("2026-07-01T15:00:00+09:00", False)],
    )
    async def test_an_end_with_an_offset_is_compared_as_an_instant(
        self, end_date, open_ended
    ):
        # 14:32 in Shanghai is 06:32Z. Read as wall clocks, 07:00Z falls before
        # it and 15:00+09:00 (06:00Z) after it.
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        venue_now = datetime(2026, 7, 1, 14, 32, tzinfo=ZoneInfo("Asia/Shanghai"))

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
             patch(f"{_PRICES_MOD}.venue_local_time", return_value=venue_now):
            _, artifact = await fetch_daily_prices(
                "600519.SS", start_date="2026-06-01", end_date=end_date
            )

        assert ("freshness" in artifact) is open_ended

    @pytest.mark.asyncio
    async def test_intraday_chart_freshness_is_measured_and_stated(self):
        daily_bars = _make_provider_bars(5)
        intraday_bars = _make_provider_bars(50)
        provider = _make_fake_market_provider(
            daily_bars=daily_bars, intraday_bars=intraday_bars, source="fmp"
        )

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("AAPL", limit=5)

        assert artifact["chart_interval"] == "5min"
        assert artifact["chart_source"] == "fmp"
        assert artifact["chart_freshness"]["interval"] == "5min"
        # The canned bars are years old, so the series is not current and the
        # agent-visible text has to say which provider served it.
        assert artifact["chart_freshness"]["label"] != "live"
        assert "**Chart bars:** fmp" in content

    @pytest.mark.asyncio
    async def test_card_source_is_the_daily_publisher_not_the_charts(self):
        # The inline card plots `ohlcv` (the daily table) but badges `source`
        # and `freshness`, so a chart filled by another provider must not end
        # up crediting that provider for the table.
        provider = _make_fake_market_provider(
            daily_bars=_make_provider_bars(5),
            intraday_bars=_make_provider_bars(50),
            source="tushare",
            intraday_source="fmp",
        )

        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("600519.SS", limit=5)

        assert artifact["chart_interval"] == "5min"
        assert artifact["source"] == "tushare"
        assert artifact["freshness"]["source"] == "tushare"
        assert artifact["freshness"]["interval"] == "1day"
        assert artifact["chart_source"] == "fmp"
        assert artifact["chart_freshness"]["source"] == "fmp"

    @pytest.mark.asyncio
    async def test_the_daily_rows_name_their_price_treatment(self):
        # A CN fallback serves unadjusted closes where Tushare's are
        # forward-adjusted; the text has to say which the rows are.
        for source, words in (
            ("tushare", "adjusted for splits and dividends"), ("fmp", "unadjusted"),
        ):
            provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5), source=source)
            with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
                content, _ = await fetch_daily_prices("600519.SS", limit=5)
            assert f"**Prices:** {words}, from {source}" in content


class TestFreshnessStamp:
    @pytest.mark.asyncio
    async def test_daily_prices_stamped_when_live(self):
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)
        provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "AAPL", "price": 210.0, "change_percent": 1.1,
             "volume": 1_000, "last_trade_price": 211.50, "market_status": "open",
             "tier": "realtime"},
        ])
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
             patch("src.tools.market_data.quote_format.get_market_session",
                   return_value=("REGULAR_HOURS", _FIXED_ET)), \
             patch("src.data_client.freshness._now", side_effect=lambda now: now or _FIXED_ET):
            content, _ = await fetch_daily_prices("AAPL", limit=5)
        assert content.startswith("[Live: AAPL $211.50")

    @pytest.mark.asyncio
    async def test_daily_prices_unstamped_when_closed(self):
        # A priced snapshot IS available — only the CLOSED session gate must
        # suppress the stamp (an empty snapshot list would suppress it in any
        # session and prove nothing about the gate).
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)
        provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "AAPL", "price": 210.0, "change_percent": 1.1,
             "volume": 1_000, "last_trade_price": 211.50, "market_status": "closed"},
        ])
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
             patch("src.tools.market_data.quote_format.get_market_session",
                   return_value=("CLOSED", _FIXED_ET)):
            content, _ = await fetch_daily_prices("AAPL", limit=5)
        assert not content.startswith("[Live:")

    @pytest.mark.asyncio
    async def test_daily_prices_snapshot_failure_is_silent(self):
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)
        provider.get_snapshots = AsyncMock(side_effect=RuntimeError("down"))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_daily_prices("AAPL", limit=5)
        assert "Daily Prices" in content  # normal output, no crash


# ---------------------------------------------------------------------------
# fetch_company_overview
# ---------------------------------------------------------------------------

class TestFetchCompanyOverview:
    @pytest.fixture
    def full_profile(self):
        return [{
            "companyName": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "marketCap": 3_500_000_000_000,
            "price": 235.50,
            "exchangeShortName": "NASDAQ",
        }]

    @pytest.fixture
    def full_financial(self, full_profile):
        return _make_fake_financial_source(
            profile_data=full_profile,
            income_stmt=[{
                "date": "2025-06-30",
                "period": "Q3",
                "fiscalYear": "2025",
                "revenue": 94_000_000_000,
                "netIncome": 23_000_000_000,
                "grossProfit": 44_000_000_000,
                "operatingIncome": 29_000_000_000,
                "ebitda": 32_000_000_000,
                "epsdiluted": 1.52,
                "grossProfitRatio": 0.468,
                "operatingIncomeRatio": 0.308,
                "netIncomeRatio": 0.245,
            }],
            earnings_calendar=[{
                "date": "2025-07-24",
                "epsActual": 1.52,
                "epsEstimated": 1.45,
                "revenueActual": 94_000_000_000,
                "revenueEstimated": 92_000_000_000,
                "fiscalDateEnding": "2025-06-30",
            }],
            price_change=[{"1D": 0.5, "5D": 1.2, "1M": -2.3, "ytd": 15.0, "1Y": 30.0}],
            # FMP stable key-metrics-ttm / ratios-ttm spellings.
            key_metrics=[{"returnOnEquityTTM": 1.60, "returnOnAssetsTTM": 0.30, "evToOperatingCashFlowTTM": 28.4}],
            ratios=[{
                "priceToEarningsRatioTTM": 35.2,
                "priceToEarningsGrowthRatioTTM": 2.1,
                "interestCoverageRatioTTM": 29.5,
                "priceToBookRatioTTM": 50.0,
                "netProfitMarginTTM": 0.245,
                "debtToEquityRatioTTM": 1.87,
                "currentRatioTTM": 0.99,
            }],
            price_target_consensus=[{"targetMedian": 260.0, "targetLow": 200.0, "targetHigh": 300.0, "targetConsensus": "Buy"}],
            grades_summary=[{"strongBuy": 10, "buy": 15, "hold": 5, "sell": 1, "strongSell": 0, "consensus": "Buy"}],
            quote_data=[{
                "price": 235.50, "change": 2.30, "changePercentage": 0.99,
                "dayHigh": 237.0, "dayLow": 233.0, "yearHigh": 260.0, "yearLow": 165.0,
                "open": 234.0, "previousClose": 233.20, "volume": 55_000_000, "avgVolume": 60_000_000,
                "marketCap": 3_500_000_000_000,
            }],
            cash_flow=[{
                "date": "2025-06-30",
                "operatingCashFlow": 28_000_000_000,
                "capitalExpenditure": -3_000_000_000,
                "freeCashFlow": 25_000_000_000,
            }],
            product_data=[{"2025-06-30": {"iPhone": 46_000_000_000, "Services": 24_000_000_000}}],
            geo_data=[{"2025-06-30": {"Americas": 40_000_000_000, "Europe": 25_000_000_000}}],
        )

    @pytest.mark.asyncio
    async def test_full_overview(self, full_financial, full_profile):
        """Full data should produce comprehensive formatted output."""
        provider = _make_fake_financial_provider(financial=full_financial)
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            content, artifact = await fetch_company_overview("AAPL")

        assert "Apple Inc." in content
        assert "Technology" in content
        # The fixture's provider quote carries no tier and no print time.
        assert "### Quote (freshness unknown)" in content
        assert "Real-Time Quote" not in content
        assert "Stock Price Performance" in content
        assert "Key Financial Metrics" in content
        assert "| P/E Ratio | 35.20x |" in content
        assert "| PEG Ratio | 2.10 |" in content
        assert "| P/B Ratio | 50.00x |" in content
        assert "| EV/OCF | 28.40x |" in content
        # A return above 100% is still a fraction on the wire.
        assert "| ROE (Return on Equity) | 160.00% |" in content
        assert "| Interest Coverage | 29.50x |" in content
        assert "| Net Profit Margin | 24.50% |" in content
        assert "| Debt/Equity Ratio | 1.87 |" in content
        assert "| Current Ratio | 0.99 |" in content
        assert "Earnings Performance" in content
        assert "Analyst Consensus" in content
        assert "Revenue Breakdown" in content
        assert artifact["type"] == "company_overview"
        assert artifact["symbol"] == "AAPL"
        assert artifact["name"] == "Apple Inc."
        assert "quote" in artifact
        assert "performance" in artifact
        assert "analystRatings" in artifact

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", ["epsDiluted", "epsdiluted"])
    async def test_diluted_eps_reads_the_stable_and_legacy_spellings(self, full_profile, key):
        """FMP's stable statements spell it epsDiluted; Tushare-mapped and
        yfinance rows keep epsdiluted."""
        financial = _make_fake_financial_source(
            profile_data=full_profile,
            income_stmt=[{"date": "2025-06-30", "revenue": 94_000_000_000, key: 1.57}],
        )
        provider = _make_fake_financial_provider(financial=financial)
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            _, artifact = await fetch_company_overview("AAPL")
        assert artifact["quarterlyFundamentals"][0]["epsDiluted"] == 1.57

    @pytest.mark.asyncio
    async def test_cn_profile_prefers_chinese_name(self):
        """A tushare profile's nameLocal wins over the English companyName."""
        financial = _make_fake_financial_source(profile_data=[{
            "companyName": "Alpha Co., Ltd.",
            "nameLocal": "甲公司",
            "sector": "Technology",
            "industry": "Semiconductors",
            "marketCap": 1_000_000_000_000,
            "price": 42.0,
            "exchangeShortName": "SSE",
        }])
        provider = _make_fake_financial_provider(financial=financial)
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            content, artifact = await fetch_company_overview("600001.SS")

        assert "**Company:** 甲公司" in content
        assert artifact["name"] == "甲公司"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stable", [False, True], ids=["legacy", "stable"])
    async def test_cn_metrics_render_through_the_cn_source(self, stable):
        """ginlix-data's CN snapshot rows carry no TTM suffix; the cn source
        renames them to FMP's keys, so the table fills from the same reads,
        whether the rows use FMP's legacy spellings or its stable ones."""
        from src.data_client.financial_data_provider import MarketRoute, RoutedFinancialSource
        from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

        pe, de = ("priceToEarningsRatio", "debtToEquityRatio") if stable else ("peRatio", "debtToEquity")
        rows = {
            "profile": [{
                "symbol": "600001.XSHG",
                "companyName": "Alpha Co., Ltd.",
                "nameLocal": "\u7532\u516c\u53f8",
                "sector": None,
                "industry": None,
                "marketCap": 1_000_000_000_000,
                "price": 42.0,
                "exchange": "SSE",
                **({} if stable else {"exchangeShortName": "SSE"}),
            }],
            "key_metrics": [{
                "symbol": "600001.XSHG",
                "returnOnEquity": 0.25,
                "returnOnAssets": 0.12,
            }],
            "financial_ratios": [{
                "symbol": "600001.XSHG",
                pe: 20.0,
                "priceToBookRatio": 4.0,
                "netProfitMargin": 0.40,
                "operatingProfitMargin": 0.50,
                de: 0.30,
                "currentRatio": 3.0,
                "quickRatio": 2.5,
            }],
        }
        client = MagicMock()
        client.get_fundamentals_v2 = AsyncMock(side_effect=lambda key, kind, **_: rows.get(kind, []))
        financial = RoutedFinancialSource(
            default=_make_fake_financial_source(profile_data=[]),
            by_market={"cn": MarketRoute(GinlixDataCnFinancialSource(client))},
        )
        provider = _make_fake_financial_provider(financial=financial)
        market = MagicMock()
        market.get_snapshots = AsyncMock(return_value=[])
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}.get_market_data_provider", AsyncMock(return_value=market)),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            content, _ = await fetch_company_overview("600001.SS")

        assert "| P/E Ratio | 20.00x |" in content
        assert "| P/B Ratio | 4.00x |" in content
        assert "| ROE (Return on Equity) | 25.00% |" in content
        assert "| ROA (Return on Assets) | 12.00% |" in content
        assert "| Net Profit Margin | 40.00% |" in content
        assert "| Operating Margin | 50.00% |" in content
        assert "| Debt/Equity Ratio | 0.30 |" in content
        assert "| Current Ratio | 3.00 |" in content
        assert "| Quick Ratio | 2.50 |" in content
        assert "**Market:** SSE" in content
        # A key present with a None value must not print the literal "None".
        assert "Sector: N/A | Industry: N/A" in content

    @pytest.mark.asyncio
    async def test_past_row_without_eps_is_not_an_upcoming_report(self):
        """epsActual is None on a past date means never populated, not upcoming."""
        financial = _make_fake_financial_source(
            profile_data=[{"companyName": "Alpha Co., Ltd.", "price": 42.0}],
            earnings_calendar=[
                {"date": "2020-01-31", "epsActual": None, "fiscalDateEnding": "2019-12-31"},
            ],
        )
        provider = _make_fake_financial_provider(financial=financial)
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            content, _ = await fetch_company_overview("600001.SS")

        assert "Next Earnings Report" not in content

    @pytest.mark.asyncio
    async def test_future_row_without_eps_is_an_upcoming_report(self):
        financial = _make_fake_financial_source(
            profile_data=[{"companyName": "Alpha Co., Ltd.", "price": 42.0}],
            earnings_calendar=[
                {"date": "2099-01-31", "epsActual": None, "fiscalDateEnding": "2098-12-31"},
            ],
        )
        provider = _make_fake_financial_provider(financial=financial)
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            content, _ = await fetch_company_overview("600001.SS")

        assert "Next Earnings Report" in content
        assert "2099-01-31" in content

    @pytest.mark.asyncio
    async def test_missing_profile_returns_error(self):
        """Missing profile should return error content."""
        financial = _make_fake_financial_source(profile_data=[])
        provider = _make_fake_financial_provider(financial=financial)

        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            content, artifact = await fetch_company_overview("FAKE")

        assert "No data found for symbol FAKE" in content
        assert artifact["type"] == "company_overview"
        assert "error" not in artifact  # it's not an exception error

    @pytest.mark.asyncio
    async def test_partial_data_handled_gracefully(self, full_profile):
        """Some gather calls raising exceptions should not crash."""
        financial = _make_fake_financial_source(
            profile_data=full_profile,
            quote_data=[{
                "price": 235.50, "change": 2.30, "changePercentage": 0.99,
                "dayHigh": 237.0, "dayLow": 233.0, "yearHigh": 260.0, "yearLow": 165.0,
                "open": 234.0, "previousClose": 233.20, "volume": 55_000_000,
                "avgVolume": 60_000_000, "marketCap": 3_500_000_000_000,
            }],
        )
        # Make some calls raise exceptions (simulating partial failures)
        financial.get_income_statements = AsyncMock(side_effect=Exception("API error"))
        financial.get_price_performance = AsyncMock(side_effect=Exception("timeout"))
        provider = _make_fake_financial_provider(financial=financial)

        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            content, artifact = await fetch_company_overview("AAPL")

        # Should still have basic profile info
        assert "Apple Inc." in content
        assert artifact["type"] == "company_overview"
        # Should NOT have sections dependent on failed calls
        assert "Stock Price Performance" not in content

    @pytest.mark.asyncio
    async def test_provider_exception_returns_error(self):
        """get_financial_data_provider raising should produce error content."""
        with patch(f"{_COMPANY_MOD}.get_financial_data_provider", side_effect=Exception("Connection failed")):
            content, artifact = await fetch_company_overview("AAPL")

        assert "Error" in content
        assert "error" in artifact

    @pytest.mark.asyncio
    async def test_boundary_normalizes_symbol_before_provider(self):
        """A non-canonical spelling reaches the provider canonicalized (FIX 3).

        ``zzzz.us`` is a neutral placeholder: lowercase + ``.US`` suffix, which
        the protocol boundary folds to bare ``ZZZZ`` before any provider call.
        """
        financial = _make_fake_financial_source(profile_data=None)  # early-return path
        provider = _make_fake_financial_provider(financial=financial)
        with patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider):
            await fetch_company_overview("zzzz.us")

        financial.get_company_profile.assert_called_once_with("ZZZZ")

    @pytest.mark.asyncio
    async def test_malformed_snapshot_stamp_failure_does_not_lose_report(
        self, full_financial, full_profile
    ):
        """A snapshot with an unparsable price must not blow away the whole report."""
        provider = _make_fake_financial_provider(financial=full_financial)
        market_provider = _make_fake_market_provider()
        market_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "AAPL", "market_status": "open", "last_trade_price": "N/A"},
        ])
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}.get_market_data_provider", return_value=market_provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
            patch("src.tools.market_data.quote_format.get_market_session",
                  return_value=("REGULAR_HOURS", _FIXED_ET)),
        ):
            content, artifact = await fetch_company_overview("AAPL")

        assert "Apple Inc." in content
        assert "Technology" in content
        assert artifact["type"] == "company_overview"
        assert artifact["symbol"] == "AAPL"
        assert "error" not in artifact
        assert not content.startswith("[Live:")

    @pytest.mark.asyncio
    async def test_stamped_when_snapshot_valid_and_market_open(
        self, full_financial, full_profile
    ):
        """Valid snapshot + open market should prefix content with the [Live: ...] stamp."""
        provider = _make_fake_financial_provider(financial=full_financial)
        market_provider = _make_fake_market_provider()
        market_provider.get_snapshots = AsyncMock(return_value=[
            {
                "symbol": "AAPL",
                "price": 235.50,
                "last_trade_price": 236.10,
                "change_percent": 0.99,
                "market_status": "open",
                "tier": "realtime",
            },
        ])
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}.get_market_data_provider", return_value=market_provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
            patch("src.tools.market_data.quote_format.get_market_session",
                  return_value=("REGULAR_HOURS", _FIXED_ET)),
            patch("src.data_client.freshness._now", side_effect=lambda now: now or _FIXED_ET),
        ):
            content, artifact = await fetch_company_overview("AAPL")

        assert content.startswith("[Live: AAPL $236.10")
        assert "### Real-Time Quote" in content

    @staticmethod
    async def _overview_metrics(profile, key_metrics, ratios):
        financial = _make_fake_financial_source(
            profile_data=profile, key_metrics=key_metrics, ratios=ratios,
        )
        provider = _make_fake_financial_provider(financial=financial)
        market_provider = _make_fake_market_provider()
        market_provider.get_snapshots = AsyncMock(return_value=[])
        with (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}.get_market_data_provider", return_value=market_provider),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        ):
            content, _ = await fetch_company_overview("AAPL")
        return content

    @pytest.mark.asyncio
    async def test_key_metrics_read_fmp_stable_ttm_fields(self, full_profile):
        """The table reads the field names FMP's stable key-metrics-ttm and
        ratios-ttm endpoints actually return, and prints ROE above 100% as such.
        """
        key_metrics = [{
            "symbol": "AAPL",
            "marketCap": 3_500_000_000_000,
            "evToOperatingCashFlowTTM": 35.66,
            "returnOnEquityTTM": 1.55,
            "returnOnAssetsTTM": 0.30,
            "currentRatioTTM": 0.87,
        }]
        ratios = [{
            "symbol": "AAPL",
            "priceToEarningsRatioTTM": 38.44,
            "priceToBookRatioTTM": 57.97,
            "priceToEarningsGrowthRatioTTM": 14.54,
            "netProfitMarginTTM": 0.243,
            "operatingProfitMarginTTM": 0.319,
            "debtToEquityRatioTTM": 1.54,
            "currentRatioTTM": 0.87,
            "quickRatioTTM": 0.83,
            "interestCoverageRatioTTM": 30.5,
        }]
        content = await self._overview_metrics(full_profile, key_metrics, ratios)

        assert "| P/E Ratio | 38.44x |" in content
        assert "| P/B Ratio | 57.97x |" in content
        assert "| PEG Ratio | 14.54 |" in content
        assert "| ROE (Return on Equity) | 155.00% |" in content
        assert "| ROA (Return on Assets) | 30.00% |" in content
        assert "| Debt/Equity Ratio | 1.54 |" in content
        assert "| Interest Coverage | 30.50x |" in content

    @pytest.mark.asyncio
    async def test_key_metrics_fields_unaffected_by_rename(self, full_profile):
        """Control: fields whose stable names never changed render as before."""
        content = await self._overview_metrics(
            full_profile,
            [{"symbol": "AAPL", "evToOperatingCashFlowTTM": 35.66}],
            [{
                "netProfitMarginTTM": 0.243,
                "operatingProfitMarginTTM": 0.319,
                "currentRatioTTM": 0.87,
                "quickRatioTTM": 0.83,
            }],
        )
        assert "| EV/OCF | 35.66x |" in content
        assert "| Net Profit Margin | 24.30% |" in content
        assert "| Operating Margin | 31.90% |" in content
        assert "| Current Ratio | 0.87 |" in content
        assert "| Quick Ratio | 0.83 |" in content

    @pytest.mark.asyncio
    async def test_key_metrics_from_yfinance_fallback(self, full_profile):
        """The yfinance fallback feeds the same table: its rows carry the stable
        names and Yahoo's percent-scaled debt/equity comes out as a ratio.
        """
        from src.data_client.yfinance.financial_source import (
            _get_financial_ratios,
            _get_key_metrics,
        )

        info = {
            "trailingPE": 38.44,
            "trailingPegRatio": 2.71,
            "priceToBook": 57.97,
            "returnOnEquity": 1.55,
            "returnOnAssets": 0.30,
            "profitMargins": 0.243,
            "operatingMargins": 0.319,
            "debtToEquity": 154.0,
            "currentRatio": 0.87,
            "quickRatio": 0.83,
        }
        with patch("src.data_client.yfinance.financial_source.yf.Ticker") as ticker_cls:
            ticker = MagicMock()
            ticker.info = info
            ticker.fast_info = {"marketCap": 3_500_000_000_000}
            ticker_cls.return_value = ticker
            key_metrics = _get_key_metrics("AAPL")
            ratios = _get_financial_ratios("AAPL")

        content = await self._overview_metrics(full_profile, key_metrics, ratios)

        assert "| P/E Ratio | 38.44x |" in content
        assert "| P/B Ratio | 57.97x |" in content
        assert "| PEG Ratio | 2.71 |" in content
        assert "| ROE (Return on Equity) | 155.00% |" in content
        assert "| Net Profit Margin | 24.30% |" in content
        assert "| Debt/Equity Ratio | 1.54 |" in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value, shown", [
        (1.55, "155.00%"),
        (-0.12, "-12.00%"),
        (0.0, "0.00%"),
    ])
    async def test_profitability_rows_show_any_reported_value(
        self, full_profile, value, shown,
    ):
        """Above 100%, negative and exactly 0 all render: 0 is a reported value."""
        content = await self._overview_metrics(
            full_profile,
            [{"returnOnEquityTTM": value, "returnOnAssetsTTM": value}],
            [{"netProfitMarginTTM": value, "operatingProfitMarginTTM": value}],
        )
        for label in (
            "ROE (Return on Equity)",
            "ROA (Return on Assets)",
            "Net Profit Margin",
            "Operating Margin",
        ):
            assert f"| {label} | {shown} |" in content

    @pytest.mark.asyncio
    async def test_zero_placeholders_drop_but_zero_leverage_renders(self, full_profile):
        """FMP writes 0 for a ratio with a zero denominator (AAPL's interest
        coverage, with no reported interest expense). A multiple or coverage
        ratio of 0 is that placeholder and drops; a 0 debt/equity is real.
        """
        content = await self._overview_metrics(
            full_profile,
            [{"evToOperatingCashFlowTTM": 0.0}],
            [{
                "priceToEarningsRatioTTM": 0.0,
                "priceToBookRatioTTM": 0.0,
                "priceToEarningsGrowthRatioTTM": 0.0,
                "debtToEquityRatioTTM": 0.0,
                "currentRatioTTM": 0.0,
                "quickRatioTTM": 0.0,
                "interestCoverageRatioTTM": 0.0,
            }],
        )
        for label in (
            "P/E Ratio", "P/B Ratio", "PEG Ratio", "EV/OCF", "Interest Coverage",
        ):
            assert f"| {label} |" not in content
        assert "| Debt/Equity Ratio | 0.00 |" in content
        assert "| Current Ratio | 0.00 |" in content
        assert "| Quick Ratio | 0.00 |" in content

    @pytest.mark.asyncio
    async def test_negative_multiples_still_render(self, full_profile):
        """A loss-maker's negative P/E is real data, unlike FMP's 0."""
        content = await self._overview_metrics(
            full_profile,
            [{"evToOperatingCashFlowTTM": -12.5}],
            [{"priceToEarningsRatioTTM": -5.8, "interestCoverageRatioTTM": -4.18}],
        )
        assert "| P/E Ratio | -5.80x |" in content
        assert "| EV/OCF | -12.50x |" in content
        assert "| Interest Coverage | -4.18x |" in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pe", ["Infinity", float("inf"), float("nan")])
    async def test_a_non_finite_ratio_drops_only_its_row(self, full_profile, pe):
        """A P/E over zero earnings arriving as Infinity drops its own row; the
        rest of the overview renders instead of an error."""
        content = await self._overview_metrics(
            full_profile, [], [{"priceToEarningsRatioTTM": pe, "debtToEquityRatioTTM": 0.78}],
        )
        assert "| P/E Ratio |" not in content
        assert "| Debt/Equity Ratio | 0.78 |" in content

    @pytest.mark.asyncio
    async def test_missing_ratios_are_omitted(self, full_profile):
        """None or absent fields drop their row."""
        content = await self._overview_metrics(
            full_profile,
            [{
                "evToOperatingCashFlowTTM": None,
                "returnOnEquityTTM": None,
                "returnOnAssetsTTM": 0.30,
            }],
            [{
                "priceToEarningsRatioTTM": None,
                "priceToBookRatioTTM": None,
                "netProfitMarginTTM": None,
                "debtToEquityRatioTTM": None,
                "interestCoverageRatioTTM": None,
            }],
        )
        assert "| ROA (Return on Assets) | 30.00% |" in content
        for label in (
            "P/E Ratio",
            "P/B Ratio",
            "PEG Ratio",
            "EV/OCF",
            "ROE (Return on Equity)",
            "Net Profit Margin",
            "Operating Margin",
            "Debt/Equity Ratio",
            "Current Ratio",
            "Quick Ratio",
            "Interest Coverage",
        ):
            assert f"| {label} |" not in content

    @pytest.mark.asyncio
    async def test_ratios_render_without_key_metrics(self, full_profile):
        """Key metrics failing alone drops only EV/OCF, ROE and ROA."""
        content = await self._overview_metrics(
            full_profile,
            [],
            [{"priceToEarningsRatioTTM": 37.6, "debtToEquityRatioTTM": 0.78}],
        )
        assert "| P/E Ratio | 37.60x |" in content
        assert "| Debt/Equity Ratio | 0.78 |" in content
        for label in ("EV/OCF", "ROE (Return on Equity)", "ROA (Return on Assets)"):
            assert f"| {label} |" not in content

    @pytest.mark.asyncio
    async def test_zero_from_yfinance_fallback_reaches_the_table(self, full_profile):
        """A debt-free company's 0 debt/equity survives the percent conversion
        and renders, as do 0% returns and margins. A 0 PEG drops like FMP's.
        """
        from src.data_client.yfinance.financial_source import (
            _get_financial_ratios,
            _get_key_metrics,
        )

        info = {
            "trailingPE": 12.0,
            "trailingPegRatio": 0.0,
            "returnOnEquity": 0.0,
            "operatingMargins": 0.0,
            "debtToEquity": 0.0,
        }
        with patch("src.data_client.yfinance.financial_source.yf.Ticker") as ticker_cls:
            ticker = MagicMock()
            ticker.info = info
            ticker.fast_info = {"marketCap": 1_000_000_000}
            ticker_cls.return_value = ticker
            key_metrics = _get_key_metrics("AAPL")
            ratios = _get_financial_ratios("AAPL")

        assert ratios[0]["debtToEquityRatioTTM"] == 0.0
        content = await self._overview_metrics(full_profile, key_metrics, ratios)

        assert "| P/E Ratio | 12.00x |" in content
        assert "| PEG Ratio |" not in content
        assert "| ROE (Return on Equity) | 0.00% |" in content
        assert "| Operating Margin | 0.00% |" in content
        assert "| Debt/Equity Ratio | 0.00 |" in content


# ---------------------------------------------------------------------------
# fetch_sector_performance
# ---------------------------------------------------------------------------

def _fund_bars(closes_by_day):
    """Daily bars in the market-data shape, newest last, stamped at 16:00 ET."""
    return [
        {"time": int(datetime.fromisoformat(f"{day}T16:00:00-05:00").timestamp() * 1000),
         "open": close, "high": close, "low": close, "close": close, "volume": 1}
        for day, close in closes_by_day
    ]


def _sector_provider(bars_by_symbol):
    provider = AsyncMock()

    async def daily(symbol, **kwargs):
        return bars_by_symbol.get(symbol, []), "ginlix-data", None

    provider.get_daily_with_source = AsyncMock(side_effect=daily)
    return provider


class TestFetchSectorPerformance:
    @pytest.mark.asyncio
    async def test_each_sector_is_its_spdr_fund_close_to_close(self):
        bars = {
            "XLK": _fund_bars([("2026-10-29", 100.0), ("2026-10-30", 95.11)]),
            "XLRE": _fund_bars([("2026-10-29", 40.0), ("2026-10-30", 40.104)]),
            # A fund whose newest bar is an older session is left out.
            "XLE": _fund_bars([("2026-10-28", 50.0), ("2026-10-29", 51.0)]),
        }
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_sector_provider(bars)):
            content, artifact = await fetch_sector_performance(date="2026-11-01")

        assert artifact["date"] == "2026-10-30"
        assert [(s["sector"], round(s["changePercentage"], 2)) for s in artifact["sectors"]] == [
            ("Real Estate", 0.26),
            ("Information Technology", -4.89),
        ]
        assert "S&P 500 sectors, 2026-10-30\n" in content
        assert "| Information Technology | XLK | -4.89% |" in content
        assert "_No bar for 2026-10-30: " in content and "Energy (XLE)" in content

    @pytest.mark.asyncio
    async def test_the_requested_date_bounds_every_fund_read(self):
        provider = _sector_provider({"XLK": _fund_bars([("2025-01-14", 1.0), ("2025-01-15", 1.1)])})
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider):
            await fetch_sector_performance(date="2025-01-15")
        assert {c.kwargs["to_date"] for c in provider.get_daily_with_source.await_args_list} == {"2025-01-15"}
        assert provider.get_daily_with_source.await_count == 11

    @pytest.mark.asyncio
    async def test_no_fund_data_says_so(self):
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_sector_provider({})):
            content, artifact = await fetch_sector_performance(date="2025-01-15")
        assert "No sector fund data" in content
        assert artifact["sectors"] == []

    @pytest.mark.asyncio
    async def test_every_fund_read_failing_is_a_source_failure(self):
        provider = AsyncMock()
        provider.get_daily_with_source = AsyncMock(side_effect=RuntimeError("upstream 503"))
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider), \
             patch(f"{_MKT_MOD}._mark_failure") as marked:
            content, artifact = await fetch_sector_performance(date="2025-01-15")
        assert "11 of 11 fund reads failed (upstream 503)" in content
        assert artifact["sectors"] == [] and marked.call_count == 1

    @pytest.mark.asyncio
    async def test_failed_reads_beside_empty_ones_are_still_a_failure(self):
        async def daily(symbol, **kwargs):
            if symbol == "XLK":
                return [], "ginlix-data", None
            raise RuntimeError("upstream 503")

        provider = AsyncMock()
        provider.get_daily_with_source = AsyncMock(side_effect=daily)
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider), \
             patch(f"{_MKT_MOD}._mark_failure") as marked:
            content, _ = await fetch_sector_performance(date="2025-01-15")
        assert "10 of 11 fund reads failed (upstream 503)" in content and marked.call_count == 1

    @pytest.mark.asyncio
    async def test_a_failed_fund_read_is_not_called_a_missing_bar(self):
        bars = {"XLK": _fund_bars([("2026-10-29", 100.0), ("2026-10-30", 95.11)])}

        async def daily(symbol, **kwargs):
            if symbol == "XLE":
                raise RuntimeError("upstream 503")
            return bars.get(symbol, []), "ginlix-data", None

        provider = AsyncMock()
        provider.get_daily_with_source = AsyncMock(side_effect=daily)
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_sector_performance(date="2026-10-30")
        assert "_Read failed: Energy (XLE)_" in content
        no_bar = next(line for line in content.splitlines() if line.startswith("_No bar for"))
        assert "XLE" not in no_bar

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("hour", "forming"), [(11, True), (17, False)], ids=["session", "after-hours"])
    async def test_todays_session_reads_in_progress_until_the_regular_close(self, hour, forming):
        bars = {"XLK": _fund_bars([("2026-11-03", 100.0), ("2026-11-04", 101.0)])}
        midday = datetime(2026, 11, 4, hour, 0, tzinfo=ZoneInfo("America/New_York"))

        class _Midday(datetime):
            @classmethod
            def now(cls, tz=None):
                return midday.astimezone(tz)

        with patch(f"{_MKT_MOD}.datetime", _Midday), \
             patch("src.data_client.normalize.datetime", _Midday), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_sector_provider(bars)):
            content, artifact = await fetch_sector_performance()
        assert artifact["date"] == "2026-11-04"
        assert ("S&P 500 sectors, 2026-11-04 (in progress)" in content) is forming
        assert artifact["in_progress"] is forming


# ---------------------------------------------------------------------------
# fetch_market_overview
# ---------------------------------------------------------------------------

def _no_snapshot_provider():
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=[])
    return provider


def _snapshot_result(date="2025-02-14"):
    return (
        "IDX-CONTENT",
        {"type": "market_indices", "indices": {"^GSPC": {"name": "S&P 500"}}},
        date,
    )


class TestFetchMarketOverview:
    @pytest.mark.asyncio
    async def test_us_includes_sectors(self):
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result())), \
             patch(f"{_MKT_MOD}.fetch_sector_performance",
                   AsyncMock(return_value=("SECTOR-CONTENT", {"type": "sector_performance", "sectors": []}))) as mock_sec, \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="us")
        assert "IDX-CONTENT" in content
        assert "SECTOR-CONTENT" in content
        assert artifact["type"] == "market_overview"
        assert artifact["region"] == "us"
        assert artifact["date"] == "2025-02-14"
        # Sector half aligned to the resolved trading day
        assert mock_sec.await_args.kwargs["date"] == "2025-02-14"

    @pytest.mark.asyncio
    async def test_live_index_stamp_when_open(self):
        idx_snaps = [{"symbol": "^GSPC", "price": 6120.5, "change_percent": 0.42,
                      "volume": None, "last_trade_price": None,
                      "market_status": "open", "tier": "realtime"}]
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=idx_snaps)
        # An index's clock reads closed outside 09:30-16:00 ET, so the
        # measurement is pinned inside the session the patched header claims.
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result())), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider), \
             patch("src.tools.market_data.quote_format.get_market_session",
                   return_value=("REGULAR_HOURS", _FIXED_ET)), \
             patch("src.data_client.freshness._now", side_effect=lambda now: now or _FIXED_ET):
            content, _ = await fetch_market_overview(region="us")
        assert content.startswith("[Live: ^GSPC 6,120.50")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("markets", "raised", "indices", "outcome", "error_class"),
        [
            pytest.param(
                {"us"}, RuntimeError("upstream 503"), None,
                "unavailable", "MarketNotCovered", id="market-not-covered",
            ),
            pytest.param(
                {"all"}, RuntimeError("upstream 503"), None,
                "error", "RuntimeError", id="source-failed",
            ),
            pytest.param(
                {"all"}, InvalidSymbol("Symbol 'A B' contains ' '"), None,
                "ok", None, id="invalid-symbol",
            ),
            pytest.param(
                {"us"}, InvalidSymbol("Symbol 'A B' contains ' '"), ["A B", "^N225"],
                "unavailable", "MarketNotCovered", id="malformed-then-uncovered",
            ),
            pytest.param(
                {"all"}, None, None,
                "unavailable", "no_data", id="no-bars",
            ),
        ],
    )
    async def test_no_index_coming_back_marks_why(
        self, markets, raised, indices, outcome, error_class
    ):
        """Each index's error is caught to keep the rest of the basket, so with
        none back the snapshot itself marks the run, by the same rule a single
        symbol's fetch does, with the worst of its errors."""
        from src.data_client.market_data_provider import (
            MarketDataProvider,
            ProviderEntry,
        )
        from src.observability.langsmith_tracer import NonRetainingTracer
        from src.tools.market_data.tool import get_market_overview

        source = AsyncMock()
        source.get_daily = AsyncMock(side_effect=raised, return_value=[])
        provider = MarketDataProvider([ProviderEntry("ginlix", source, markets)])
        client = _RecordingClient()

        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider):
            msg = await get_market_overview.ainvoke(
                {
                    "type": "tool_call",
                    "name": "get_market_overview",
                    # A past day, so no live snapshot stands in for the bars.
                    "args": {
                        "region": "jp",
                        "date": "2025-02-14",
                        **({"indices": indices} if indices else {}),
                    },
                    "id": "tc-1",
                },
                {"callbacks": [NonRetainingTracer(project_name="proj", client=client)]},
            )

        assert "No index data available" in msg.content
        (update,) = [u for u in client.updated if u["run_type"] == "tool"]
        metadata = update["extra"]["metadata"]
        assert metadata["tool_outcome"] == outcome
        assert metadata.get("error_class") == error_class
        assert (update["error"] is not None) == (outcome == "error")

    @pytest.mark.asyncio
    async def test_historical_date_skips_live_stamp(self):
        provider = _no_snapshot_provider()
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2025-02-14"))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider):
            await fetch_market_overview(region="us", date="2025-02-14")
        provider.get_snapshots.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_us_degrades_sectors_gracefully(self):
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result())) as mock_snap, \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="hk")
        assert "IDX-CONTENT" in content
        assert "Sector breakdown unavailable" in content
        assert "sectors" not in artifact
        # HK basket passed through
        assert mock_snap.await_args.args[0] == ["^HSI", "^HSCE"]

    @pytest.mark.asyncio
    async def test_unknown_region(self):
        content, artifact = await fetch_market_overview(region="mars")
        assert "Unknown region" in content
        assert "us" in content  # lists supported regions

    @pytest.mark.asyncio
    async def test_invalid_date_is_graceful(self):
        content, artifact = await fetch_market_overview(region="us", date="last tuesday")
        assert "Invalid date" in content
        assert artifact["type"] == "market_overview"
        assert "error" in artifact

    @pytest.mark.asyncio
    async def test_explicit_indices_override_basket(self):
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result())) as mock_snap, \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("S", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            await fetch_market_overview(region="us", indices=["^VIX"])
        assert mock_snap.await_args.args[0] == ["^VIX"]

    @pytest.mark.asyncio
    async def test_fallback_reminder_when_requested_day_has_no_bar(self):
        # Request a Sunday; snapshot resolves to the prior Friday.
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2025-02-14"))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="us", date="2025-02-16")
        assert "No daily bar for 2025-02-16** (weekend / holiday)" in content
        assert "**2025-02-14**" in content
        assert artifact["date"] == "2025-02-14"
        assert artifact["requested_date"] == "2025-02-16"

    @pytest.mark.asyncio
    async def test_before_the_open_the_last_session_is_no_fallback(self):
        """At 05:48 Shanghai on a trading day the newest session that has
        opened is yesterday's, so showing it is no holiday fallback. A caller
        who names today is still told that day has no bar yet."""
        before_open = datetime(2026, 11, 4, 5, 48, tzinfo=ZoneInfo("Asia/Shanghai"))

        class _BeforeOpen(datetime):
            @classmethod
            def now(cls, tz=None):
                return before_open.astimezone(tz)

        with patch(f"{_MKT_MOD}.datetime", _BeforeOpen), \
             patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2026-11-03"))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="cn")
            named, named_artifact = await fetch_market_overview(region="cn", date="2026-11-04")
        assert "No daily bar" not in content
        assert "requested_date" not in artifact
        assert "No daily bar for 2026-11-04** (the session has not opened yet)" in named
        assert named_artifact["requested_date"] == "2026-11-04"

    @pytest.mark.asyncio
    async def test_an_open_session_with_no_bar_yet_is_no_holiday(self):
        """At 09:38 ET the delayed index feed has not printed the new session."""
        after_open = datetime(2026, 11, 5, 9, 38, tzinfo=ZoneInfo("America/New_York"))

        class _AfterOpen(datetime):
            @classmethod
            def now(cls, tz=None):
                return after_open.astimezone(tz)

        with patch(f"{_MKT_MOD}.datetime", _AfterOpen), \
             patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2026-11-04"))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="us")
        assert "No daily bar for 2026-11-05** (the session has opened but has no published bar yet)" in content
        assert "holiday" not in content
        assert artifact["requested_date"] == "2026-11-05"

    @pytest.mark.asyncio
    async def test_today_named_on_a_weekend_is_no_session_to_come(self):
        """An agent passing today's date on a Saturday is told it is a weekend,
        not that a session has yet to open."""
        saturday = datetime(2026, 11, 7, 11, 0, tzinfo=ZoneInfo("America/New_York"))

        class _Saturday(datetime):
            @classmethod
            def now(cls, tz=None):
                return saturday.astimezone(tz)

        with patch(f"{_MKT_MOD}.datetime", _Saturday), \
             patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2026-11-06"))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, _ = await fetch_market_overview(region="us", date="2026-11-07")
        assert "No daily bar for 2026-11-07** (weekend / holiday)" in content

    @pytest.mark.asyncio
    async def test_a_past_session_with_no_bar_is_missing_not_unpublished(self):
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2025-02-12"))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, _ = await fetch_market_overview(region="us", date="2025-02-13")
        assert "No daily bar for 2025-02-13** (missing from the source)" in content

    @pytest.mark.asyncio
    async def test_an_empty_basket_reads_sectors_up_to_its_own_session(self):
        from src.data_client.instrument_clock import clock_for

        sectors = AsyncMock(return_value=("SEC", {}))
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot", AsyncMock(return_value=("IDX", [], None))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", sectors), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            await fetch_market_overview(region="us", indices=["^N225"])
        opened = clock_for("^N225", True).expected_latest_daily_date(datetime.now(timezone.utc))
        assert sectors.await_args.kwargs["date"] == opened

    @pytest.mark.asyncio
    async def test_a_sectors_only_overview_is_dated_by_the_funds_session(self):
        """With no index row, the sector funds' session dates the overview and its notice."""
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot", AsyncMock(return_value=("IDX", [], None))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance",
                   AsyncMock(return_value=("SEC", {"date": "2025-02-14"}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="us", date="2025-02-16")
        assert "Market Overview — US (2025-02-14)" in content
        assert "No daily bar for 2025-02-16** (weekend / holiday)" in content
        assert (artifact["date"], artifact["requested_date"]) == ("2025-02-14", "2025-02-16")

    @pytest.mark.asyncio
    async def test_a_bar_dated_past_the_target_is_no_fallback(self):
        """A publisher stamping a bar a day ahead must not read as a missing session."""
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2025-02-18"))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="us", date="2025-02-14")
        assert "No daily bar" not in content
        assert "requested_date" not in artifact

    @pytest.mark.asyncio
    async def test_the_default_date_is_the_venue_day_not_utc(self):
        """At 21:00 ET the UTC date is already tomorrow; the US default stays
        on today and does not warn of a bar that cannot exist yet."""
        evening = datetime(2025, 2, 14, 21, 0, tzinfo=ZoneInfo("America/New_York"))

        class _Evening(datetime):
            @classmethod
            def now(cls, tz=None):
                return evening.astimezone(tz)

        with patch(f"{_MKT_MOD}.datetime", _Evening), \
             patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2025-02-14"))) as mock_snap, \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, _ = await fetch_market_overview(region="us")
        assert mock_snap.await_args.args[1] == "2025-02-14"
        assert "No daily bar" not in content

    @pytest.mark.asyncio
    async def test_no_reminder_when_date_matches(self):
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=_snapshot_result("2025-02-14"))), \
             patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, _ = await fetch_market_overview(region="us", date="2025-02-14")
        assert "No daily bar" not in content

    @pytest.mark.asyncio
    async def test_indices_failure_still_returns_sectors(self):
        empty = ("No index data available for the requested date.",
                 {"type": "market_indices", "indices": {}}, None)
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(return_value=empty)), \
             patch(f"{_MKT_MOD}.fetch_sector_performance",
                   AsyncMock(return_value=("SECTOR-CONTENT", {"type": "sector_performance"}))) as mock_sec, \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="us", date="2025-02-14")
        assert "No index data available" in content
        assert "SECTOR-CONTENT" in content
        # Unresolved snapshot: the sector provider resolves the requested date itself
        assert mock_sec.await_args.kwargs["date"] == "2025-02-14"

    @pytest.mark.asyncio
    async def test_caret_free_snapshot_row_reaches_its_caret_basket_entry(self):
        # Providers answer ^GSPC as GSPC; that row must still supply the forming bar.
        # Its previous close is the series' last close, so it is the next session.
        bars = _make_provider_bars(10)
        last_close = max(bars, key=lambda b: b["time"])["close"]
        provider = _make_fake_market_provider(daily_bars=bars)
        provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "GSPC", "price": 6120.5, "previous_close": last_close,
            "source": "ginlix-data", "tier": "realtime",
        }])
        with patch(f"{_MKT_MOD}.fetch_sector_performance", AsyncMock(return_value=("SEC", {}))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider):
            _, artifact = await fetch_market_overview(region="us", indices=["^GSPC"])
        entry = artifact["indices"]["indices"]["^GSPC"]
        assert entry["ohlcv"][-1]["close"] == 6120.5
        assert entry["source"] == "ginlix-data"

    @pytest.mark.asyncio
    async def test_snapshot_error_returns_error_artifact(self):
        with patch(f"{_MKT_MOD}._fetch_index_day_snapshot",
                   AsyncMock(side_effect=RuntimeError("provider down"))), \
             patch(f"{_MKT_MOD}.get_market_data_provider", return_value=_no_snapshot_provider()):
            content, artifact = await fetch_market_overview(region="us")
        assert "Error retrieving market overview" in content
        assert artifact["error"] == "provider down"


# ---------------------------------------------------------------------------
# _fetch_index_day_snapshot
# ---------------------------------------------------------------------------

class TestFetchIndexDaySnapshot:
    @pytest.mark.asyncio
    async def test_snapshot_shape_and_day_change(self):
        from src.tools.market_data.market_overview import _fetch_index_day_snapshot

        bars = _make_provider_bars(30)
        provider = _make_fake_market_provider(daily_bars=bars)
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider):
            content, artifact, snapshot_date = await _fetch_index_day_snapshot(
                ["^GSPC"], "2025-01-30"
            )

        provider.get_daily_with_source.assert_called_once()
        assert provider.get_daily_with_source.call_args.kwargs["is_index"] is True
        assert provider.get_daily_with_source.call_args.kwargs["to_date"] == "2025-01-30"

        assert snapshot_date == "2025-01-30"
        assert "| Index | Close | Day Change | Volume | Date |" in content
        assert "S&P 500 (^GSPC)" in content
        # Index levels are unitless points — no currency prefix anywhere.
        assert "$" not in content

        entry = artifact["indices"]["^GSPC"]
        assert set(entry) == {
            "name", "ohlcv", "chart_ohlcv", "chart_interval", "stats",
            "source", "tier", "freshness",
        }
        assert entry["source"] == "fmp"
        # A past date is a closed window, so there is nothing to label stale.
        assert entry["freshness"] is None
        assert entry["chart_interval"] == "daily"
        # ohlcv ascending for the chart
        dates = [b["date"] for b in entry["ohlcv"]]
        assert dates == sorted(dates)
        # period_change_pct is the DAY move (last close vs prior close), not the
        # window move — take the final two bars chronologically (ohlcv is
        # ascending), never by close value.
        prior, last = entry["ohlcv"][-2:]
        expected_day_pct = (last["close"] - prior["close"]) / prior["close"] * 100
        assert entry["stats"]["period_change_pct"] == pytest.approx(expected_day_pct)

    @pytest.mark.asyncio
    async def test_today_in_the_venue_stays_open_after_the_utc_date_rolls(self):
        from src.tools.market_data.market_overview import _fetch_index_day_snapshot

        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(30))
        # 21:00 ET on 07-01 is already 07-02 in UTC.
        venue_now = datetime(2026, 7, 1, 21, 0)
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider), \
             patch(f"{_MKT_MOD}.venue_local_time", return_value=venue_now):
            _, artifact, _ = await _fetch_index_day_snapshot(["^GSPC"], "2026-07-01")

        assert artifact["indices"]["^GSPC"]["freshness"] is not None

    @pytest.mark.asyncio
    async def test_running_session_row_is_marked_in_progress(self):
        from src.tools.market_data.market_overview import _fetch_index_day_snapshot

        # Bars open 2025-01-01..30; the 01-30 session is still running.
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(30))
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider), \
                _frozen_clock(_ET.localize(datetime(2025, 1, 30, 11, 0))):
            content, _, _ = await _fetch_index_day_snapshot(["^GSPC"], "2025-01-30")

        assert "| 2025-01-30 (in progress) |" in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("hour, marked", [(11, True), (17, False)])
    async def test_a_snapshot_row_is_in_progress_only_while_its_session_runs(self, hour, marked):
        from src.tools.market_data.market_overview import _fetch_index_day_snapshot

        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(30))
        row = {
            "date": "2025-01-31", "open": 6100.0, "high": 6130.0, "low": 6090.0, "close": 6120.5,
            "volume": None, "change": 20.5, "changePercent": 0.34, "symbol": "^GSPC", "forming": True,
        }
        snap = {"symbol": "GSPC", "price": 6120.5, "source": "ginlix-data", "tier": "delayed_15m"}
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider), \
                patch(f"{_MKT_MOD}._forming_bar", return_value=row), \
                _frozen_clock(_ET.localize(datetime(2025, 1, 31, hour, 0))):
            content, _, _ = await _fetch_index_day_snapshot(
                ["^GSPC"], "2025-01-31", snaps={"GSPC": snap}
            )

        assert "2025-01-31" in content
        assert ("2025-01-31 (in progress)" in content) is marked

    @pytest.mark.asyncio
    async def test_partial_failure_lists_missing_symbols(self):
        from src.tools.market_data.market_overview import _fetch_index_day_snapshot

        bars = _make_provider_bars(10)
        provider = AsyncMock()
        provider.get_daily_with_source = AsyncMock(
            side_effect=[(bars, "fmp", False), RuntimeError("boom")]
        )
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider):
            content, artifact, snapshot_date = await _fetch_index_day_snapshot(
                ["^GSPC", "^IXIC"], "2025-01-30"
            )
        assert "^GSPC" in artifact["indices"]
        assert "^IXIC" not in artifact["indices"]
        assert "_No data: ^IXIC_" in content
        assert snapshot_date is not None

    @pytest.mark.asyncio
    async def test_all_indices_empty(self):
        from src.tools.market_data.market_overview import _fetch_index_day_snapshot

        provider = _make_fake_market_provider(daily_bars=[])
        with patch(f"{_MKT_MOD}.get_market_data_provider", return_value=provider):
            content, artifact, snapshot_date = await _fetch_index_day_snapshot(
                ["^GSPC", "^IXIC"], "2025-01-30"
            )
        assert "No index data available" in content
        assert artifact == {"type": "market_indices", "indices": {}}
        assert snapshot_date is None

    def test_index_day_row_volume_uses_fmt_count(self):
        # Volume renders via fmt_count (B/M/T), matching the sibling price table —
        # not the raw grouped integer; a falsy volume is an em-dash.
        from src.tools.market_data.market_overview import _format_index_day_row

        row = _format_index_day_row(
            "^GSPC",
            {"close": 6120.5, "change": 12.0, "changePercent": 0.2,
             "volume": 3_900_000, "date": "2025-01-30"},
        )
        assert "3.90M" in row
        assert "3,900,000" not in row
        zero = _format_index_day_row(
            "^GSPC", {"close": 6120.5, "volume": 0, "date": "2025-01-30"}
        )
        assert "| — |" in zero


# ---------------------------------------------------------------------------
# fetch_stock_screener
# ---------------------------------------------------------------------------

class TestFetchStockScreener:
    def _make_screener_provider(self, results):
        financial = _make_fake_financial_source(screener_results=results)
        return _make_fake_financial_provider(financial=financial), financial

    @pytest.mark.asyncio
    async def test_with_filters(self):
        """Filters should be passed through and results formatted."""
        results = [
            {
                "symbol": "AAPL",
                "companyName": "Apple Inc.",
                "price": 235.50,
                "marketCap": 3_500_000_000_000,
                "sector": "Technology",
                "beta": 1.24,
                "volume": 55_000_000,
                "change": 2.30,
            },
            {
                "symbol": "MSFT",
                "companyName": "Microsoft Corporation",
                "price": 420.0,
                "marketCap": 3_100_000_000_000,
                "sector": "Technology",
                "beta": 0.93,
                "volume": 25_000_000,
                "change": -1.50,
            },
        ]
        provider, financial = self._make_screener_provider(results)

        with patch(f"{_SCREEN_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_stock_screener(
                sector="Technology",
                market_cap_more_than=1_000_000_000_000,
            )

        assert "AAPL" in content
        assert "MSFT" in content
        assert "Stock Screener Results" in content
        assert "2 stocks" in content
        assert artifact["type"] == "stock_screener"
        assert artifact["count"] == 2
        # Verify params were passed to financial source
        financial.screen_stocks.assert_called_once()
        call_kwargs = financial.screen_stocks.call_args[1]
        assert call_kwargs["sector"] == "Technology"
        assert call_kwargs["marketCapMoreThan"] == 1_000_000_000_000

    @pytest.mark.asyncio
    async def test_an_overview_sector_name_screens_under_the_screeners_name(self):
        # The market overview names sectors by GICS ("Health Care"); the
        # screener matches only its own vocabulary ("Healthcare").
        provider, financial = self._make_screener_provider([{"symbol": "UNH", "sector": "Healthcare"}])
        with patch(f"{_SCREEN_MOD}.get_financial_data_provider", return_value=provider):
            await fetch_stock_screener(sector="Health Care")
        assert financial.screen_stocks.await_args.kwargs["sector"] == "Healthcare"

    @pytest.mark.asyncio
    async def test_empty_results(self):
        """No matches should return appropriate message."""
        provider, _ = self._make_screener_provider([])

        with patch(f"{_SCREEN_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_stock_screener(sector="Nonexistent")

        assert "No stocks matched" in content
        assert artifact["count"] == 0

    @pytest.mark.asyncio
    async def test_all_filter_params_passed(self):
        """All filter params should be converted to camelCase and passed."""
        provider, financial = self._make_screener_provider([])

        with patch(f"{_SCREEN_MOD}.get_financial_data_provider", return_value=provider):
            await fetch_stock_screener(
                market_cap_more_than=1e9,
                price_more_than=10.0,
                volume_more_than=1e6,
                beta_more_than=0.5,
                dividend_more_than=2.0,
                is_etf=False,
                is_actively_trading=True,
                exchange="NASDAQ",
                country="US",
                limit=25,
            )

        call_kwargs = financial.screen_stocks.call_args[1]
        assert call_kwargs["marketCapMoreThan"] == 1e9
        assert call_kwargs["priceMoreThan"] == 10.0
        assert call_kwargs["volumeMoreThan"] == 1e6
        assert call_kwargs["betaMoreThan"] == 0.5
        assert call_kwargs["dividendMoreThan"] == 2.0
        assert call_kwargs["isEtf"] is False
        assert call_kwargs["isActivelyTrading"] is True
        assert call_kwargs["exchange"] == "NASDAQ"
        assert call_kwargs["country"] == "US"
        assert call_kwargs["limit"] == 25

    @pytest.mark.asyncio
    async def test_provider_exception_returns_error(self):
        """Provider exception should return error content."""
        with patch(f"{_SCREEN_MOD}.get_financial_data_provider", side_effect=Exception("Connection failed")):
            content, artifact = await fetch_stock_screener()

        assert "Error" in content
        assert artifact["count"] == 0


# ---------------------------------------------------------------------------
# Helpers for intel-based tools
# ---------------------------------------------------------------------------

def _make_fake_intel_source(
    *,
    options_chain=None,
    options_ohlcv=None,
    short_interest=None,
    short_volume=None,
    float_shares=None,
    movers=None,
):
    """Build a mock MarketIntelSource."""
    src = AsyncMock()
    src.get_options_chain = AsyncMock(return_value=options_chain or {"results": []})
    src.get_options_ohlcv = AsyncMock(return_value=options_ohlcv or [])
    src.get_short_interest = AsyncMock(return_value=short_interest or [])
    src.get_short_volume = AsyncMock(return_value=short_volume or [])
    src.get_float_shares = AsyncMock(return_value=float_shares or {})
    src.get_movers = AsyncMock(return_value=movers or [])
    return src


# ---------------------------------------------------------------------------
# fetch_options_chain
# ---------------------------------------------------------------------------

class TestFetchOptionsChain:
    @pytest.mark.asyncio
    async def test_with_filters(self):
        """Filters should be passed and results formatted as table."""
        chain_data = {
            "results": [
                {
                    "ticker": "O:AAPL250117C00200000",
                    "contract_type": "call",
                    "strike_price": 200.0,
                    "expiration_date": "2025-01-17",
                    "exercise_style": "american",
                },
                {
                    "ticker": "O:AAPL250117P00180000",
                    "contract_type": "put",
                    "strike_price": 180.0,
                    "expiration_date": "2025-01-17",
                    "exercise_style": "american",
                },
            ],
            "next_cursor": "abc123",
        }
        intel = _make_fake_intel_source(options_chain=chain_data)
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_options_chain(
                "AAPL", contract_type="call", strike_min=150.0, limit=10
            )

        assert "Options Chain: AAPL" in content
        assert "contracts" in content
        assert "O:AAPL250117C00200000" in content
        assert "$200.00" in content
        assert artifact["type"] == "options_chain"
        assert len(artifact["results"]) >= 2

        # Verify filters passed (implementation may paginate, so check first call)
        assert intel.get_options_chain.await_count >= 1
        first_call = intel.get_options_chain.call_args_list[0]
        assert first_call[0][0] == "AAPL"
        assert first_call[1]["contract_type"] == "call"
        assert first_call[1]["strike_price_gte"] == 150.0

    @pytest.mark.asyncio
    async def test_artifact_keeps_each_contracts_session(self):
        """The card prints close, change and volume, so the artifact keeps the
        snapshot session the text table is built from."""
        chain = {"results": [
            {"ticker": "O:ZZZZ250117C00010000", "contract_type": "call", "strike_price": 10.0, "expiration_date": "2025-01-17"},
            {"ticker": "O:ZZZZ250117C00011000", "contract_type": "call", "strike_price": 11.0, "expiration_date": "2025-01-17"},
        ]}
        intel = _make_fake_intel_source(options_chain=chain)
        intel.get_options_snapshot = AsyncMock(return_value=[
            {"ticker": "O:ZZZZ250117C00010000", "market_status": "closed",
             "session": {"close": 1.25, "change_percent": 4.0, "volume": 30.0}},
        ])
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            _, artifact = await fetch_options_chain("ZZZZ", contract_type="call")

        priced, unpriced = artifact["results"]
        assert priced["session"] == {"close": 1.25, "change_percent": 4.0, "volume": 30.0}
        assert unpriced["session"] is None
        assert artifact["market_status"] == "closed"
        assert artifact["contract_type"] == "call"
        assert artifact["currency"] == "USD"

    @pytest.mark.asyncio
    async def test_table_dates_a_stale_close_and_states_the_delay(self):
        """A contract not traded this session prints its last day, marked stale,
        beside its IV, delta and open interest, under the feed's delay."""
        chain = {"results": [
            {"ticker": "O:ZZZZ261120C00125000", "contract_type": "call", "strike_price": 125.0, "expiration_date": "2026-11-20"},
        ]}
        intel = _make_fake_intel_source(options_chain=chain)
        intel.get_options_snapshot = AsyncMock(return_value=[
            {"ticker": "O:ZZZZ261120C00125000", "market_status": "open",
             # 2026-10-23 10:54:03 New York, weeks before the read.
             "session": {"close": 222.0, "change_percent": 0.0, "volume": 1.0,
                         "timeframe": "DELAYED", "last_updated": 1792767243597000000},
             "greeks": {"delta": 0.9819}, "implied_volatility": 0.394, "open_interest": 11599,
             "underlying_asset": {"ticker": "ZZZZ", "price": 343.32, "timeframe": "REAL-TIME"}},
        ])
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_options_chain("ZZZZ")

        assert "| Market: open | Underlying: $343.32 (real-time)" in content
        assert "**Prices:** 15 minutes delayed." in content
        assert (
            "| $222.00 | +0.00% | 1 | 2026-10-23 10:54 (stale) | 39.4% | 0.98 | 11,599 |"
            in content
        )
        assert artifact["results"][0]["session"]["stale"] is True
        assert artifact["underlying"] == "ZZZZ"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("underlying, price, shown", [("ZZZZ", 0.8012, "$0.8012"), ("^GSPC", 0.5, "0.50")])
    async def test_the_underlying_is_a_share_at_its_own_places_and_an_index_at_two(self, underlying, price, shown):
        chain = {"results": [
            {"ticker": "O:ZZZZ261120C00001000", "contract_type": "call", "strike_price": 1.0, "expiration_date": "2026-11-20"},
        ]}
        intel = _make_fake_intel_source(options_chain=chain)
        intel.get_options_snapshot = AsyncMock(return_value=[
            {"ticker": "O:ZZZZ261120C00001000", "market_status": "open",
             "underlying_asset": {"ticker": underlying, "price": price}},
        ])
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_options_chain(underlying)

        assert f"| Underlying: {shown}\n" in content

    @pytest.mark.asyncio
    async def test_a_real_time_feed_still_explains_the_stale_mark(self):
        chain = {"results": [
            {"ticker": "O:ZZZZ261120C00125000", "contract_type": "call", "strike_price": 125.0, "expiration_date": "2026-11-20"},
        ]}
        intel = _make_fake_intel_source(options_chain=chain)
        intel.get_options_snapshot = AsyncMock(return_value=[
            {"ticker": "O:ZZZZ261120C00125000",
             "session": {"close": 222.0, "timeframe": "REAL-TIME", "last_updated": 1792767243597000000}},
        ])
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_options_chain("ZZZZ")

        assert "**Prices:** real-time. Updated is when the feed showed the last print (New York time); a stale" in content
        assert "2026-10-23 10:54 (stale)" in content

    @pytest.mark.asyncio
    async def test_a_feed_that_names_no_timeframe_states_no_delay(self):
        chain = {"results": [
            {"ticker": "O:ZZZZ261120C00125000", "contract_type": "call", "strike_price": 125.0, "expiration_date": "2026-11-20"},
        ]}
        intel = _make_fake_intel_source(options_chain=chain)
        intel.get_options_snapshot = AsyncMock(return_value=[
            {"ticker": "O:ZZZZ261120C00125000", "session": {"close": 2.0}},
        ])
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_options_chain("ZZZZ")

        assert "**Prices:**" not in content
        assert "| $2.00 | — | — | — | — | — | — |" in content

    @pytest.mark.asyncio
    async def test_empty_results(self):
        intel = _make_fake_intel_source(options_chain={"results": []})
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_options_chain("FAKE")

        assert "No contracts found" in content
        assert artifact["results"] == []

    @pytest.mark.asyncio
    async def test_no_intel_source(self):
        """Missing intel source should return unavailable message."""
        provider = _make_fake_financial_provider(intel=None)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_options_chain("AAPL")

        assert "not available" in content
        assert artifact["results"] == []

    @pytest.mark.asyncio
    async def test_boundary_normalizes_underlying_before_provider(self):
        """A non-canonical underlying reaches the provider canonicalized (FIX 3).

        ``zzzz.us`` is a neutral placeholder: lowercase + ``.US`` suffix, which
        the protocol boundary folds to bare ``ZZZZ`` before the provider call.
        """
        intel = _make_fake_intel_source(options_chain={"results": []})
        provider = _make_fake_financial_provider(intel=intel)
        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            await fetch_options_chain("zzzz.us")

        assert intel.get_options_chain.await_count >= 1
        assert intel.get_options_chain.call_args_list[0][0][0] == "ZZZZ"


# ---------------------------------------------------------------------------
# fetch_market_movers
# ---------------------------------------------------------------------------

def _mover(symbol, *, price, change_percent, traded_value=None, traded_at=None):
    """A mover as a MarketIntelSource hands it over."""
    return {"symbol": symbol, "price": price, "change": None,
            "change_percent": change_percent, "volume": None,
            "traded_value": traded_value, "traded_at": traded_at}


class TestFetchMarketMovers:
    @pytest.mark.asyncio
    async def test_gainers(self):
        movers = [
            _mover("NVDA", price=950.0, change_percent=8.5),
            _mover("AMD", price=180.0, change_percent=5.2),
        ]
        intel = _make_fake_intel_source(movers=movers)
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers(direction="gainers")

        assert "Market Gainers" in content
        assert "2 stocks" in content
        assert "NVDA" in content
        assert "+8.50%" in content
        assert artifact["type"] == "market_movers"
        assert artifact["direction"] == "gainers"
        assert len(artifact["results"]) == 2

    @pytest.mark.asyncio
    async def test_a_name_column_shows_only_when_a_name_is_known(self):
        movers = [_mover("0700.HK", price=401.6, change_percent=5.0), _mover("NVDA", price=950.0, change_percent=8.5)]
        provider = _make_fake_financial_provider(intel=_make_fake_intel_source(movers=movers))

        async def name_tencent(rows):
            for row in rows:
                row["name"] = "Tencent" if row["symbol"] == "0700.HK" else None

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            with patch(f"{_QUOTES_MOD}.fill_quote_names", new=AsyncMock()):
                unnamed, _ = await fetch_market_movers(direction="gainers")
            with patch(f"{_QUOTES_MOD}.fill_quote_names", new=name_tencent):
                named, _ = await fetch_market_movers(direction="gainers")

        assert "| # | Symbol | Price | Change% | Last Trade (ET) |" in unnamed
        assert "| NVDA | $950.00 |" in unnamed
        assert "| # | Symbol | Name | Price | Change% | Last Trade (ET) |" in named
        assert "| 0700.HK | Tencent |" in named
        assert "| NVDA | N/A |" in named

    @pytest.mark.asyncio
    async def test_vendor_row_is_priced_at_its_last_trade(self):
        """The vendor's row has no price or name field: the last trade is the
        price its change was measured at, the day bar's close stands in."""
        from src.data_client.ginlix_data.market_intel_source import GinlixMarketIntelSource

        movers = [
            {"ticker": "ZZZZ", "change": 1.5, "change_percent": 50.0,
             "day": {"close": 4.4, "volume": 1200.0}, "last_trade": {"price": 4.5}},
            {"ticker": "YYYY", "change": 0.5, "change_percent": 25.0, "day": {"close": 2.5, "volume": 900.0}},
        ]
        # The vendor row reaches the tool through the source that shapes it.
        client = AsyncMock()
        client.get_movers = AsyncMock(return_value=movers)
        provider = _make_fake_financial_provider(intel=GinlixMarketIntelSource(client))

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers(direction="gainers")

        assert "$4.50" in content and "$2.50" in content
        first, second = artifact["results"]
        assert first == {"symbol": "ZZZZ", "name": None, "price": 4.5, "change": 1.5,
                         "change_percent": 50.0, "volume": 1200.0, "traded_value": None,
                         "traded_at": None, "currency": "USD"}
        assert second["price"] == 2.5

    @pytest.mark.asyncio
    async def test_a_mover_with_little_money_traded_is_excluded_and_named(self):
        movers = [
            _mover("WRNTW", price=0.08, change_percent=150.0, traded_value=900.0),
            _mover("BIGG", price=12.0, change_percent=40.0, traded_value=5_000_000.0),
            _mover("UNKN", price=3.0, change_percent=30.0),
        ]
        provider = _make_fake_financial_provider(intel=_make_fake_intel_source(movers=movers))

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers(direction="gainers")

        assert [r["symbol"] for r in artifact["results"]] == ["BIGG", "UNKN"]
        assert "**Excluded (under $100,000 traded):** WRNTW" in content
        assert "| WRNTW |" not in content

    @pytest.mark.asyncio
    async def test_when_every_mover_is_thin_it_says_none_traded_enough(self):
        early = int(datetime(2026, 11, 4, 9, 19, tzinfo=timezone.utc).timestamp() * 1000)
        movers = [_mover("WRNTW", price=0.08, change_percent=150.0, traded_value=900.0, traded_at=early)]
        provider = _make_fake_financial_provider(intel=_make_fake_intel_source(movers=movers))

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_market_movers(direction="gainers")

        assert "**Excluded (under $100,000 traded on 2026-11-04):** WRNTW" in content
        assert "No gainer traded $100,000 or more on 2026-11-04." in content
        assert "data available" not in content

    @pytest.mark.asyncio
    async def test_a_sub_dollar_price_keeps_four_places(self):
        movers = [
            _mover("SUBD", price=0.1896, change_percent=50.0),
            _mover("WHOL", price=12.0, change_percent=40.0),
        ]
        provider = _make_fake_financial_provider(intel=_make_fake_intel_source(movers=movers))

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_market_movers(direction="gainers")

        assert "| $0.1896 |" in content
        assert "| $12.00 |" in content

    @pytest.mark.asyncio
    async def test_each_last_trade_names_its_session(self):
        # 2026-11-03, a NYSE session: 19:59 ET is after the close, 13:11 ET inside it.
        after_close = int(datetime(2026, 11, 4, 0, 59, tzinfo=timezone.utc).timestamp() * 1000)
        in_session = int(datetime(2026, 11, 3, 18, 11, tzinfo=timezone.utc).timestamp() * 1000)
        movers = [
            _mover("AHUP", price=3.59, change_percent=262.4, traded_at=after_close),
            _mover("RGUP", price=8.0, change_percent=40.0, traded_at=in_session),
        ]
        provider = _make_fake_financial_provider(intel=_make_fake_intel_source(movers=movers))

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_market_movers(direction="gainers")

        assert "**Prices:** each stock's latest trade on 2026-11-03 (ET)" in content
        assert "| +262.40% | 19:59 Extended Hours |" in content
        assert "| +40.00% | 13:11 Regular Hours |" in content

    @pytest.mark.asyncio
    async def test_the_closing_cross_and_an_older_trade_read_as_what_they_are(self):
        # The closing auction reports a moment after 16:00 ET; a name that last
        # traded the session before keeps that day beside the newest one's.
        cross = int(datetime(2026, 11, 3, 21, 0, 2, tzinfo=timezone.utc).timestamp() * 1000)
        older = int(datetime(2026, 11, 3, 0, 58, tzinfo=timezone.utc).timestamp() * 1000)
        after = int(datetime(2026, 11, 3, 21, 0, 30, tzinfo=timezone.utc).timestamp() * 1000)
        movers = [
            _mover("CRSS", price=5.0, change_percent=30.0, traded_at=cross),
            _mover("OLDR", price=2.0, change_percent=20.0, traded_at=older),
            _mover("AFTR", price=3.0, change_percent=10.0, traded_at=after),
        ]
        provider = _make_fake_financial_provider(intel=_make_fake_intel_source(movers=movers))

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_market_movers(direction="gainers")

        # The rows trade on two days, so the header claims neither.
        assert "each stock's latest trade (ET); Change% is from the previous close" in content
        assert "| +30.00% | 16:00 Regular Hours |" in content
        assert "| +20.00% | 2026-11-02 19:58 Extended Hours |" in content
        assert "| +10.00% | 16:00 Extended Hours |" in content

    @pytest.mark.asyncio
    async def test_a_row_priced_by_its_day_close_is_named_so(self):
        traded = int(datetime(2026, 11, 3, 18, 11, tzinfo=timezone.utc).timestamp() * 1000)
        movers = [
            _mover("TRDD", price=5.0, change_percent=30.0, traded_at=traded),
            _mover("BARR", price=2.0, change_percent=20.0),
        ]
        provider = _make_fake_financial_provider(intel=_make_fake_intel_source(movers=movers))

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_market_movers(direction="gainers")

        assert (
            "**Prices:** each stock's latest trade (ET), or its day close where Last Trade is N/A;"
            " Change% is from the previous close"
        ) in content

    @pytest.mark.asyncio
    async def test_losers(self):
        movers = [_mover("INTC", price=20.0, change_percent=-6.3)]
        intel = _make_fake_intel_source(movers=movers)
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers(direction="losers")

        assert "Market Losers" in content
        assert "INTC" in content
        assert "-6.30%" in content

    @pytest.mark.asyncio
    async def test_empty_results(self):
        intel = _make_fake_intel_source(movers=[])
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers()

        assert "No gainers data available" in content
        assert artifact["results"] == []

    @pytest.mark.asyncio
    async def test_no_intel_source(self):
        provider = _make_fake_financial_provider(intel=None)

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers()

        assert "not available" in content

    @pytest.mark.asyncio
    async def test_an_unknown_direction_is_refused_unmarked(self):
        """The data API 422s any other direction; asking it would read as our
        failure, so the tool refuses first and the run stays ok."""
        from src.observability.langsmith_tracer import NonRetainingTracer
        from src.tools.market_data.tool import get_market_movers

        intel = _make_fake_intel_source()
        provider = _make_fake_financial_provider(intel=intel)
        client = _RecordingClient()

        with patch(f"{_QUOTES_MOD}.get_financial_data_provider", return_value=provider):
            msg = await get_market_movers.ainvoke(
                {
                    "type": "tool_call",
                    "name": "get_market_movers",
                    "args": {"direction": "sideways"},
                    "id": "tc-1",
                },
                {"callbacks": [NonRetainingTracer(project_name="proj", client=client)]},
            )

        assert "direction must be 'gainers' or 'losers', got 'sideways'" in msg.content
        assert msg.artifact["error"] == (
            "direction must be 'gainers' or 'losers', got 'sideways'"
        )
        intel.get_movers.assert_not_called()
        (update,) = [u for u in client.updated if u["run_type"] == "tool"]
        assert update["extra"]["metadata"]["tool_outcome"] == "ok"
        assert update["error"] is None


# ---------------------------------------------------------------------------
# CMDP display fixes: per-instrument Market header, unitless index levels,
# US-Eastern session line gated to US listings. Neutral placeholder symbols.
# ---------------------------------------------------------------------------

def _quote_only_financial(profile, quote):
    """Financial source with just a profile + realtime quote (FMP-quote path)."""
    return _make_fake_financial_source(profile_data=profile, quote_data=quote)


class TestProviderQuoteFallback:
    """With no snapshot, the overview prints the financial provider's quote,
    which must say how current it is and survive null fields."""

    _PROFILE = [{"companyName": "Placeholder HK Co.", "marketCap": 1e12,
                 "price": 318.20, "exchangeShortName": "HKSE"}]

    async def _overview(self, quote):
        mdp = AsyncMock()
        mdp.get_snapshots = AsyncMock(return_value=[])
        provider = _make_fake_financial_provider(
            financial=_quote_only_financial(self._PROFILE, [quote])
        )
        # Venue open: a closed one titles any current row as its last close.
        with patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider), \
                patch(f"{_COMPANY_MOD}.get_market_data_provider", return_value=mdp), \
                patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]), \
                patch("src.data_client.freshness._venue_closed", return_value=False):
            content, _ = await fetch_company_overview("0700.HK")
        return content

    @pytest.mark.asyncio
    async def test_a_delayed_provider_quote_is_not_titled_real_time(self):
        content = await self._overview({"price": 318.20, "change": 1.5,
                                        "changePercentage": 0.47, "tier": "delayed_15m"})
        assert "### Quote (delayed 15m)" in content
        assert "Real-Time Quote" not in content
        assert "**Price:** HK$318.20 (+1.50 / +0.47%)" in content

    @pytest.mark.asyncio
    async def test_null_move_fields_drop_out_instead_of_raising(self):
        content = await self._overview({"price": 318.20, "change": None,
                                        "changePercentage": None})
        assert "Error" not in content
        assert "### Quote (freshness unknown)" in content
        assert "**Price:** HK$318.20\n" in content


@pytest.mark.parametrize(
    ("closed", "label", "heading"),
    [
        (True, "live", "### Quote (last close)"),
        (True, "delayed", "### Quote (last close)"),
        (False, "live", "### Real-Time Quote"),
        (False, "delayed", "### Quote (delayed 15m)"),
        (True, "stale", "### Quote (stale)"),
    ],
)
def test_a_closed_sessions_final_row_is_titled_last_close(closed, label, heading):
    # On a closed venue a current row, or a delayed one nothing measured, is the
    # session's final price, not a real-time one; a stale row keeps naming its
    # staleness.
    assert _quote_heading({"closed": closed, "label": label}) == heading


@pytest.mark.parametrize(
    ("symbol", "printed", "at", "heading"),
    [
        # A measured print still inside the feed's delay predates the auction.
        ("0700.HK", datetime(2026, 9, 16, 15, 57, tzinfo=ZoneInfo("Asia/Hong_Kong")),
         datetime(2026, 9, 16, 16, 12, tzinfo=ZoneInfo("Asia/Hong_Kong")), "### Quote (delayed 15m)"),
        ("600519.SH", datetime(2026, 9, 16, 14, 46, tzinfo=ZoneInfo("Asia/Shanghai")),
         datetime(2026, 9, 16, 15, 2, tzinfo=ZoneInfo("Asia/Shanghai")), "### Quote (delayed 15m)"),
        # Once the auction print has arrived the row is the settled close.
        ("0700.HK", datetime(2026, 9, 16, 16, 8, tzinfo=ZoneInfo("Asia/Hong_Kong")),
         datetime(2026, 9, 16, 16, 25, tzinfo=ZoneInfo("Asia/Hong_Kong")), "### Quote (last close)"),
        ("600519.SH", datetime(2026, 9, 16, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
         datetime(2026, 9, 16, 15, 16, tzinfo=ZoneInfo("Asia/Shanghai")), "### Quote (last close)"),
    ],
)
def test_a_measured_delayed_row_is_last_close_only_once_the_auction_printed(
    symbol, printed, at, heading
):
    row = {"symbol": symbol, "as_of": int(printed.timestamp() * 1000), "tier": "delayed_15m"}
    freshness = stamp_quote(row, at, ref=resolve_ref(symbol))["freshness"]
    assert _quote_heading(freshness) == heading


class TestMarketHeaderLabel:
    """`**Market:**` header is derived from the resolved instrument, not hardcoded."""

    @pytest.mark.asyncio
    async def test_us_header_byte_compatible(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_daily_prices("AAPL", limit=5)
        assert "**Market:** US Stock" in content

    @pytest.mark.asyncio
    async def test_hk_header(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_daily_prices("0700.HK", limit=5)
        assert "**Market:** HK Stock" in content
        assert "**Market:** US Stock" not in content

    @pytest.mark.asyncio
    async def test_ashare_header(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_daily_prices("600519.SS", limit=5)
        assert "**Market:** A-Share" in content

    @pytest.mark.asyncio
    async def test_an_unknown_venue_names_no_market_or_currency(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("PETR4.SA", limit=5)
        assert "**Market:** Stock" in content
        assert "price_currency" not in artifact

    @pytest.mark.asyncio
    async def test_beijing_listing_is_an_ashare_too(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_daily_prices("430047.BJ", limit=5)
        assert "**Market:** A-Share" in content

    @pytest.mark.asyncio
    async def test_ashare_header_carries_chinese_name(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider), \
                patch(f"{_PRICES_MOD}.display_names", new=AsyncMock(return_value=("甲公司", None))):
            content, artifact = await fetch_daily_prices("600519.SS", limit=5)
        assert "**Company:** 甲公司" in content
        assert artifact["name"] == "甲公司"

    @pytest.mark.asyncio
    async def test_us_header_has_no_company_line(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_PRICES_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_daily_prices("AAPL", limit=5)
        assert "**Company:**" not in content
        assert "name" not in artifact


class TestOverviewSessionClockGating:
    """US listings show the US-Eastern session phase + 'As of … ET' clock; non-US
    listings show their exchange-calendar phase + the exchange-local clock (never ET)."""

    _US_PROFILE = [{
        "companyName": "Placeholder US Co.",
        "sector": "Technology",
        "industry": "Software",
        "marketCap": 1_000_000_000_000,
        "price": 100.0,
        "exchangeShortName": "NASDAQ",
    }]
    _HK_PROFILE = [{
        "companyName": "Placeholder HK Co.",
        "sector": "Technology",
        "industry": "Software",
        "marketCap": 1_000_000_000_000,
        "price": 318.20,
        "exchangeShortName": "HKSE",
    }]
    _QUOTE = [{
        "price": 100.0, "change": 1.0, "changePercentage": 1.0,
        "dayHigh": 101.0, "dayLow": 99.0, "yearHigh": 120.0, "yearLow": 80.0,
        "open": 99.5, "previousClose": 99.0, "volume": 1_000_000, "avgVolume": 1_200_000,
    }]

    def _patches(self, financial):
        mdp = AsyncMock()
        mdp.get_snapshots = AsyncMock(return_value=[])  # force FMP-quote path
        provider = _make_fake_financial_provider(financial=financial)
        return (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}.get_market_data_provider", return_value=mdp),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        )

    @pytest.mark.asyncio
    async def test_us_shows_session_and_et_clock(self):
        financial = _quote_only_financial(self._US_PROFILE, self._QUOTE)
        p1, p2, p3 = self._patches(financial)
        with p1, p2, p3:
            content, _ = await fetch_company_overview("AAPL")
        assert "**Market Status:**" in content
        assert "ET" in content

    @pytest.mark.asyncio
    async def test_non_us_shows_calendar_phase_and_local_clock(self):
        financial = _quote_only_financial(self._HK_PROFILE, self._QUOTE)
        p1, p2, p3 = self._patches(financial)
        with p1, p2, p3:
            content, _ = await fetch_company_overview("0700.HK")
        status_line = next(
            (ln for ln in content.splitlines() if ln.startswith("**Market Status:**")), None
        )
        # Non-US listings DO get a status line, but from the exchange calendar:
        # the HK exchange-local clock (HKT), never the US-Eastern "ET" clock.
        assert status_line is not None
        assert "HKT" in status_line
        assert " ET" not in status_line
        # …and a phase label the calendar can actually produce (time-of-run dependent).
        assert any(
            label in status_line
            for label in (
                "Regular Hours", "Lunch Break", "Market Closed",
                "Pre-Market", "After-Hours", "Halted",
            )
        )
        # Prices still localize to the HK listing currency.
        assert "HK$" in content


# ---------------------------------------------------------------------------
# fetch_quote
# ---------------------------------------------------------------------------

def _make_fake_snapshot_provider(snaps):
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=snaps)
    return provider


class TestFetchQuote:
    @pytest.fixture(autouse=True)
    def _empty_quote_cache(self):
        # Every quote is a miss the provider fills, and names stay off the network.
        store = {}

        async def mget(keys):
            return [store.get(k) for k in keys]

        async def set_many(items):
            store.update((key, value) for key, value, _ in items)

        cache = MagicMock(mget=mget, set_many=set_many)
        with patch(f"{_QCS_MOD}.get_cache_client", return_value=cache), \
                patch(f"{_QCS_MOD}.fill_quote_names", new=AsyncMock()):
            yield

    @pytest.mark.asyncio
    async def test_multi_symbol_quotes(self):
        snaps = [
            {"symbol": "NVDA", "price": 231.0, "change_percent": 2.31,
             "volume": 187_000_000, "last_trade_price": 233.45, "market_status": "open"},
            {"symbol": "TSLA", "price": 410.0, "change_percent": -1.02,
             "volume": 95_000_000, "last_trade_price": 412.10, "market_status": "open"},
        ]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_quote(["nvda", "TSLA"])
        provider.get_snapshots.assert_awaited_once()
        # Canonical entry: US tickers resolve to themselves and reach the provider.
        assert provider.get_snapshots.await_args.args[0] == ["NVDA", "TSLA"]
        assert "$233.45" in content
        assert "TSLA" in content
        assert artifact["type"] == "quote"
        assert len(artifact["quotes"]) == 2

    @pytest.mark.asyncio
    async def test_a_quote_is_read_from_the_chain_not_a_cached_row(self):
        # The Source fallbacks note names what this read's chain took; a row
        # another request cached carries none.
        snaps = [{"symbol": "NVDA", "last_trade_price": 233.45, "tier": "realtime"}]
        provider = _make_fake_snapshot_provider(snaps)
        cached = {"symbol": "NVDA", "last_trade_price": 1.0, "tier": "realtime"}
        cache = MagicMock(mget=AsyncMock(return_value=[cached]), set_many=AsyncMock())
        with patch(f"{_QCS_MOD}.get_cache_client", return_value=cache), \
                patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_quote(["NVDA"])
        provider.get_snapshots.assert_awaited_once()
        assert "$233.45" in content

    @pytest.mark.asyncio
    async def test_a_pre_market_row_is_priced_at_its_last_trade(self):
        # Before the open a snapshot has no session close, only the last trade.
        snaps = [{"symbol": "AAPL", "price": None, "last_trade_price": 332.21, "change_percent": 0.09,
                  "previous_close": 331.9, "market_status": "early_trading", "tier": "realtime"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_quote(["AAPL"])
        assert "AAPL  $332.21" in content
        assert artifact["quotes"][0]["price"] == 332.21

    @pytest.mark.asyncio
    async def test_stamping_a_row_leaves_the_fetched_row_alone(self):
        # Requests that share one fetch share its rows, so the tool stamps a copy.
        snaps = [{"symbol": "NVDA", "last_trade_price": 233.45, "tier": "realtime"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            _, artifact = await fetch_quote(["NVDA"])
        assert "freshness" in artifact["quotes"][0]
        assert "freshness" not in snaps[0]

    @pytest.mark.asyncio
    async def test_quote_rows_carry_tier_and_measured_freshness(self):
        # The chart card reads freshness off each row, so every row carries the
        # declared tier and the measurement taken against it.
        snaps = [
            {"symbol": "0700.HK", "price": 318.20, "change_percent": -0.5,
             "volume": 12_000_000, "last_trade_price": 318.20,
             "tier": "delayed_15m", "source": "fmp"},
            {"symbol": "600519.SS", "price": 1712.40, "change_percent": 1.25,
             "volume": 3_000_000, "last_trade_price": 1712.40,
             "tier": "realtime", "source": "tushare",
             "as_of": int(datetime.now(timezone.utc).timestamp() * 1000)},
        ]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            _, artifact = await fetch_quote(["0700.HK", "600519.SS"])

        hk, cn = artifact["quotes"]
        assert hk["tier"] == "delayed_15m"
        assert hk["freshness"]["label"] == "delayed"
        assert hk["freshness"]["measured"] is False
        assert hk["freshness"]["source"] == "fmp"
        assert cn["tier"] == "realtime"
        assert cn["freshness"]["measured"] is True
        assert cn["freshness"]["actual_latest"] == cn["as_of"]
        # as_of_local is the PRINT time in venue local time, not retrieval.
        assert cn["as_of_local"].endswith("UTC+8")
        assert artifact["all_realtime"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("closed", [False, True])
    async def test_all_realtime_true_only_when_every_row_is_live(self, closed):
        # A realtime row on a closed venue is its last close, so the card must
        # not head it "Live Quotes".
        snaps = [{"symbol": "NVDA", "price": 231.0, "change_percent": 2.31,
                  "volume": 1, "last_trade_price": 233.45,
                  "market_status": "open", "tier": "realtime"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
                patch("src.data_client.freshness._venue_closed", return_value=closed):
            _, artifact = await fetch_quote(["NVDA"])
        assert artifact["all_realtime"] is (not closed)
        assert artifact["quotes"][0]["currency"] == "USD"

    @pytest.mark.asyncio
    async def test_a_quote_on_an_unknown_venue_names_no_currency(self):
        snaps = [{"symbol": "PETR4.SA", "price": 45.78, "change_percent": -0.37, "volume": 1,
                  "market_status": "open", "tier": "delayed_15m"},
                 {"symbol": "BRK-B", "price": 571.48, "change_percent": -0.15, "volume": 1,
                  "market_status": "open", "tier": "realtime"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            _, artifact = await fetch_quote(["PETR4.SA", "BRK.B"])
        assert [q["currency"] for q in artifact["quotes"]] == [None, "USD"]

    @pytest.mark.asyncio
    async def test_a_closed_listing_no_provider_quotes_reads_its_daily_close(self):
        # Beijing has no realtime provider: while its venue is closed the row
        # comes from the daily bars, as on the REST quote routes, and is named.
        snaps = [{"symbol": "600519.SH", "price": 1255.79, "change_percent": -0.22,
                  "volume": 2_500_000, "last_trade_price": 1255.79, "tier": "realtime"}]
        daily = {"symbol": "920395.BJ", "price": 6.68, "change": 0.06,
                 "change_percent": 0.91, "previous_close": 6.62, "volume": 307_600,
                 "market_status": "closed", "source": "daily", "tier": "eod",
                 "as_of": int(datetime(2026, 9, 30, 7, tzinfo=timezone.utc).timestamp() * 1000),
                 "regular_only": True}
        provider = _make_fake_snapshot_provider(snaps)
        names = AsyncMock()
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
                patch(f"{_DAILY_MOD}.daily_fallback_snapshot", new=AsyncMock(return_value=daily)), \
                patch(f"{_DAILY_MOD}.clock_for_ref") as clock, \
                patch(f"{_QCS_MOD}.fill_quote_names", new=names):
            clock.return_value.is_closed.return_value = True
            content, artifact = await fetch_quote(["600519.SH", "920395.BJ"])

        assert "920395.BJ  CN¥6.68  +0.91% on 2026-09-30" in content
        assert "no data" not in content
        bj = artifact["quotes"][1]
        assert (bj["source"], bj["tier"], bj["currency"]) == ("daily", "eod", "CNY")
        assert [r["symbol"] for r in names.await_args.args[0]] == ["600519.SH", "920395.BJ"]

    @pytest.mark.asyncio
    async def test_no_row_for_any_symbol_is_unavailable(self):
        """Nothing quoted for any symbol named is the instrument having
        nothing, not a source failing, so the run is not errored."""
        from src.observability.langsmith_tracer import NonRetainingTracer
        from src.tools.market_data.tool import get_quote

        provider = _make_fake_snapshot_provider([])
        client = _RecordingClient()
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
                patch(f"{_DAILY_MOD}.daily_fallback_snapshot", new=AsyncMock(return_value=None)):
            msg = await get_quote.ainvoke(
                {"type": "tool_call", "name": "get_quote", "args": {"symbols": ["ZZZQ"]}, "id": "tc-1"},
                {"callbacks": [NonRetainingTracer(project_name="proj", client=client)]},
            )

        assert "No quote data available for ZZZQ" in msg.content
        (update,) = [u for u in client.updated if u["run_type"] == "tool"]
        metadata = update["extra"]["metadata"]
        assert (metadata["tool_outcome"], metadata["error_class"]) == ("unavailable", "no_data")
        assert update["error"] is None

    @pytest.mark.asyncio
    async def test_a_missing_listing_stays_missing_while_its_venue_trades(self):
        # During a session a last close would pass a quote outage off as live.
        snaps = [{"symbol": "600519.SH", "price": 1255.79, "change_percent": -0.22,
                  "volume": 2_500_000, "last_trade_price": 1255.79, "tier": "realtime"}]
        provider = _make_fake_snapshot_provider(snaps)
        daily = AsyncMock()
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
                patch(f"{_DAILY_MOD}.daily_fallback_snapshot", new=daily), \
                patch(f"{_DAILY_MOD}.clock_for_ref") as clock:
            clock.return_value.is_closed.return_value = False
            content, _ = await fetch_quote(["600519.SH", "920395.BJ"])

        assert "(no data: 920395.BJ)" in content
        daily.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_two_spellings_of_one_listing_are_quoted_once(self):
        snaps = [{"symbol": "BRK-B", "price": 557.07, "change_percent": 0.2,
                  "volume": 1, "last_trade_price": 557.07, "market_status": "open"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_quote(["BRK.B", "BRK-B"])
        assert provider.get_snapshots.await_args.args[0] == ["BRK-B"]
        assert len(artifact["quotes"]) == 1 and content.count("BRK-B") == 1

    @pytest.mark.asyncio
    async def test_a_listing_whose_sources_failed_is_named_as_a_fallback_not_no_data(self):
        from src.data_client.market_data_provider import MarketDataProvider, ProviderEntry
        from src.tools.market_data.tool import get_quote

        class _Tushare:
            async def get_snapshots(self, **_):
                raise ConnectionError("down")

        class _Fmp:
            async def get_snapshots(self, symbols, **_):
                return [{"symbol": "600519.SH", "price": 1255.79, "change_percent": -0.22,
                         "volume": 1, "last_trade_price": 1255.79}]

        provider = MarketDataProvider([
            ProviderEntry("tushare", _Tushare(), {"cn"}), ProviderEntry("fmp", _Fmp(), {"all"}),
        ])
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
                patch(f"{_QUOTES_MOD}.fill_quote_names", new=AsyncMock()), \
                patch(f"{_DAILY_MOD}.clock_for_ref") as clock:
            clock.return_value.is_closed.return_value = False
            msg = await get_quote.ainvoke({
                "type": "tool_call", "name": "get_quote", "id": "tc-1",
                "args": {"symbols": ["920799.BJ", "600519.SH"]},
            })

        assert "no data" not in msg.content
        assert "- 600519.SH: tushare failed (ConnectionError); fmp answered quote" in msg.content
        assert "- 920799.BJ: tushare failed (ConnectionError); no source answered quote" in msg.content

    @pytest.mark.asyncio
    async def test_a_closed_listing_whose_sources_failed_names_the_daily_close(self):
        from src.data_client.market_data_provider import MarketDataProvider, ProviderEntry
        from src.tools.market_data.tool import get_quote

        class _Tushare:
            async def get_snapshots(self, **_):
                raise ConnectionError("down")

        daily = {"symbol": "920799.BJ", "price": 6.68, "change": 0.06, "change_percent": 0.91,
                 "previous_close": 6.62, "market_status": "closed", "source": "daily", "tier": "eod",
                 "as_of": int(datetime(2026, 9, 30, 7, tzinfo=timezone.utc).timestamp() * 1000),
                 "regular_only": True}
        provider = MarketDataProvider([ProviderEntry("tushare", _Tushare(), {"cn"})])
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
                patch(f"{_DAILY_MOD}.daily_fallback_snapshot", new=AsyncMock(return_value=daily)), \
                patch(f"{_DAILY_MOD}.clock_for_ref") as clock:
            clock.return_value.is_closed.return_value = True
            msg = await get_quote.ainvoke({
                "type": "tool_call", "name": "get_quote", "id": "tc-1",
                "args": {"symbols": ["920799.BJ"]},
            })

        assert "920799.BJ  CN¥6.68" in msg.content
        assert "- 920799.BJ: tushare failed (ConnectionError); daily close answered quote" in msg.content

    @pytest.mark.asyncio
    async def test_reports_missing_symbols(self):
        snaps = [{"symbol": "NVDA", "price": 231.0, "change_percent": 2.31,
                  "volume": 1, "last_trade_price": 233.45, "market_status": "open"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_quote(["NVDA", "ZZZZ"])
        assert "no data: ZZZZ" in content

    @pytest.mark.asyncio
    async def test_empty_symbols(self):
        content, artifact = await fetch_quote([])
        assert "No symbols" in content
        assert artifact["quotes"] == []

    @pytest.mark.asyncio
    async def test_asset_type_coerced_to_known_values(self):
        """LLM-supplied asset_type reaches the provider request path — anything
        outside the two documented values is coerced, never forwarded."""
        snaps = [{"symbol": "NVDA", "price": 231.0, "change_percent": 2.31,
                  "volume": 1, "last_trade_price": 233.45, "market_status": "open"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            await fetch_quote(["NVDA"], asset_type="../../admin")
        assert provider.get_snapshots.await_args.kwargs["asset_type"] == "stocks"

    @pytest.mark.asyncio
    async def test_provider_error_returns_message(self):
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(side_effect=RuntimeError("boom"))
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_quote(["NVDA"])
        assert "Error" in content
        assert artifact["quotes"] == []

    @pytest.mark.asyncio
    async def test_canonical_index_spelling_reaches_provider(self):
        # "^GSPC" canonicalizes to its legacy REST spelling before the snapshot
        # fetch, and FMP returns it caret-stripped — neither may read as missing.
        snaps = [{"symbol": "GSPC", "price": 5000.0, "change_percent": 0.5,
                  "volume": 0, "last_trade_price": 5000.0, "market_status": "open"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_quote(["^GSPC"], asset_type="indices")
        # Resolved (caret-stripped) spelling reached the provider.
        assert provider.get_snapshots.await_args.args[0] == ["GSPC"]
        assert "GSPC" in content
        assert "no data" not in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("spelling", ["^GSPC", "I:SPX", "SPX.INDEX"])
    async def test_a_caret_index_is_asked_as_an_index_on_the_stocks_path(self, spelling):
        # The legacy spelling of any index-only spelling is GSPC, which only
        # the indices endpoint reads as the S&P 500; the stock stays on stocks.
        rows = {
            "stocks": [{"symbol": "AAPL", "last_trade_price": 230.0, "change_percent": 1.0}],
            "indices": [{"symbol": "GSPC", "price": 6120.5, "change_percent": 0.42}],
        }
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(
            side_effect=lambda symbols, asset_type, user_id: rows[asset_type]
        )
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_quote(["AAPL", spelling])
        asked = {c.kwargs["asset_type"]: c.args[0] for c in provider.get_snapshots.await_args_list}
        assert asked == {"stocks": ["AAPL"], "indices": ["GSPC"]}
        assert "GSPC  6,120.50" in content
        assert "no data" not in content
        assert [q["asset_class"] for q in artifact["quotes"]] == ["equity", "index"]

    @pytest.mark.asyncio
    async def test_one_endpoint_failing_keeps_the_other_endpoints_rows(self):
        def answer(symbols, asset_type, user_id):
            if asset_type == "stocks":
                raise RuntimeError("stocks endpoint down")
            return [{"symbol": "GSPC", "price": 6120.5, "change_percent": 0.42}]

        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(side_effect=answer)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
             patch(f"{_QCS_MOD}.fill_from_daily",
                   AsyncMock(side_effect=lambda served, refs, user_id: served)):
            content, artifact = await fetch_quote(["AAPL", "^GSPC"])
        assert "GSPC  6,120.50" in content
        # The failed request is not reported as a listing with no data.
        assert "(request failed: AAPL)" in content
        assert "no data" not in content
        assert [q["asset_class"] for q in artifact["quotes"]] == ["index"]

    @pytest.mark.asyncio
    async def test_a_failed_endpoint_leaves_the_daily_fill_to_the_rest(self):
        # The indices call failing must not cost the stock its daily close.
        def answer(symbols, asset_type, user_id):
            if asset_type == "indices":
                raise RuntimeError("indices endpoint down")
            return []

        daily = {"symbol": "AAPL", "price": 230.0, "change_percent": 1.0}
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(side_effect=answer)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
             patch(f"{_QCS_MOD}.fill_from_daily",
                   AsyncMock(side_effect=lambda served, refs, user_id: [*served, (refs[0], daily)])):
            content, artifact = await fetch_quote(["AAPL", "^GSPC"])
        assert "AAPL" in content
        assert "(request failed: ^GSPC)" in content
        assert [q["symbol"] for q in artifact["quotes"]] == ["AAPL"]

    @pytest.mark.asyncio
    async def test_a_failed_endpoint_whose_rows_the_daily_fill_answered_is_not_reported(self):
        def answer(symbols, asset_type, user_id):
            if asset_type == "stocks":
                raise RuntimeError("stocks endpoint down")
            return [{"symbol": "GSPC", "price": 6120.5, "change_percent": 0.42}]

        daily = {"symbol": "AAPL", "price": 230.0, "change_percent": 1.0}
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(side_effect=answer)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider), \
             patch(f"{_QCS_MOD}.fill_from_daily",
                   AsyncMock(side_effect=lambda served, refs, user_id: [*served, (refs[0], daily)])), \
             patch(f"{_QUOTES_MOD}._mark_failure") as marked:
            content, _ = await fetch_quote(["AAPL", "^GSPC"])
        assert "request failed" not in content
        marked.assert_not_called()

    @pytest.mark.asyncio
    async def test_alias_with_differing_canonical_not_flagged_missing(self):
        # "SPX" is requested as its legacy spelling "GSPC"; diffing on the raw
        # input (vs the resolved spelling the provider saw) would wrongly flag it.
        snaps = [{"symbol": "GSPC", "price": 6120.5, "change_percent": 0.42,
                  "volume": 0, "last_trade_price": 6120.5, "market_status": "open"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_quote(["SPX"], asset_type="indices")
        assert provider.get_snapshots.await_args.args[0] == ["GSPC"]
        assert "no data" not in content

    @pytest.mark.asyncio
    async def test_missing_reported_under_input_spelling(self):
        # A resolved-alias hit ("SPX"->"GSPC") plus a truly-absent symbol: only
        # the absent one is flagged, under the caller's input spelling.
        snaps = [{"symbol": "GSPC", "price": 6120.5, "change_percent": 0.42,
                  "volume": 0, "last_trade_price": 6120.5, "market_status": "open"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_quote(["SPX", "ZZZZ"], asset_type="indices")
        assert "no data: ZZZZ" in content
        assert "no data: SPX" not in content

    @pytest.mark.asyncio
    async def test_rows_carry_the_class_they_were_asked_as(self):
        # The echo "GSPC" is a bare ticker on its own; the row is the index
        # that "SPX" was asked as, so it is measured on the index clock.
        snaps = [{"symbol": "GSPC", "last_trade_price": 6120.5, "tier": "realtime"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            _, artifact = await fetch_quote(["SPX"], asset_type="indices")
        assert [q["asset_class"] for q in artifact["quotes"]] == ["index"]

    @pytest.mark.asyncio
    async def test_a_bare_alias_asked_as_a_stock_is_the_stock(self):
        # COMP names both Compass and the Nasdaq Composite; on the stocks path
        # it is requested and stamped as the stock.
        snaps = [{"symbol": "COMP", "last_trade_price": 9.22, "tier": "realtime"}]
        provider = _make_fake_snapshot_provider(snaps)
        with patch(f"{_QCS_MOD}.get_market_data_provider", return_value=provider):
            _, artifact = await fetch_quote(["COMP"])
        assert provider.get_snapshots.await_args.args[0] == ["COMP"]
        assert [q["asset_class"] for q in artifact["quotes"]] == ["equity"]


class TestFormingIndexRow:
    """A daily-only index publisher is a session behind all day; the snapshot
    chain supplies today's row, but only once the snapshot prices today."""

    # The series' last posted bar: the 2026-09-09 session, closed at 3951.51.
    _NEWEST = {"date": "2026-09-09", "close": 3951.51}
    _CST = ZoneInfo("Asia/Shanghai")

    def _snap(self, **over):
        snap = {
            "symbol": "000001.SS", "price": 3934.02, "change": -17.49,
            "change_percent": -0.4426, "previous_close": 3951.51,
            "open": 3950.0, "high": 3955.0, "low": 3930.0, "volume": None,
            "tier": "delayed_15m", "source": "fmp", "as_of": None,
        }
        snap.update(over)
        return snap

    def _forming(self, snap, newest):
        from src.tools.market_data.market_overview import _forming_bar

        with patch(f"{_MKT_MOD}.clock_for") as clock:
            clock.return_value.expected_latest_daily_date.return_value = "2026-09-10"
            clock.return_value.tz = self._CST
            return _forming_bar("000001.SS", snap, newest)

    def _as_of(self, *local):
        return int(datetime(*local, tzinfo=self._CST).timestamp() * 1000)

    def test_stands_in_when_the_series_is_behind_the_venue_date(self):
        row = self._forming(self._snap(), self._NEWEST)
        assert row is not None
        assert row["date"] == "2026-09-10"
        assert row["close"] == 3934.02
        assert row["changePercent"] == pytest.approx(-0.4426)
        assert row["forming"] is True

    def test_steps_aside_once_the_publisher_has_posted_today(self):
        newest = {"date": "2026-09-10", "close": 3934.02}
        assert self._forming(self._snap(), newest) is None

    def test_derives_the_move_from_previous_close_when_the_feed_omits_it(self):
        row = self._forming(self._snap(change=None, change_percent=None), self._NEWEST)
        assert row["change"] == pytest.approx(3934.02 - 3951.51)
        assert row["changePercent"] == pytest.approx((3934.02 - 3951.51) / 3951.51 * 100)

    def test_no_price_means_no_row(self):
        assert self._forming(self._snap(price=None), self._NEWEST) is None

    def test_a_snapshot_printed_today_stands_in(self):
        snap = self._snap(as_of=self._as_of(2026, 9, 10, 9, 45))
        row = self._forming(snap, self._NEWEST)
        assert row is not None and row["date"] == "2026-09-10"

    def test_yesterdays_print_just_after_the_open_is_not_today(self):
        # 09:31 on the 10th: the snapshot still holds the 9th's close, printed
        # at 15:00 that day. Standing it in would date yesterday's move today
        # and repeat the 9th's bar.
        snap = self._snap(as_of=self._as_of(2026, 9, 9, 15, 0), price=3951.51,
                          previous_close=3969.00, change=-17.49)
        assert self._forming(snap, self._NEWEST) is None

    def test_unprinted_snapshot_a_session_behind_is_not_today(self):
        # No print time, and its previous close is the bar BEFORE the series'
        # last one: the snapshot is the 9th, not the 10th.
        snap = self._snap(price=3951.51, previous_close=3969.00)
        assert self._forming(snap, self._NEWEST) is None

    def test_unprinted_snapshot_with_no_series_to_check_against_is_not_today(self):
        assert self._forming(self._snap(), None) is None


class TestCnOverviewHeaderAndCurrency:
    """A CNY listing's overview: headline price, English name, and currency."""

    # A CN profile's price comes from the last PUBLISHED daily row, which during
    # a session is the previous session's close.
    _PROFILE = [{
        "companyName": "Alpha Co., Ltd.",
        "nameLocal": "甲公司",
        "sector": None,
        "industry": "白酒",
        "marketCap": 1_000_000_000_000,
        "price": 100.0,
        "exchangeShortName": "SSE",
        "currency": "CNY",
    }]
    _SNAPSHOT = {
        "symbol": "600001.SS",
        "price": 104.0,
        "previous_close": 100.0,
        "change": 4.0,
        "change_percent": 4.0,
        "open": 100.5,
        "high": 105.0,
        "low": 99.5,
        "volume": 1_000_000,
        "source": "tushare",
        "tier": "realtime",
    }

    def _financial(self):
        return _make_fake_financial_source(
            profile_data=self._PROFILE,
            income_stmt=[{
                "date": "2026-06-30", "period": "Q2", "fiscalYear": "2026",
                "reportedCurrency": "CNY",
                "revenue": 90_000_000_000, "netIncome": 40_000_000_000,
            }],
            cash_flow=[{
                "date": "2026-06-30", "reportedCurrency": "CNY",
                "operatingCashFlow": 28_000_000_000,
                "capitalExpenditure": -3_000_000_000,
                "freeCashFlow": 25_000_000_000,
            }],
            price_target_consensus=[{
                "targetMedian": 160.0, "targetLow": 140.0, "targetHigh": 180.0,
                "targetConsensus": 165.5,
            }],
            product_data=[{"2026-06-30": {"甲酒": 70_000_000_000, "乙酒": 20_000_000_000}}],
        )

    def _patches(self, snapshot):
        mdp = AsyncMock()
        mdp.get_snapshots = AsyncMock(return_value=[snapshot] if snapshot else [])
        provider = _make_fake_financial_provider(financial=self._financial())
        return (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}.get_market_data_provider", return_value=mdp),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        )

    @pytest.mark.asyncio
    async def test_headline_price_is_the_snapshot_not_the_last_published_close(self):
        p1, p2, p3 = self._patches(self._SNAPSHOT)
        with p1, p2, p3:
            content, _ = await fetch_company_overview("600001.SS")

        assert "Current Price: CN¥104.00" in content
        assert "Current Price: CN¥100.00" not in content

    @pytest.mark.asyncio
    async def test_headline_price_falls_back_to_the_profile_without_a_snapshot(self):
        p1, p2, p3 = self._patches(None)
        with p1, p2, p3:
            content, _ = await fetch_company_overview("600001.SS")

        assert "Current Price: CN¥100.00" in content

    @pytest.mark.asyncio
    async def test_statement_tables_carry_the_reported_currency(self):
        p1, p2, p3 = self._patches(self._SNAPSHOT)
        with p1, p2, p3:
            content, _ = await fetch_company_overview("600001.SS")

        assert "| Q2 FY2026 | CN¥28.00B | -CN¥3.00B | CN¥25.00B |" in content
        assert "| 甲酒 | CN¥70.00B |" in content
        assert "- **Analyst Consensus:** CN¥165.50" in content
        assert "$" not in content.split("### Cash Flow (Quarterly)")[1]

    @pytest.mark.asyncio
    async def test_artifact_carries_the_english_name_alongside_the_local_one(self):
        p1, p2, p3 = self._patches(self._SNAPSHOT)
        with p1, p2, p3:
            _, artifact = await fetch_company_overview("600001.SS")

        assert artifact["name"] == "甲公司"
        assert artifact["nameEn"] == "Alpha Co., Ltd."
        assert artifact["currency"] == "CNY"


class TestOverviewOneArtifact:
    """The REST endpoint and the agent tool build the overview from one gather,
    so they cannot disagree on what a field means."""

    def _financial(self):
        # An HK listing whose income rows omit the reporting currency while
        # its cash-flow rows name it: the statements are in CNY, not HKD.
        return _make_fake_financial_source(
            profile_data=[{"companyName": "Beta Holdings", "marketCap": 4e12,
                           "price": 500.0, "currency": "HKD"}],
            income_stmt=[{"date": "2026-06-30", "period": "Q2", "fiscalYear": "2026",
                          "revenue": 180_000_000_000, "netIncome": 50_000_000_000}],
            earnings_calendar=[{"date": "2026-08-13", "fiscalDateEnding": "2026-06-30",
                                "epsActual": 5.2, "epsEstimated": 5.0,
                                "revenueActual": 180_000_000_000}],
            cash_flow=[{"date": "2026-06-30", "reportedCurrency": "CNY",
                        "operatingCashFlow": 70_000_000_000,
                        "capitalExpenditure": -20_000_000_000,
                        "freeCashFlow": 50_000_000_000}],
            ratios=[{"priceToEarningsRatioTTM": 22.0}],
            quote_data=[{"price": 500.0, "change": 5.0, "changePercentage": 1.0,
                         "yearHigh": 560.0, "yearLow": 380.0, "volume": 2e7}],
        )

    def _patches(self):
        mdp = AsyncMock()
        mdp.get_snapshots = AsyncMock(return_value=[])
        provider = _make_fake_financial_provider(financial=self._financial())
        return (
            patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_COMPANY_MOD}.get_market_data_provider", return_value=mdp),
            patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        )

    @staticmethod
    def _unstamped(artifact):
        # The freshness stamp is measured at each call's own instant.
        quote = {k: v for k, v in artifact["quote"].items() if k != "freshness"}
        return {**artifact, "quote": quote}

    @pytest.mark.asyncio
    async def test_rest_and_agent_artifacts_match(self):
        from src.tools.market_data.company import fetch_company_overview_data

        p1, p2, p3 = self._patches()
        with p1, p2, p3:
            _, agent = await fetch_company_overview("0001.HK")
            rest = await fetch_company_overview_data("0001.HK")

        assert self._unstamped(agent) == self._unstamped(rest)
        assert rest["currency"] == "HKD" and rest["reportedCurrency"] == "CNY"
        # A CN-style quote carries no P/E; the TTM ratio stands in.
        assert rest["quote"]["pe"] == 22.0

    @pytest.mark.asyncio
    async def test_agent_text_prints_earnings_in_the_listings_currency(self):
        # Earnings are consensus figures in the trading currency: an HK listing
        # reporting in CNY, like an ADR reporting in TWD, has them in its own.
        p1, p2, p3 = self._patches()
        with p1, p2, p3:
            content, _ = await fetch_company_overview("0001.HK")

        assert "- **EPS (consensus basis):** HK$5.20 actual vs HK$5.00 estimate" in content
        assert "- **Revenue:** HK$180.00B (no estimate available)" in content
        assert "Current Price: HK$500.00" in content


@pytest.mark.asyncio
class TestIndexOverviewOutage:
    def _down(self):
        provider = MagicMock()
        provider.get_snapshots = AsyncMock(side_effect=RuntimeError("upstream 503"))
        provider.get_daily_with_source = AsyncMock(side_effect=RuntimeError("upstream 503"))
        return patch(
            "src.tools.market_data.index_overview.get_market_data_provider",
            AsyncMock(return_value=provider),
        )

    async def test_the_agent_reads_an_outage_as_an_error_not_as_no_data(self):
        with self._down():
            content, artifact = await fetch_company_overview("^GSPC")

        assert "**Status:** Error" in content
        assert "No level data available." not in content
        assert artifact["error"]

    async def test_the_rest_artifact_raises_rather_than_answer_empty(self):
        from src.tools.market_data.company import fetch_company_overview_data

        with self._down(), pytest.raises(RuntimeError, match="upstream 503"):
            await fetch_company_overview_data("^GSPC")


_YEAR_OF_BARS = [{"date": "2026-01-02", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("symbol", "bars"),
    [("FAKE", []), ("^GSPC", []), ("^GSPC", _YEAR_OF_BARS)],
    ids=["company", "index", "index-year-range-only"],
)
async def test_an_overview_with_nothing_behind_it_is_unavailable(symbol, bars):
    """A company with no profile, or an index with no level (a year range
    alone included), is the instrument having nothing, not a source failing,
    so the run is not errored."""
    from src.observability.langsmith_tracer import NonRetainingTracer
    from src.tools.market_data.tool import get_company_overview

    market = MagicMock()
    market.get_snapshots = AsyncMock(return_value=[])
    market.get_daily_with_source = AsyncMock(return_value=(bars, "ginlix-data", None))
    provider = _make_fake_financial_provider(
        financial=_make_fake_financial_source(profile_data=[])
    )
    client = _RecordingClient()
    with (
        patch(f"{_COMPANY_MOD}.get_financial_data_provider", return_value=provider),
        patch(f"{_COMPANY_MOD}._fmp_request", return_value=[]),
        patch(
            "src.tools.market_data.index_overview.get_market_data_provider",
            AsyncMock(return_value=market),
        ),
    ):
        await get_company_overview.ainvoke(
            {"type": "tool_call", "name": "get_company_overview", "args": {"symbol": symbol}, "id": "tc-1"},
            {"callbacks": [NonRetainingTracer(project_name="proj", client=client)]},
        )

    (update,) = [u for u in client.updated if u["run_type"] == "tool"]
    metadata = update["extra"]["metadata"]
    assert (metadata["tool_outcome"], metadata["error_class"]) == ("unavailable", "no_data")
    assert update["error"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("now_et", "printed_et", "line"),
    [
        ((2026, 11, 4, 1, 0), (2026, 11, 3, 16, 4, 18), "| As of | 2026-11-03 16:04:18 ET, session close |"),
        ((2026, 11, 3, 16, 6), (2026, 11, 3, 15, 51, 31),
         "| As of | 2026-11-03 15:51:31 ET, delayed 15m, before the session close |"),
        ((2026, 11, 3, 11, 0), (2026, 11, 3, 10, 45), "| As of | 2026-11-03 10:45:00 ET, delayed 15m |"),
    ],
    ids=["after-the-close", "inside-the-delay-after-the-close", "in-session"],
)
async def test_an_index_level_says_when_it_printed_and_whether_it_is_the_close(now_et, printed_et, line):
    from market_protocol import AssetClass
    from src.tools.market_data.display import _symbol_currency
    from src.tools.market_data.index_overview import fetch_index_overview, index_overview_text

    snap = {
        "symbol": "GSPC", "price": 7568.92, "change": 27.52, "change_percent": 0.36,
        "as_of": int(_ET.localize(datetime(*printed_et)).timestamp() * 1000),
        "tier": "delayed_15m", "source": "ginlix-data",
    }
    market = MagicMock()
    market.get_snapshots = AsyncMock(return_value=[snap])
    market.get_daily_with_source = AsyncMock(return_value=([], "ginlix-data", None))
    ref = resolve_ref("^GSPC", AssetClass.INDEX)
    with patch("src.tools.market_data.index_overview.get_market_data_provider", AsyncMock(return_value=market)), \
         patch("src.data_client.freshness._now", side_effect=lambda now: now or _ET.localize(datetime(*now_et))):
        artifact = await fetch_index_overview("^GSPC", ref)
    assert line in index_overview_text(artifact, _symbol_currency(ref))


@pytest.mark.asyncio
async def test_an_index_level_with_no_print_time_names_none():
    from market_protocol import AssetClass
    from src.tools.market_data.index_overview import fetch_index_overview

    market = MagicMock()
    market.get_snapshots = AsyncMock(return_value=[{"symbol": "GSPC", "price": 7568.92}])
    market.get_daily_with_source = AsyncMock(return_value=([], "fmp", None))
    with patch("src.tools.market_data.index_overview.get_market_data_provider", AsyncMock(return_value=market)):
        artifact = await fetch_index_overview("^GSPC", resolve_ref("^GSPC", AssetClass.INDEX))
    assert artifact["quote"]["price"] == 7568.92
    assert "levelPrintedAt" not in artifact["quote"]
