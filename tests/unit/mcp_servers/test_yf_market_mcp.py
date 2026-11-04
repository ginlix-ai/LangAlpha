"""Tests for yf_market_mcp_server — standard envelope + machine error codes."""

from unittest.mock import MagicMock, patch

import pandas as pd

from plugins.yfinance.yf_market_mcp_server import (
    get_earnings_calendar,
    get_industry_info,
    get_market_status,
    get_predefined_screen,
    get_sector_info,
    screen_stocks,
    search_tickers,
)

from .conftest import assert_error, assert_ok_envelope


class TestSearchTickers:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_basic_search(self, mock_yf):
        mock_search = MagicMock()
        mock_search.quotes = [{"symbol": "AAPL", "shortname": "Apple Inc."}]
        mock_search.news = [{"title": "Apple news", "link": "https://example.com"}]
        mock_yf.Search.return_value = mock_search

        result = search_tickers("apple")

        mock_yf.Search.assert_called_once_with("apple", max_results=8, news_count=5)
        assert_ok_envelope(result, source="yfinance", count=2)  # quotes + news
        assert result["data"]["quotes"] == [
            {"symbol": "AAPL", "shortname": "Apple Inc."}
        ]
        assert len(result["data"]["news"]) == 1

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_search_with_custom_params(self, mock_yf):
        mock_search = MagicMock()
        mock_search.quotes = []
        mock_search.news = []
        mock_yf.Search.return_value = mock_search

        search_tickers("xyz", max_results=3, news_count=0)

        mock_yf.Search.assert_called_once_with("xyz", max_results=3, news_count=0)

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_search_error(self, mock_yf):
        mock_yf.Search.side_effect = Exception("network error")

        result = search_tickers("fail")

        assert_error(result, "upstream_error", detail_excludes=("network error",))


class TestGetMarketStatus:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_us_market(self, mock_yf):
        mock_market = MagicMock()
        mock_market.status = "OPEN"
        mock_market.summary = {"regularMarketTime": 1234}
        mock_yf.Market.return_value = mock_market

        result = get_market_status("US")

        mock_yf.Market.assert_called_once_with("US")
        assert_ok_envelope(result, source="yfinance", count=1)
        assert result["market"] == "US"
        assert result["data"]["status"] == "OPEN"

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_default_market(self, mock_yf):
        mock_market = MagicMock()
        mock_market.status = "CLOSED"
        mock_market.summary = {}
        mock_yf.Market.return_value = mock_market

        get_market_status()

        mock_yf.Market.assert_called_once_with("US")

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_market_error(self, mock_yf):
        mock_yf.Market.side_effect = Exception("invalid market")

        result = get_market_status("INVALID")

        assert_error(result, "upstream_error")
        assert result["market"] == "INVALID"


class TestScreenStocks:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_single_filter(self, mock_yf):
        mock_query = MagicMock()
        mock_yf.EquityQuery.return_value = mock_query
        mock_yf.screen.return_value = {"quotes": [{"symbol": "TSLA"}], "total": 1}

        filters = [{"operator": "gt", "operands": ["percentchange", 3]}]
        result = screen_stocks(filters)

        mock_yf.EquityQuery.assert_called_once_with("GT", ["percentchange", 3])
        mock_yf.screen.assert_called_once_with(
            mock_query, sortField="percentchange", sortAsc=False, size=25
        )
        assert_ok_envelope(result, source="yfinance", count=1)
        assert result["data"]["quotes"] == [{"symbol": "TSLA"}]

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_multiple_filters_wrapped_in_and(self, mock_yf):
        mock_query = MagicMock()
        mock_yf.EquityQuery.return_value = mock_query
        mock_yf.screen.return_value = {"quotes": [], "total": 0}

        filters = [
            {"operator": "gt", "operands": ["percentchange", 3]},
            {"operator": "lt", "operands": ["price", 100]},
        ]
        result = screen_stocks(filters, sort_field="price", sort_asc=True, count=10)

        mock_yf.screen.assert_called_once_with(
            mock_query, sortField="price", sortAsc=True, size=10
        )
        assert_ok_envelope(result, count=0)

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_nested_filters(self, mock_yf):
        mock_query = MagicMock()
        mock_yf.EquityQuery.return_value = mock_query
        mock_yf.screen.return_value = {"quotes": [], "total": 0}

        filters = [
            {
                "operator": "and",
                "operands": [
                    {"operator": "gt", "operands": ["percentchange", 3]},
                    {"operator": "lt", "operands": ["price", 50]},
                ],
            }
        ]
        result = screen_stocks(filters)

        assert_ok_envelope(result, source="yfinance")

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_screen_error(self, mock_yf):
        mock_yf.EquityQuery.side_effect = Exception("bad filter")

        result = screen_stocks([{"operator": "gt", "operands": ["x", 1]}])

        assert_error(result, "upstream_error")

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_invalid_field_surfaces_validation_message(self, mock_yf):
        # EquityQuery validation raises ValueError/TypeError with a local,
        # actionable message (no URLs) — surfaced as invalid_argument so the
        # caller can fix the filter instead of retrying upstream.
        mock_yf.EquityQuery.side_effect = ValueError(
            'Invalid field for EquityQuery "badfield"'
        )

        result = screen_stocks([{"operator": "gt", "operands": ["badfield", 1]}])

        assert_error(result, "invalid_argument", detail_contains="badfield")


class TestGetPredefinedScreen:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_valid_screen(self, mock_yf):
        mock_yf.PREDEFINED_SCREENER_QUERIES = {"day_gainers": MagicMock()}
        mock_yf.screen.return_value = {"quotes": [{"symbol": "NVDA"}], "total": 1}

        result = get_predefined_screen("day_gainers")

        mock_yf.screen.assert_called_once_with("day_gainers")
        assert_ok_envelope(result, count=1)
        assert result["screen_name"] == "day_gainers"

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_invalid_screen(self, mock_yf):
        mock_yf.PREDEFINED_SCREENER_QUERIES = {"day_gainers": MagicMock()}

        result = get_predefined_screen("nonexistent")

        assert_error(result, "invalid_argument")
        assert "day_gainers" in result["supported"]

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_screen_error(self, mock_yf):
        mock_yf.PREDEFINED_SCREENER_QUERIES = {"day_gainers": MagicMock()}
        mock_yf.screen.side_effect = Exception("api error")

        result = get_predefined_screen("day_gainers")

        assert_error(result, "upstream_error")


class TestGetEarningsCalendar:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_earnings_calendar(self, mock_yf):
        mock_cal = MagicMock()
        mock_cal.get_earnings_calendar.return_value = pd.DataFrame(
            {
                "Marketcap": [1.0e9, 3.0e12, 5.0e9],
                "Event Start Date": pd.to_datetime(
                    ["2026-01-15 21:00", "2026-01-16 13:30", "2026-02-01 00:00"],
                    utc=True,
                ),
            },
            index=pd.Index(["SMALL", "AAPL", "LATE"], name="Symbol"),
        )
        mock_yf.Calendars.return_value = mock_cal

        result = get_earnings_calendar("2026-01-01", "2026-01-31")

        mock_yf.Calendars.assert_called_once_with(start="2026-01-01", end="2026-02-01")
        mock_cal.get_earnings_calendar.assert_called_once_with(
            filter_most_active=False, limit=100, force=True
        )
        assert_ok_envelope(result, source="yfinance", count=2)
        assert result["start"] == "2026-01-01"
        assert result["end"] == "2026-01-31"
        assert "truncated" not in result
        assert result["timezone"] == "America/New_York"
        assert [row["symbol"] for row in result["data"]] == ["AAPL", "SMALL"]
        assert result["data"][0]["event_start_date"] == "2026-01-16 08:30:00"

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_full_page_is_reported_as_truncated(self, mock_yf):
        mock_cal = MagicMock()
        mock_cal.get_earnings_calendar.return_value = pd.DataFrame(
            {
                "Marketcap": [float(i) for i in range(100)],
                "Event Start Date": pd.to_datetime(["2026-01-15 21:00"] * 100, utc=True),
            },
            index=pd.Index([f"T{i}" for i in range(100)], name="Symbol"),
        )
        mock_yf.Calendars.return_value = mock_cal

        result = get_earnings_calendar("2026-01-15", "2026-01-15")

        assert result["truncated"] is True

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_empty_calendar(self, mock_yf):
        mock_cal = MagicMock()
        mock_cal.get_earnings_calendar.return_value = pd.DataFrame(
            {"Marketcap": [], "Event Start Date": []}
        )
        mock_yf.Calendars.return_value = mock_cal

        result = get_earnings_calendar("2026-06-01", "2026-06-02")

        assert_ok_envelope(result, count=0)
        assert result["timezone"] == "America/New_York"

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_calendar_error(self, mock_yf):
        mock_yf.Calendars.side_effect = Exception("down")

        result = get_earnings_calendar("2026-06-01", "2026-06-02")

        assert_error(result, "upstream_error")

    def test_malformed_dates_are_invalid_arguments(self):
        assert_error(get_earnings_calendar("invalid", "2026-06-02"), "invalid_argument")


class TestGetSectorInfo:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_sector_info(self, mock_yf):
        mock_sector = MagicMock()
        mock_sector.overview = {"name": "Technology", "market_cap": 1e12}
        mock_sector.top_companies = pd.DataFrame(
            {"Symbol": ["AAPL", "MSFT"], "Market Cap": [3e12, 2.8e12]}
        )
        mock_sector.top_etfs = {"XLK": "Technology Select SPDR"}
        mock_sector.industries = pd.DataFrame(
            {"Name": ["Software", "Hardware"], "Count": [100, 50]}
        )
        mock_yf.Sector.return_value = mock_sector

        result = get_sector_info("technology")

        mock_yf.Sector.assert_called_once_with("technology")
        assert_ok_envelope(result, count=4)  # top_companies + industries
        assert result["sector"] == "technology"
        assert result["data"]["overview"]["name"] == "Technology"
        assert len(result["data"]["top_companies"]) == 2
        assert result["data"]["top_etfs"] == {"XLK": "Technology Select SPDR"}
        assert len(result["data"]["industries"]) == 2

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_sector_error(self, mock_yf):
        mock_yf.Sector.side_effect = Exception("invalid sector")

        result = get_sector_info("nonexistent")

        assert_error(result, "upstream_error")
        assert result["sector"] == "nonexistent"


class TestGetIndustryInfo:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_industry_info(self, mock_yf):
        mock_industry = MagicMock()
        mock_industry.overview = {"name": "Software - Infrastructure"}
        mock_industry.top_performing_companies = pd.DataFrame(
            {"Symbol": ["CRM"], "Return": [0.25]}
        )
        mock_industry.top_growth_companies = pd.DataFrame(
            {"Symbol": ["SNOW"], "Growth": [0.4]}
        )
        mock_industry.sector_key = "technology"
        mock_industry.sector_name = "Technology"
        mock_yf.Industry.return_value = mock_industry

        result = get_industry_info("software-infrastructure")

        mock_yf.Industry.assert_called_once_with("software-infrastructure")
        assert_ok_envelope(result, count=2)  # top_performing + top_growth
        assert result["industry"] == "software-infrastructure"
        assert result["data"]["sector_key"] == "technology"
        assert result["data"]["sector_name"] == "Technology"
        assert len(result["data"]["top_performing_companies"]) == 1
        assert len(result["data"]["top_growth_companies"]) == 1

    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_industry_error(self, mock_yf):
        mock_yf.Industry.side_effect = Exception("invalid industry")

        result = get_industry_info("nonexistent")

        assert_error(result, "upstream_error")
        assert result["industry"] == "nonexistent"


class TestScreenStaleness:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_quotes_that_did_not_trade_in_the_session_are_flagged(self, mock_yf):
        today = 1_793_739_602
        mock_yf.screen.return_value = {
            "quotes": [
                {"symbol": "OTCX", "regularMarketTime": today - 30 * 3600},
                {"symbol": "EARLY", "regularMarketTime": today - 7 * 3600},
                {"symbol": "NYSEX", "regularMarketTime": today},
                {"symbol": "NOTIME"},
            ]
        }

        result = screen_stocks([{"operator": "gt", "operands": ["percentchange", 3]}])

        flags = {q["symbol"]: q.get("stale_price") for q in result["data"]["quotes"]}
        assert flags == {"OTCX": True, "EARLY": None, "NYSEX": None, "NOTIME": None}
        assert all("stale_price" not in q for q in result["data"]["quotes"][1:])


class TestMarketStatusTimes:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_session_times_are_exchange_local(self, mock_yf):
        from datetime import datetime, timezone

        market = MagicMock()
        market.status = {
            "open": datetime(2026, 11, 3, 14, 30, tzinfo=timezone.utc),
            "close": datetime(2026, 11, 3, 21, 0, tzinfo=timezone.utc),
            "timezone": {"short": "EST", "$text": "America/New_York"},
        }
        market.summary = {}
        mock_yf.Market.return_value = market

        result = get_market_status("US")

        assert result["timezone"] == "America/New_York"
        assert result["data"]["status"]["open"] == "2026-11-03 09:30:00"
        assert result["data"]["status"]["close"] == "2026-11-03 16:00:00"


class TestCalendarTruncation:
    @patch("plugins.yfinance.yf_market_mcp_server.yf")
    def test_rows_filtered_after_a_full_page_still_report_truncated(self, mock_yf):
        cal = MagicMock()
        stamps = ["2026-01-15 21:00"] * 99 + ["2026-01-16 00:00"]
        cal.get_earnings_calendar.return_value = pd.DataFrame(
            {
                "Marketcap": [float(i) for i in range(100)],
                "Event Start Date": pd.to_datetime(stamps, utc=True),
            },
            index=pd.Index([f"T{i}" for i in range(100)], name="Symbol"),
        )
        mock_yf.Calendars.return_value = cal

        result = get_earnings_calendar("2026-01-15", "2026-01-15")

        assert result["count"] == 99
        assert result["truncated"] is True
