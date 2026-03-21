"""Tests for yf_market_mcp_server."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from mcp_servers.yf_market_mcp_server import (
    get_earnings_calendar,
    get_industry_info,
    get_market_status,
    get_predefined_screen,
    get_sector_info,
    screen_stocks,
    search_tickers,
)


class TestSearchTickers:
    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_basic_search(self, mock_yf):
        mock_search = MagicMock()
        mock_search.quotes = [
            {"symbol": "AAPL", "shortname": "Apple Inc."}
        ]
        mock_search.news = [
            {"title": "Apple news", "link": "https://example.com"}
        ]
        mock_yf.Search.return_value = mock_search

        result = search_tickers("apple")

        mock_yf.Search.assert_called_once_with(
            "apple", max_results=8, news_count=5
        )
        assert result["data_type"] == "search_results"
        assert result["source"] == "yfinance"
        assert result["data"]["quotes"] == [
            {"symbol": "AAPL", "shortname": "Apple Inc."}
        ]
        assert len(result["data"]["news"]) == 1

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_search_with_custom_params(self, mock_yf):
        mock_search = MagicMock()
        mock_search.quotes = []
        mock_search.news = []
        mock_yf.Search.return_value = mock_search

        search_tickers("xyz", max_results=3, news_count=0)

        mock_yf.Search.assert_called_once_with(
            "xyz", max_results=3, news_count=0
        )

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_search_error(self, mock_yf):
        mock_yf.Search.side_effect = Exception("network error")

        result = search_tickers("fail")

        assert "error" in result
        assert "network error" in result["error"]


class TestGetMarketStatus:
    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_us_market(self, mock_yf):
        mock_market = MagicMock()
        mock_market.status = "OPEN"
        mock_market.summary = {"regularMarketTime": 1234}
        mock_yf.Market.return_value = mock_market

        result = get_market_status("US")

        mock_yf.Market.assert_called_once_with("US")
        assert result["data_type"] == "market_status"
        assert result["data"]["status"] == "OPEN"
        assert result["market"] == "US"

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_default_market(self, mock_yf):
        mock_market = MagicMock()
        mock_market.status = "CLOSED"
        mock_market.summary = {}
        mock_yf.Market.return_value = mock_market

        result = get_market_status()

        mock_yf.Market.assert_called_once_with("US")

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_market_error(self, mock_yf):
        mock_yf.Market.side_effect = Exception("invalid market")

        result = get_market_status("INVALID")

        assert "error" in result


class TestScreenStocks:
    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_single_filter(self, mock_yf):
        mock_query = MagicMock()
        mock_yf.EquityQuery.return_value = mock_query
        mock_yf.screen.return_value = {
            "quotes": [{"symbol": "TSLA"}],
            "total": 1,
        }

        filters = [{"operator": "gt", "operands": ["percentchange", 3]}]
        result = screen_stocks(filters)

        mock_yf.EquityQuery.assert_called_once_with("GT", ["percentchange", 3])
        mock_yf.screen.assert_called_once_with(
            mock_query,
            sortField="percentchange",
            sortAsc=False,
            size=25,
        )
        assert result["data_type"] == "screen_results"

    @patch("mcp_servers.yf_market_mcp_server.yf")
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
            mock_query,
            sortField="price",
            sortAsc=True,
            size=10,
        )
        assert result["data_type"] == "screen_results"

    @patch("mcp_servers.yf_market_mcp_server.yf")
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

        assert result["data_type"] == "screen_results"

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_screen_error(self, mock_yf):
        mock_yf.EquityQuery.side_effect = Exception("bad filter")

        result = screen_stocks([{"operator": "gt", "operands": ["x", 1]}])

        assert "error" in result


class TestGetPredefinedScreen:
    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_valid_screen(self, mock_yf):
        mock_yf.PREDEFINED_SCREENER_QUERIES = {"day_gainers": MagicMock()}
        mock_yf.screen.return_value = {
            "quotes": [{"symbol": "NVDA"}],
            "total": 1,
        }

        result = get_predefined_screen("day_gainers")

        mock_yf.screen.assert_called_once_with("day_gainers")
        assert result["data_type"] == "predefined_screen"
        assert result["screen_name"] == "day_gainers"

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_invalid_screen(self, mock_yf):
        mock_yf.PREDEFINED_SCREENER_QUERIES = {"day_gainers": MagicMock()}

        result = get_predefined_screen("nonexistent")

        assert "error" in result
        assert "nonexistent" in result["error"]
        assert "day_gainers" in result["error"]

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_screen_error(self, mock_yf):
        mock_yf.PREDEFINED_SCREENER_QUERIES = {"day_gainers": MagicMock()}
        mock_yf.screen.side_effect = Exception("api error")

        result = get_predefined_screen("day_gainers")

        assert "error" in result


class TestGetEarningsCalendar:
    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_earnings_calendar(self, mock_yf):
        mock_cal = MagicMock()
        mock_cal.earnings_calendar = pd.DataFrame(
            {
                "Symbol": ["AAPL", "MSFT"],
                "Earnings Date": ["2026-01-15", "2026-01-16"],
            }
        )
        mock_yf.Calendars.return_value = mock_cal

        result = get_earnings_calendar("2026-01-01", "2026-01-31")

        mock_yf.Calendars.assert_called_once_with(
            start="2026-01-01", end="2026-01-31"
        )
        assert result["data_type"] == "earnings_calendar"
        assert result["start"] == "2026-01-01"
        assert result["end"] == "2026-01-31"
        assert result["count"] == 2
        assert result["data"][0]["symbol"] == "AAPL"

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_empty_calendar(self, mock_yf):
        mock_cal = MagicMock()
        mock_cal.earnings_calendar = pd.DataFrame()
        mock_yf.Calendars.return_value = mock_cal

        result = get_earnings_calendar("2026-06-01", "2026-06-02")

        assert result["data_type"] == "earnings_calendar"
        assert result["data"] == []

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_calendar_error(self, mock_yf):
        mock_yf.Calendars.side_effect = Exception("bad dates")

        result = get_earnings_calendar("invalid", "invalid")

        assert "error" in result


class TestGetSectorInfo:
    @patch("mcp_servers.yf_market_mcp_server.yf")
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
        assert result["data_type"] == "sector_info"
        assert result["sector"] == "technology"
        assert result["data"]["overview"]["name"] == "Technology"
        assert len(result["data"]["top_companies"]) == 2
        assert result["data"]["top_etfs"] == {"XLK": "Technology Select SPDR"}
        assert len(result["data"]["industries"]) == 2

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_sector_error(self, mock_yf):
        mock_yf.Sector.side_effect = Exception("invalid sector")

        result = get_sector_info("nonexistent")

        assert "error" in result


class TestGetIndustryInfo:
    @patch("mcp_servers.yf_market_mcp_server.yf")
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
        assert result["data_type"] == "industry_info"
        assert result["industry"] == "software-infrastructure"
        assert result["data"]["sector_key"] == "technology"
        assert result["data"]["sector_name"] == "Technology"
        assert len(result["data"]["top_performing_companies"]) == 1
        assert len(result["data"]["top_growth_companies"]) == 1

    @patch("mcp_servers.yf_market_mcp_server.yf")
    def test_industry_error(self, mock_yf):
        mock_yf.Industry.side_effect = Exception("invalid industry")

        result = get_industry_info("nonexistent")

        assert "error" in result
