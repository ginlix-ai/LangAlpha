"""Tests for yf_analysis_mcp_server — standard envelope + machine error codes."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from plugins.yfinance.yf_analysis_mcp_server import (
    get_analyst_price_targets,
    get_analyst_recommendations,
    get_earnings_estimates,
    get_earnings_history,
    get_growth_estimates,
    get_insider_roster,
    get_insider_transactions,
    get_institutional_holders,
    get_major_holders,
    get_mutualfund_holders,
    get_news,
    get_revenue_estimates,
    get_sustainability_data,
    get_upgrades_downgrades,
)

from .conftest import assert_error, assert_ok_envelope


# --- Fixtures ---


@pytest.fixture
def mock_ticker():
    with patch("plugins.yfinance.yf_analysis_mcp_server.yf.Ticker") as mock_cls:
        stock = MagicMock()
        mock_cls.return_value = stock
        yield stock


@pytest.fixture
def mock_search():
    """Yahoo search, the news fallback; empty unless a test sets ``.news``."""
    with patch("plugins.yfinance.yf_analysis_mcp_server.yf.Search") as mock_cls:
        mock_cls.return_value.news = []
        yield mock_cls


def _df(data, columns=None, index=None):
    return pd.DataFrame(data, columns=columns, index=index)


# --- get_analyst_recommendations ---


class TestGetAnalystRecommendations:
    def test_success(self, mock_ticker):
        mock_ticker.recommendations = _df(
            [["2024-01-15", "Firm A", "Buy", "Hold", "upgrade"]],
            columns=["Date", "Firm", "To Grade", "From Grade", "Action"],
        )
        result = get_analyst_recommendations("AAPL")
        assert_ok_envelope(result, source="yfinance", symbol="AAPL", count=1)
        assert result["data"][0]["firm"] == "Firm A"

    def test_empty(self, mock_ticker):
        mock_ticker.recommendations = pd.DataFrame()
        result = get_analyst_recommendations("AAPL")
        assert_ok_envelope(result, count=0)

    def test_none(self, mock_ticker):
        mock_ticker.recommendations = None
        result = get_analyst_recommendations("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).recommendations = property(
            lambda self: (_ for _ in ()).throw(Exception("API error"))
        )
        result = get_analyst_recommendations("AAPL")
        assert_error(result, "upstream_error", symbol="AAPL")


# --- get_sustainability_data ---


class TestGetSustainabilityData:
    def test_success(self, mock_ticker):
        mock_ticker.sustainability = pd.DataFrame(
            {"Value": [25.0, 10.0, 8.0]},
            index=["totalEsg", "environmentScore", "socialScore"],
        )
        result = get_sustainability_data("AAPL")
        assert_ok_envelope(result, symbol="AAPL", count=1)
        assert result["data"]["totalEsg"] == 25.0
        assert result["data"]["environmentScore"] == 10.0

    def test_empty(self, mock_ticker):
        mock_ticker.sustainability = pd.DataFrame()
        result = get_sustainability_data("AAPL")
        assert_ok_envelope(result, count=0)
        assert result["data"] == {}

    def test_none(self, mock_ticker):
        mock_ticker.sustainability = None
        result = get_sustainability_data("AAPL")
        assert result["data"] == {}

    def test_nan_values(self, mock_ticker):
        mock_ticker.sustainability = pd.DataFrame(
            {"Value": [25.0, float("nan")]},
            index=["totalEsg", "governanceScore"],
        )
        result = get_sustainability_data("AAPL")
        assert result["data"]["totalEsg"] == 25.0
        assert result["data"]["governanceScore"] is None

    def test_exception(self, mock_ticker):
        type(mock_ticker).sustainability = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_sustainability_data("AAPL")
        assert_error(result, "upstream_error")


# --- get_institutional_holders ---


class TestGetInstitutionalHolders:
    def test_success(self, mock_ticker):
        mock_ticker.institutional_holders = _df(
            [[datetime(2024, 3, 31), "Vanguard", 1200000, 250000000, 0.07]],
            columns=["Date Reported", "Holder", "Shares", "Value", "pctHeld"],
        )
        result = get_institutional_holders("AAPL")
        assert_ok_envelope(result, symbol="AAPL", count=1)
        assert result["data"][0]["holder"] == "Vanguard"
        assert result["data"][0]["shares"] == 1200000

    def test_empty(self, mock_ticker):
        mock_ticker.institutional_holders = pd.DataFrame()
        result = get_institutional_holders("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).institutional_holders = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_institutional_holders("AAPL")
        assert_error(result, "upstream_error")


# --- get_mutualfund_holders ---


class TestGetMutualfundHolders:
    def test_success(self, mock_ticker):
        mock_ticker.mutualfund_holders = _df(
            [[datetime(2024, 3, 31), "Fidelity Fund", 500000, 100000000, 0.03]],
            columns=["Date Reported", "Holder", "Shares", "Value", "pctHeld"],
        )
        result = get_mutualfund_holders("AAPL")
        assert_ok_envelope(result, symbol="AAPL")
        assert result["data"][0]["holder"] == "Fidelity Fund"

    def test_empty(self, mock_ticker):
        mock_ticker.mutualfund_holders = pd.DataFrame()
        result = get_mutualfund_holders("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).mutualfund_holders = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_mutualfund_holders("AAPL")
        assert_error(result, "upstream_error")


# --- get_insider_transactions ---


class TestGetInsiderTransactions:
    def test_success(self, mock_ticker):
        mock_ticker.insider_transactions = _df(
            [[
                "2024-01-10",
                "John Doe",
                "CEO",
                "http://example.com",
                "Sale",
                "Sale of shares",
                -5000,
                750000,
                "Direct",
            ]],
            columns=[
                "Start Date", "Insider", "Position", "URL",
                "Transaction", "Text", "Shares", "Value", "Ownership",
            ],
        )
        result = get_insider_transactions("AAPL")
        assert_ok_envelope(result, symbol="AAPL")
        assert result["data"][0]["insider"] == "John Doe"
        assert result["data"][0]["transaction"] == "Sale"

    def test_empty(self, mock_ticker):
        mock_ticker.insider_transactions = pd.DataFrame()
        result = get_insider_transactions("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).insider_transactions = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_insider_transactions("AAPL")
        assert_error(result, "upstream_error")


# --- get_insider_roster ---


class TestGetInsiderRoster:
    def test_success(self, mock_ticker):
        mock_ticker.insider_roster_holders = _df(
            [[
                "Jane Smith",
                "CFO",
                "http://example.com",
                "Sale",
                "2024-02-15",
                10000,
                "2024-01-01",
                5000,
                "2023-06-01",
            ]],
            columns=[
                "Name", "Position", "URL", "Most Recent Transaction",
                "Latest Transaction Date", "Shares Owned Directly",
                "Position Direct Date", "Shares Owned Indirectly",
                "Position Indirect Date",
            ],
        )
        result = get_insider_roster("AAPL")
        assert_ok_envelope(result, symbol="AAPL")
        assert result["data"][0]["name"] == "Jane Smith"
        assert result["data"][0]["position"] == "CFO"

    def test_empty(self, mock_ticker):
        mock_ticker.insider_roster_holders = pd.DataFrame()
        result = get_insider_roster("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).insider_roster_holders = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_insider_roster("AAPL")
        assert_error(result, "upstream_error")


# --- get_news ---


class TestGetNews:
    def test_success(self, mock_ticker):
        mock_ticker.get_news.return_value = [
            {
                "uuid": "abc123",
                "title": "Apple announces new product",
                "publisher": "Reuters",
                "link": "https://example.com/article",
                "providerPublishTime": 1700000000,
                "content": {"summary": "Apple announced..."},
            },
            {
                "uuid": "def456",
                "title": "AAPL earnings beat",
                "publisher": "Bloomberg",
                "link": "https://example.com/article2",
            },
        ]
        result = get_news("AAPL", count=5)
        assert_ok_envelope(result, symbol="AAPL", count=2)
        assert result["data"][0]["title"] == "Apple announces new product"

    def test_empty(self, mock_ticker, mock_search):
        mock_ticker.get_news.return_value = []
        result = get_news("AAPL")
        assert_ok_envelope(result, count=0)

    def test_none(self, mock_ticker, mock_search):
        mock_ticker.get_news.return_value = None
        result = get_news("AAPL")
        assert_ok_envelope(result, count=0)

    def test_with_datetime_objects(self, mock_ticker):
        mock_ticker.get_news.return_value = [
            {
                "title": "Test",
                "published_at": datetime(2024, 1, 15, 12, 0, 0),
                "nested": {"date": datetime(2024, 1, 16)},
            },
        ]
        result = get_news("AAPL")
        assert result["data"][0]["published_at"] == "2024-01-15 12:00:00"
        assert result["data"][0]["nested"]["date"] == "2024-01-16"

    def test_with_nan_values(self, mock_ticker):
        mock_ticker.get_news.return_value = [
            {"title": "Test", "score": float("nan")},
        ]
        result = get_news("AAPL")
        assert result["data"][0]["score"] is None

    def test_tab_parameter(self, mock_ticker, mock_search):
        mock_ticker.get_news.return_value = []
        get_news("AAPL", count=5, tab="press releases")
        mock_ticker.get_news.assert_called_once_with(count=5, tab="press releases")

    def test_empty_primary_falls_back_to_search_in_the_same_shape(
        self, mock_ticker, mock_search
    ):
        mock_ticker.get_news.return_value = []
        mock_search.return_value.news = [
            {
                "uuid": "abc",
                "title": "Headline",
                "publisher": "Wire",
                "link": "https://example.com/a",
                "providerPublishTime": 1700000000,
                "type": "STORY",
                "relatedTickers": ["AAPL", "MSFT"],
            }
        ]
        result = get_news("AAPL", count=5)
        assert_ok_envelope(result, symbol="AAPL", count=1)
        assert result["data"][0] == {
            "id": "abc",
            "relatedTickers": ["AAPL", "MSFT"],
            "content": {
                "title": "Headline",
                "contentType": "STORY",
                "pubDate": "2023-11-14T22:13:20+00:00",
                "provider": {"displayName": "Wire"},
                "canonicalUrl": {"url": "https://example.com/a"},
            },
        }

    def test_press_releases_never_fall_back(self, mock_ticker, mock_search):
        mock_ticker.get_news.return_value = []
        result = get_news("AAPL", tab="press releases")
        assert_ok_envelope(result, count=0)
        mock_search.assert_not_called()

    def test_a_failed_search_leaves_the_empty_result(self, mock_ticker, mock_search):
        mock_ticker.get_news.return_value = []
        mock_search.side_effect = Exception("down")
        assert_ok_envelope(get_news("AAPL"), count=0)

    def test_exception(self, mock_ticker, mock_search):
        mock_ticker.get_news.side_effect = Exception("API error")
        result = get_news("AAPL")
        assert_error(result, "upstream_error")

    def test_raising_primary_falls_back_to_search(self, mock_ticker, mock_search):
        mock_ticker.get_news.side_effect = Exception("withdrawn endpoint")
        mock_search.return_value.news = [
            {"uuid": "abc", "title": "Headline", "providerPublishTime": 1700000000}
        ]
        result = get_news("AAPL", count=5)
        assert_ok_envelope(result, symbol="AAPL", count=1)
        assert result["data"][0]["content"]["title"] == "Headline"

    def test_press_releases_error_is_not_masked_by_search(
        self, mock_ticker, mock_search
    ):
        mock_ticker.get_news.side_effect = Exception("withdrawn endpoint")
        assert_error(get_news("AAPL", tab="press releases"), "upstream_error")
        mock_search.assert_not_called()

    def test_unknown_tab_error_is_not_masked_by_search(self, mock_ticker, mock_search):
        mock_ticker.get_news.side_effect = ValueError("Invalid tab name")
        mock_search.return_value.news = [{"uuid": "abc", "title": "General headline"}]
        assert_error(get_news("AAPL", tab="press_releases"), "upstream_error")
        mock_search.assert_not_called()


# --- get_analyst_price_targets ---


class TestGetAnalystPriceTargets:
    def test_success(self, mock_ticker):
        mock_ticker.analyst_price_targets = {
            "current": 185.0,
            "low": 150.0,
            "high": 220.0,
            "mean": 195.0,
            "median": 192.0,
        }
        result = get_analyst_price_targets("AAPL")
        assert_ok_envelope(result, symbol="AAPL", count=1)
        # currency omitted: values are Yahoo-reported with no declared-currency
        # source on this path (avoids mislabeling minor-unit venues).
        assert "currency" not in result
        assert result["data"]["current"] == 185.0
        assert result["data"]["high"] == 220.0

    def test_empty(self, mock_ticker):
        mock_ticker.analyst_price_targets = {}
        result = get_analyst_price_targets("AAPL")
        assert_ok_envelope(result, count=0)
        assert result["data"] == {}

    def test_none(self, mock_ticker):
        mock_ticker.analyst_price_targets = None
        result = get_analyst_price_targets("AAPL")
        assert result["data"] == {}

    def test_exception(self, mock_ticker):
        type(mock_ticker).analyst_price_targets = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_analyst_price_targets("AAPL")
        assert_error(result, "upstream_error")


# --- get_upgrades_downgrades ---


class TestGetUpgradesDowngrades:
    def test_success(self, mock_ticker):
        mock_ticker.upgrades_downgrades = _df(
            [["Morgan Stanley", "Overweight", "Equal-Weight", "upgrade"]],
            columns=["Firm", "ToGrade", "FromGrade", "Action"],
            index=pd.DatetimeIndex(["2024-01-20"]),
        )
        result = get_upgrades_downgrades("AAPL")
        assert_ok_envelope(result, symbol="AAPL")
        assert result["data"][0]["firm"] == "Morgan Stanley"
        assert result["data"][0]["tograde"] == "Overweight"

    def test_empty(self, mock_ticker):
        mock_ticker.upgrades_downgrades = pd.DataFrame()
        result = get_upgrades_downgrades("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).upgrades_downgrades = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_upgrades_downgrades("AAPL")
        assert_error(result, "upstream_error")


# --- get_earnings_history ---


class TestGetEarningsHistory:
    def test_success(self, mock_ticker):
        mock_ticker.earnings_history = _df(
            [[1.52, 1.46, 0.06, 4.1]],
            columns=["epsEstimate", "epsActual", "epsDifference", "surprisePercent"],
            index=pd.DatetimeIndex(["2024-01-25"]),
        )
        result = get_earnings_history("AAPL")
        assert_ok_envelope(result, symbol="AAPL")
        assert result["data"][0]["epsactual"] == 1.46

    def test_empty(self, mock_ticker):
        mock_ticker.earnings_history = pd.DataFrame()
        result = get_earnings_history("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).earnings_history = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_earnings_history("AAPL")
        assert_error(result, "upstream_error")


# --- get_earnings_estimates ---


class TestGetEarningsEstimates:
    def test_success(self, mock_ticker):
        mock_ticker.earnings_estimate = _df(
            [[1.5, 1.2, 1.8, 5]],
            columns=["avg", "low", "high", "numberOfAnalysts"],
            index=["0q"],
        )
        result = get_earnings_estimates("AAPL")
        assert_ok_envelope(result, symbol="AAPL", count=1)

    def test_empty(self, mock_ticker):
        mock_ticker.earnings_estimate = pd.DataFrame()
        result = get_earnings_estimates("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).earnings_estimate = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_earnings_estimates("AAPL")
        assert_error(result, "upstream_error")


# --- get_revenue_estimates ---


class TestGetRevenueEstimates:
    def test_success(self, mock_ticker):
        mock_ticker.revenue_estimate = _df(
            [[90e9, 85e9, 95e9, 30]],
            columns=["avg", "low", "high", "numberOfAnalysts"],
            index=["0q"],
        )
        result = get_revenue_estimates("AAPL")
        assert_ok_envelope(result, symbol="AAPL", count=1)

    def test_empty(self, mock_ticker):
        mock_ticker.revenue_estimate = pd.DataFrame()
        result = get_revenue_estimates("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).revenue_estimate = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_revenue_estimates("AAPL")
        assert_error(result, "upstream_error")


# --- get_growth_estimates ---


class TestGetGrowthEstimates:
    def test_success(self, mock_ticker):
        mock_ticker.growth_estimates = _df(
            [[0.12, 0.08, 0.10, 0.09]],
            columns=["AAPL", "Industry", "Sector", "Index"],
            index=["0q"],
        )
        result = get_growth_estimates("AAPL")
        assert_ok_envelope(result, symbol="AAPL", count=1)

    def test_empty(self, mock_ticker):
        mock_ticker.growth_estimates = pd.DataFrame()
        result = get_growth_estimates("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).growth_estimates = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_growth_estimates("AAPL")
        assert_error(result, "upstream_error")


# --- get_major_holders ---


class TestGetMajorHolders:
    def test_success(self, mock_ticker):
        mock_ticker.major_holders = _df(
            [["0.07%", "Insiders"], ["58.21%", "Institutions"]],
            columns=["Value", "Breakdown"],
        )
        result = get_major_holders("AAPL")
        assert_ok_envelope(result, symbol="AAPL", count=2)

    def test_empty(self, mock_ticker):
        mock_ticker.major_holders = pd.DataFrame()
        result = get_major_holders("AAPL")
        assert_ok_envelope(result, count=0)

    def test_exception(self, mock_ticker):
        type(mock_ticker).major_holders = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_major_holders("AAPL")
        assert_error(result, "upstream_error")


# --- estimate periods after a report ---


class TestEstimatePeriods:
    @staticmethod
    def _stock(ends, reported="2026-09-30"):
        stock = MagicMock()
        stock._analysis._earnings_trend = {
            None: {"trend": [{"period": p, "endDate": e} for p, e in ends.items()]}
        }
        stock.earnings_history = _df(
            [[2.1]], columns=["epsActual"], index=pd.DatetimeIndex([reported])
        )
        stock.earnings_estimate = _df(
            [[2.9, 1.85, 0.57], [2.15, 2.01, 0.07], [9.4, 7.46, 0.27], [10.6, 8.8, 0.2]],
            columns=["avg", "yearAgoEps", "growth"],
            index=pd.Index(["0q", "+1q", "0y", "+1y"], name="period"),
        )
        return stock

    def test_trend_cached_as_a_list_by_older_yfinance(self, mock_ticker):
        stock = self._stock({"0q": "2026-09-30", "+1q": "2027-03-31", "0y": "2027-09-30", "+1y": "2027-09-30"})
        ends = stock._analysis._earnings_trend[None]["trend"]
        stock._analysis._earnings_trend = ends
        mock_ticker.configure_mock(**{k: getattr(stock, k) for k in ("_analysis", "earnings_history", "earnings_estimate")})
        rows = {r["period"]: r for r in get_earnings_estimates("AAPL")["data"]}

        assert rows["0q"]["year_ago_stale"] is True
        assert "year_ago_stale" not in rows["+1q"]

    def test_rolled_periods_lose_their_year_ago_figures(self, mock_ticker):
        stock = self._stock(
            {"0q": "2026-09-30", "+1q": "2027-03-31", "0y": "2027-09-30", "+1y": "2027-09-30"}
        )
        mock_ticker.configure_mock(**{k: getattr(stock, k) for k in ("_analysis", "earnings_history", "earnings_estimate")})
        rows = {r["period"]: r for r in get_earnings_estimates("AAPL")["data"]}

        assert rows["0q"]["period_end"] == "2026-09-30"
        for period in ("0q", "0y", "+1y"):
            assert rows[period]["year_ago_stale"] is True
            assert rows[period]["yearagoeps"] is None and rows[period]["growth"] is None
        assert rows["+1q"]["yearagoeps"] == 2.01
        assert "year_ago_stale" not in rows["+1q"]

    def test_calendar_year_company_keeps_its_fourth_quarter(self, mock_ticker):
        stock = self._stock(
            {"0q": "2026-09-30", "+1q": "2026-12-31", "0y": "2026-12-31", "+1y": "2027-12-31"},
            reported="2026-06-30",
        )
        mock_ticker.configure_mock(**{k: getattr(stock, k) for k in ("_analysis", "earnings_history", "earnings_estimate")})
        rows = get_earnings_estimates("0700.HK")["data"]

        assert not any("year_ago_stale" in r for r in rows)


class TestMajorHoldersKey:
    def test_rows_are_keyed_breakdown(self, mock_ticker):
        mock_ticker.major_holders = _df(
            [[0.016], [7724.0]],
            columns=["Value"],
            index=pd.Index(["insidersPercentHeld", "institutionsCount"]),
        )
        data = get_major_holders("AAPL")["data"]
        assert data[0] == {"breakdown": "insidersPercentHeld", "value": 0.016}


class TestGradeDatesAreUtc:
    def test_naive_grade_times_are_marked_utc(self, mock_ticker):
        mock_ticker.upgrades_downgrades = _df(
            [["JP Morgan", "Overweight"]],
            columns=["Firm", "ToGrade"],
            index=pd.DatetimeIndex(["2026-11-03 18:16:44"], name="GradeDate"),
        )
        result = get_upgrades_downgrades("AAPL")
        assert result["timezone"] == "UTC"
        assert result["data"][0]["gradedate"] == "2026-11-03 18:16:44"
