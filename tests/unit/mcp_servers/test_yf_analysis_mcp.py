"""Tests for yf_analysis_mcp_server."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from mcp_servers.yf_analysis_mcp_server import (
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


# --- Fixtures ---


@pytest.fixture
def mock_ticker():
    with patch("mcp_servers.yf_analysis_mcp_server.yf.Ticker") as mock_cls:
        stock = MagicMock()
        mock_cls.return_value = stock
        yield stock


# --- Helper to build DataFrames ---


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
        assert result["data_type"] == "analyst_recommendations"
        assert result["ticker"] == "AAPL"
        assert len(result["data"]) == 1
        assert result["data"][0]["firm"] == "Firm A"

    def test_empty(self, mock_ticker):
        mock_ticker.recommendations = pd.DataFrame()
        result = get_analyst_recommendations("AAPL")
        assert result["data"] == []
        assert result["count"] == 0

    def test_none(self, mock_ticker):
        mock_ticker.recommendations = None
        result = get_analyst_recommendations("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        mock_ticker.recommendations = property(
            lambda self: (_ for _ in ()).throw(Exception("API error"))
        )
        type(mock_ticker).recommendations = property(
            lambda self: (_ for _ in ()).throw(Exception("API error"))
        )
        result = get_analyst_recommendations("AAPL")
        assert "error" in result


# --- get_sustainability_data ---


class TestGetSustainabilityData:
    def test_success(self, mock_ticker):
        mock_ticker.sustainability = pd.DataFrame(
            {"Value": [25.0, 10.0, 8.0]},
            index=["totalEsg", "environmentScore", "socialScore"],
        )
        result = get_sustainability_data("AAPL")
        assert result["data_type"] == "sustainability"
        assert result["data"]["totalEsg"] == 25.0
        assert result["data"]["environmentScore"] == 10.0

    def test_empty(self, mock_ticker):
        mock_ticker.sustainability = pd.DataFrame()
        result = get_sustainability_data("AAPL")
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
        assert "error" in result


# --- get_institutional_holders ---


class TestGetInstitutionalHolders:
    def test_success(self, mock_ticker):
        mock_ticker.institutional_holders = _df(
            [[datetime(2024, 3, 31), "Vanguard", 1200000, 250000000, 0.07]],
            columns=["Date Reported", "Holder", "Shares", "Value", "pctHeld"],
        )
        result = get_institutional_holders("AAPL")
        assert result["data_type"] == "institutional_holders"
        assert len(result["data"]) == 1
        assert result["data"][0]["holder"] == "Vanguard"
        assert result["data"][0]["shares"] == 1200000

    def test_empty(self, mock_ticker):
        mock_ticker.institutional_holders = pd.DataFrame()
        result = get_institutional_holders("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).institutional_holders = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_institutional_holders("AAPL")
        assert "error" in result


# --- get_mutualfund_holders ---


class TestGetMutualfundHolders:
    def test_success(self, mock_ticker):
        mock_ticker.mutualfund_holders = _df(
            [[datetime(2024, 3, 31), "Fidelity Fund", 500000, 100000000, 0.03]],
            columns=["Date Reported", "Holder", "Shares", "Value", "pctHeld"],
        )
        result = get_mutualfund_holders("AAPL")
        assert result["data_type"] == "mutualfund_holders"
        assert result["data"][0]["holder"] == "Fidelity Fund"

    def test_empty(self, mock_ticker):
        mock_ticker.mutualfund_holders = pd.DataFrame()
        result = get_mutualfund_holders("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).mutualfund_holders = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_mutualfund_holders("AAPL")
        assert "error" in result


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
        assert result["data_type"] == "insider_transactions"
        assert result["data"][0]["insider"] == "John Doe"
        assert result["data"][0]["transaction"] == "Sale"

    def test_empty(self, mock_ticker):
        mock_ticker.insider_transactions = pd.DataFrame()
        result = get_insider_transactions("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).insider_transactions = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_insider_transactions("AAPL")
        assert "error" in result


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
        assert result["data_type"] == "insider_roster"
        assert result["data"][0]["name"] == "Jane Smith"
        assert result["data"][0]["position"] == "CFO"

    def test_empty(self, mock_ticker):
        mock_ticker.insider_roster_holders = pd.DataFrame()
        result = get_insider_roster("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).insider_roster_holders = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_insider_roster("AAPL")
        assert "error" in result


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
        assert result["data_type"] == "news"
        assert result["count"] == 2
        assert result["data"][0]["title"] == "Apple announces new product"

    def test_empty(self, mock_ticker):
        mock_ticker.get_news.return_value = []
        result = get_news("AAPL")
        assert result["data"] == []
        assert result["count"] == 0

    def test_none(self, mock_ticker):
        mock_ticker.get_news.return_value = None
        result = get_news("AAPL")
        assert result["data"] == []

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

    def test_tab_parameter(self, mock_ticker):
        mock_ticker.get_news.return_value = []
        get_news("AAPL", count=5, tab="press releases")
        mock_ticker.get_news.assert_called_once_with(count=5, tab="press releases")

    def test_exception(self, mock_ticker):
        mock_ticker.get_news.side_effect = Exception("API error")
        result = get_news("AAPL")
        assert "error" in result


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
        assert result["data_type"] == "analyst_price_targets"
        assert result["data"]["current"] == 185.0
        assert result["data"]["high"] == 220.0

    def test_empty(self, mock_ticker):
        mock_ticker.analyst_price_targets = {}
        result = get_analyst_price_targets("AAPL")
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
        assert "error" in result


# --- get_upgrades_downgrades ---


class TestGetUpgradesDowngrades:
    def test_success(self, mock_ticker):
        mock_ticker.upgrades_downgrades = _df(
            [["Morgan Stanley", "Overweight", "Equal-Weight", "upgrade"]],
            columns=["Firm", "ToGrade", "FromGrade", "Action"],
            index=pd.DatetimeIndex(["2024-01-20"]),
        )
        result = get_upgrades_downgrades("AAPL")
        assert result["data_type"] == "upgrades_downgrades"
        assert result["data"][0]["firm"] == "Morgan Stanley"
        assert result["data"][0]["tograde"] == "Overweight"

    def test_empty(self, mock_ticker):
        mock_ticker.upgrades_downgrades = pd.DataFrame()
        result = get_upgrades_downgrades("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).upgrades_downgrades = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_upgrades_downgrades("AAPL")
        assert "error" in result


# --- get_earnings_history ---


class TestGetEarningsHistory:
    def test_success(self, mock_ticker):
        mock_ticker.earnings_history = _df(
            [[1.52, 1.46, 0.06, 4.1]],
            columns=["epsEstimate", "epsActual", "epsDifference", "surprisePercent"],
            index=pd.DatetimeIndex(["2024-01-25"]),
        )
        result = get_earnings_history("AAPL")
        assert result["data_type"] == "earnings_history"
        assert result["data"][0]["epsactual"] == 1.46

    def test_empty(self, mock_ticker):
        mock_ticker.earnings_history = pd.DataFrame()
        result = get_earnings_history("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).earnings_history = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_earnings_history("AAPL")
        assert "error" in result


# --- get_earnings_estimates ---


class TestGetEarningsEstimates:
    def test_success(self, mock_ticker):
        mock_ticker.earnings_estimate = _df(
            [[1.5, 1.2, 1.8, 5]],
            columns=["avg", "low", "high", "numberOfAnalysts"],
            index=["0q"],
        )
        result = get_earnings_estimates("AAPL")
        assert result["data_type"] == "earnings_estimates"
        assert len(result["data"]) == 1

    def test_empty(self, mock_ticker):
        mock_ticker.earnings_estimate = pd.DataFrame()
        result = get_earnings_estimates("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).earnings_estimate = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_earnings_estimates("AAPL")
        assert "error" in result


# --- get_revenue_estimates ---


class TestGetRevenueEstimates:
    def test_success(self, mock_ticker):
        mock_ticker.revenue_estimate = _df(
            [[90e9, 85e9, 95e9, 30]],
            columns=["avg", "low", "high", "numberOfAnalysts"],
            index=["0q"],
        )
        result = get_revenue_estimates("AAPL")
        assert result["data_type"] == "revenue_estimates"
        assert len(result["data"]) == 1

    def test_empty(self, mock_ticker):
        mock_ticker.revenue_estimate = pd.DataFrame()
        result = get_revenue_estimates("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).revenue_estimate = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_revenue_estimates("AAPL")
        assert "error" in result


# --- get_growth_estimates ---


class TestGetGrowthEstimates:
    def test_success(self, mock_ticker):
        mock_ticker.growth_estimates = _df(
            [[0.12, 0.08, 0.10, 0.09]],
            columns=["AAPL", "Industry", "Sector", "Index"],
            index=["0q"],
        )
        result = get_growth_estimates("AAPL")
        assert result["data_type"] == "growth_estimates"
        assert len(result["data"]) == 1

    def test_empty(self, mock_ticker):
        mock_ticker.growth_estimates = pd.DataFrame()
        result = get_growth_estimates("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).growth_estimates = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_growth_estimates("AAPL")
        assert "error" in result


# --- get_major_holders ---


class TestGetMajorHolders:
    def test_success(self, mock_ticker):
        mock_ticker.major_holders = _df(
            [["0.07%", "Insiders"], ["58.21%", "Institutions"]],
            columns=["Value", "Breakdown"],
        )
        result = get_major_holders("AAPL")
        assert result["data_type"] == "major_holders"
        assert len(result["data"]) == 2

    def test_empty(self, mock_ticker):
        mock_ticker.major_holders = pd.DataFrame()
        result = get_major_holders("AAPL")
        assert result["data"] == []

    def test_exception(self, mock_ticker):
        type(mock_ticker).major_holders = property(
            lambda self: (_ for _ in ()).throw(Exception("fail"))
        )
        result = get_major_holders("AAPL")
        assert "error" in result
