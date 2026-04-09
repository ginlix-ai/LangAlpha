"""
Unit tests for SharesightClient.

Uses httpx mock transport to avoid real network calls.
"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID, uuid5, NAMESPACE_URL

import httpx
import pytest

# ---------------------------------------------------------------------------
# Helpers — canned API responses
# ---------------------------------------------------------------------------

_TOKEN_RESPONSE = {
    "access_token": "test-access-token",
    "token_type": "Bearer",
    "expires_in": 1800,
}

_PORTFOLIOS_RESPONSE = {
    "portfolios": [
        {"id": 42, "name": "hunterbray"},
        {"id": 99, "name": "other"},
    ]
}

_HOLDINGS_RESPONSE = {
    "holdings": [
        {
            "id": 1001,
            "quantity": "100.0",
            "cost_base": "1500.00",
            "currency_code": "AUD",
            "instrument": {
                "code": "CBA",
                "market_code": "ASX",
                "instrument_type": "CommonStock",
            },
        },
        {
            "id": 1002,
            "quantity": "50.0",
            "cost_base": "5000.00",
            "currency_code": "USD",
            "instrument": {
                "code": "VGS",
                "market_code": "ASX",
                "instrument_type": "ExchangeTradedFund",
            },
        },
    ]
}


_DUMMY_REQUEST = httpx.Request("GET", "https://api.sharesight.com/")


def _make_response(status_code: int, body: dict) -> httpx.Response:
    """Build an httpx.Response with a dummy request attached (required for raise_for_status)."""
    response = httpx.Response(
        status_code=status_code,
        content=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
    )
    response.request = _DUMMY_REQUEST
    return response


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_http_client():
    """Return an AsyncMock that mimics httpx.AsyncClient."""
    client = AsyncMock(spec=httpx.AsyncClient)
    # Default: token succeeds, portfolios return hunterbray, holdings return two entries
    client.post.return_value = _make_response(200, _TOKEN_RESPONSE)
    client.get.side_effect = _portfolio_then_holdings_side_effect
    return client


def _portfolio_then_holdings_side_effect(url: str, **kwargs):
    if "portfolios.json" in url and "/42" not in url:
        return _make_response(200, _PORTFOLIOS_RESPONSE)
    elif "/42.json" in url:
        return _make_response(200, _HOLDINGS_RESPONSE)
    return _make_response(404, {"error": "not found"})


@pytest.fixture
def client_with_mock_http(mock_http_client):
    """SharesightClient pre-wired with the mock HTTP client."""
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(
        client_id="test-id",
        client_secret="test-secret",
        portfolio_name="hunterbray",
    )
    c._http = mock_http_client
    return c


# ---------------------------------------------------------------------------
# Token management tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_token_acquired_on_first_request(client_with_mock_http, mock_http_client):
    """_ensure_token should POST to /oauth2/token and store the token."""
    c = client_with_mock_http
    assert c._access_token is None

    await c._ensure_token()

    mock_http_client.post.assert_called_once()
    call_args = mock_http_client.post.call_args
    assert "token" in call_args[0][0]  # URL contains "token"
    assert c._access_token == "test-access-token"
    assert c._token_expires_at > time.time()


@pytest.mark.asyncio
async def test_token_reused_when_fresh(client_with_mock_http, mock_http_client):
    """_ensure_token should NOT re-POST if the token is still fresh."""
    c = client_with_mock_http
    c._access_token = "cached-token"
    c._token_expires_at = time.time() + 1000  # well within expiry

    await c._ensure_token()

    mock_http_client.post.assert_not_called()
    assert c._access_token == "cached-token"


@pytest.mark.asyncio
async def test_token_refreshed_when_near_expiry(client_with_mock_http, mock_http_client):
    """_ensure_token should refresh when within 5 minutes of expiry."""
    c = client_with_mock_http
    c._access_token = "old-token"
    c._token_expires_at = time.time() + 200  # under 5-minute (300s) threshold

    await c._ensure_token()

    mock_http_client.post.assert_called_once()
    assert c._access_token == "test-access-token"


@pytest.mark.asyncio
async def test_token_refreshed_when_expired(client_with_mock_http, mock_http_client):
    """_ensure_token should refresh when token is past expiry."""
    c = client_with_mock_http
    c._access_token = "expired-token"
    c._token_expires_at = time.time() - 60  # already expired

    await c._ensure_token()

    mock_http_client.post.assert_called_once()
    assert c._access_token == "test-access-token"


@pytest.mark.asyncio
async def test_auth_failure_raises_runtime_error(mock_http_client):
    """A non-200 token response should raise RuntimeError with 'authentication failed'."""
    from src.server.services.sharesight import SharesightClient

    mock_http_client.post.return_value = _make_response(401, {"error": "unauthorized"})
    c = SharesightClient(client_id="bad", client_secret="creds", portfolio_name="p")
    c._http = mock_http_client

    with pytest.raises(RuntimeError, match="authentication failed"):
        await c._ensure_token()


# ---------------------------------------------------------------------------
# Portfolio listing tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_portfolios_returns_list(client_with_mock_http):
    """get_portfolios() should return the list of portfolio dicts."""
    portfolios = await client_with_mock_http.get_portfolios()

    assert isinstance(portfolios, list)
    assert len(portfolios) == 2
    names = [p["name"] for p in portfolios]
    assert "hunterbray" in names
    assert "other" in names


# ---------------------------------------------------------------------------
# Holdings listing tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_holdings_returns_list(client_with_mock_http):
    """get_holdings(portfolio_id) should return the raw holdings list."""
    holdings = await client_with_mock_http.get_holdings(42)

    assert isinstance(holdings, list)
    assert len(holdings) == 2
    assert holdings[0]["instrument"]["code"] == "CBA"


# ---------------------------------------------------------------------------
# Portfolio not found test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_portfolio_not_found_raises_value_error(mock_http_client):
    """get_portfolio_holdings() should raise ValueError if portfolio name not found."""
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="id", client_secret="secret", portfolio_name="nonexistent")
    c._http = mock_http_client

    with pytest.raises(ValueError, match="nonexistent"):
        await c.get_portfolio_holdings()


# ---------------------------------------------------------------------------
# Holdings mapping tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_portfolio_holdings_maps_to_langalpha_schema(client_with_mock_http):
    """get_portfolio_holdings() should return list of dicts matching LangAlpha schema."""
    holdings = await client_with_mock_http.get_portfolio_holdings()

    assert len(holdings) == 2

    cba = next(h for h in holdings if h["symbol"] == "CBA")

    # Required fields present
    assert "user_portfolio_id" in cba
    assert cba["user_id"] == "sharesight"
    assert cba["symbol"] == "CBA"
    assert cba["instrument_type"] == "stock"
    assert cba["exchange"] == "ASX"
    assert cba["currency"] == "AUD"
    assert cba["account_name"] == "hunterbray"

    # Decimal types for numeric fields
    assert isinstance(cba["quantity"], Decimal)
    assert isinstance(cba["average_cost"], Decimal)
    assert cba["quantity"] == Decimal("100.0")
    assert cba["average_cost"] == Decimal("15.0")  # 1500 / 100

    # UUID type
    assert isinstance(cba["user_portfolio_id"], UUID)


@pytest.mark.asyncio
async def test_instrument_type_mapping_etf(client_with_mock_http):
    """ExchangeTradedFund should map to 'etf'."""
    holdings = await client_with_mock_http.get_portfolio_holdings()
    vgs = next(h for h in holdings if h["symbol"] == "VGS")
    assert vgs["instrument_type"] == "etf"


@pytest.mark.asyncio
async def test_average_cost_calculation(client_with_mock_http):
    """average_cost should be cost_base / quantity."""
    holdings = await client_with_mock_http.get_portfolio_holdings()
    vgs = next(h for h in holdings if h["symbol"] == "VGS")
    # 5000 / 50 = 100
    assert vgs["average_cost"] == Decimal("100.0")


@pytest.mark.asyncio
async def test_native_currency_used(client_with_mock_http):
    """currency should come from currency_code on the holding, not the instrument."""
    holdings = await client_with_mock_http.get_portfolio_holdings()
    cba = next(h for h in holdings if h["symbol"] == "CBA")
    assert cba["currency"] == "AUD"


# ---------------------------------------------------------------------------
# Stable UUID tests
# ---------------------------------------------------------------------------

def test_stable_uuid_same_holding_id_same_uuid():
    """_map_holding should produce the same UUID for the same holding ID every time."""
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="id", client_secret="secret", portfolio_name="p")
    holding = {
        "id": 9999,
        "quantity": "10",
        "cost_base": "100",
        "currency_code": "USD",
        "instrument": {"code": "AAPL", "market_code": "NASDAQ", "instrument_type": "CommonStock"},
    }

    result1 = c._map_holding(holding)
    result2 = c._map_holding(holding)

    assert result1["user_portfolio_id"] == result2["user_portfolio_id"]


def test_stable_uuid_different_holding_ids_different_uuids():
    """Different holding IDs should produce different UUIDs."""
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="id", client_secret="secret", portfolio_name="p")

    holding_a = {
        "id": 1,
        "quantity": "10",
        "cost_base": "100",
        "currency_code": "USD",
        "instrument": {"code": "AAPL", "market_code": "NASDAQ", "instrument_type": "CommonStock"},
    }
    holding_b = {**holding_a, "id": 2}

    assert c._map_holding(holding_a)["user_portfolio_id"] != c._map_holding(holding_b)["user_portfolio_id"]


def test_stable_uuid_matches_expected_value():
    """uuid5(NAMESPACE_URL, 'sharesight:1001') should produce a specific deterministic UUID."""
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="id", client_secret="secret", portfolio_name="p")
    holding = {
        "id": 1001,
        "quantity": "100",
        "cost_base": "1500",
        "currency_code": "AUD",
        "instrument": {"code": "CBA", "market_code": "ASX", "instrument_type": "CommonStock"},
    }

    result = c._map_holding(holding)
    expected = uuid5(NAMESPACE_URL, "sharesight:1001")
    assert result["user_portfolio_id"] == expected


# ---------------------------------------------------------------------------
# Edge case: zero quantity
# ---------------------------------------------------------------------------

def test_map_holding_zero_quantity_does_not_raise():
    """_map_holding with zero quantity should return Decimal(0) for average_cost."""
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="id", client_secret="secret", portfolio_name="p")
    holding = {
        "id": 5,
        "quantity": "0",
        "cost_base": "0",
        "currency_code": "USD",
        "instrument": {"code": "ZERO", "market_code": "NYSE", "instrument_type": "CommonStock"},
    }

    result = c._map_holding(holding)
    assert result["average_cost"] == Decimal(0)
    assert result["quantity"] == Decimal(0)


# ---------------------------------------------------------------------------
# Edge case: missing optional fields
# ---------------------------------------------------------------------------

def test_map_holding_missing_instrument_fields():
    """_map_holding should handle missing instrument sub-fields gracefully."""
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="id", client_secret="secret", portfolio_name="p")
    holding = {
        "id": 6,
        "quantity": "5",
        "cost_base": "50",
        "currency_code": "USD",
        "instrument": {},  # no code, market_code, or instrument_type
    }

    result = c._map_holding(holding)
    assert result["symbol"] == ""
    assert result["exchange"] is None
    assert result["instrument_type"] == "stock"  # default fallback


# ---------------------------------------------------------------------------
# Instrument type mapping coverage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sharesight_type,expected", [
    ("CommonStock", "stock"),
    ("ExchangeTradedFund", "etf"),
    ("MutualFund", "etf"),
    ("FixedInterest", "stock"),
    ("CryptoCurrency", "crypto"),
    ("Commodity", "commodity"),
    ("Currency", "currency"),
    ("Index", "index"),
    ("UnknownType", "stock"),  # fallback
])
def test_instrument_type_map(sharesight_type, expected):
    """All documented Sharesight types should map to the correct LangAlpha type."""
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="id", client_secret="secret", portfolio_name="p")
    holding = {
        "id": 7,
        "quantity": "1",
        "cost_base": "10",
        "currency_code": "USD",
        "instrument": {"code": "X", "market_code": "NYSE", "instrument_type": sharesight_type},
    }

    result = c._map_holding(holding)
    assert result["instrument_type"] == expected


# ---------------------------------------------------------------------------
# is_configured property
# ---------------------------------------------------------------------------

def test_is_configured_true_when_credentials_set():
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="id", client_secret="secret")
    assert c.is_configured is True


def test_is_configured_false_when_credentials_missing():
    from src.server.services.sharesight import SharesightClient

    c = SharesightClient(client_id="", client_secret="")
    assert c.is_configured is False


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

def test_module_singleton_exists():
    """Module should export a sharesight_client singleton."""
    from src.server.services import sharesight as mod

    assert hasattr(mod, "sharesight_client")
    from src.server.services.sharesight import SharesightClient
    assert isinstance(mod.sharesight_client, SharesightClient)
