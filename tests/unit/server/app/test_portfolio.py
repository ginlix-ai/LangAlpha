"""
Tests for the Portfolio API router (src/server/app/portfolio.py).

Covers the Sharesight-proxy GET list endpoint.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = datetime.now(timezone.utc)
HOLDING_ID = str(uuid.uuid4())


def _holding(
    user_portfolio_id=None,
    user_id="sharesight",
    symbol="AAPL",
    instrument_type="stock",
    quantity="10.0",
    **overrides,
):
    data = {
        "user_portfolio_id": user_portfolio_id or HOLDING_ID,
        "user_id": user_id,
        "symbol": symbol,
        "instrument_type": instrument_type,
        "quantity": Decimal(quantity),
        "exchange": "NASDAQ",
        "name": None,
        "average_cost": Decimal("150.00"),
        "currency": "USD",
        "account_name": "hunterbray",
        "notes": None,
        "metadata": {},
        "first_purchased_at": None,
        "created_at": "2000-01-01T00:00:00Z",
        "updated_at": "2000-01-01T00:00:00Z",
    }
    data.update(overrides)
    return data


@pytest_asyncio.fixture
async def client():
    from src.server.app.portfolio import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# GET /api/v1/users/me/portfolio
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_portfolio(client):
    h = _holding()
    configured_client = MagicMock()
    configured_client.is_configured = True
    configured_client.get_portfolio_holdings = AsyncMock(return_value=[h])

    with patch("src.server.app.portfolio.sharesight_client", configured_client):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["holdings"][0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_list_portfolio_empty(client):
    configured_client = MagicMock()
    configured_client.is_configured = True
    configured_client.get_portfolio_holdings = AsyncMock(return_value=[])

    with patch("src.server.app.portfolio.sharesight_client", configured_client):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 200
    assert resp.json()["total"] == 0


@pytest.mark.asyncio
async def test_list_portfolio_not_configured(client):
    """Returns 503 when Sharesight credentials are not set."""
    unconfigured_client = MagicMock()
    unconfigured_client.is_configured = False

    with patch("src.server.app.portfolio.sharesight_client", unconfigured_client):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 503
    assert "not configured" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_list_portfolio_portfolio_not_found(client):
    """Returns 404 when the named portfolio does not exist in Sharesight."""
    configured_client = MagicMock()
    configured_client.is_configured = True
    configured_client.get_portfolio_holdings = AsyncMock(
        side_effect=ValueError("Portfolio 'hunterbray' not found")
    )

    with patch("src.server.app.portfolio.sharesight_client", configured_client):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_list_portfolio_auth_failure(client):
    """Returns 502 when Sharesight authentication fails."""
    configured_client = MagicMock()
    configured_client.is_configured = True
    configured_client.get_portfolio_holdings = AsyncMock(
        side_effect=RuntimeError("Sharesight authentication failed (HTTP 401): Unauthorized")
    )

    with patch("src.server.app.portfolio.sharesight_client", configured_client):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 502
    assert "authentication failed" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_list_portfolio_runtime_error(client):
    """Returns 502 when Sharesight is unreachable."""
    configured_client = MagicMock()
    configured_client.is_configured = True
    configured_client.get_portfolio_holdings = AsyncMock(
        side_effect=RuntimeError("Connection refused")
    )

    with patch("src.server.app.portfolio.sharesight_client", configured_client):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 502
    assert "sharesight" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_list_portfolio_unexpected_error(client):
    """Returns 502 on any unexpected exception."""
    configured_client = MagicMock()
    configured_client.is_configured = True
    configured_client.get_portfolio_holdings = AsyncMock(
        side_effect=Exception("Unexpected failure")
    )

    with patch("src.server.app.portfolio.sharesight_client", configured_client):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 502


@pytest.mark.asyncio
async def test_list_portfolio_multiple_holdings(client):
    """Returns all holdings from Sharesight with correct total."""
    holdings = [
        _holding(symbol="AAPL"),
        _holding(symbol="MSFT", quantity="5.0"),
        _holding(symbol="GOOG", quantity="2.0"),
    ]
    configured_client = MagicMock()
    configured_client.is_configured = True
    configured_client.get_portfolio_holdings = AsyncMock(return_value=holdings)

    with patch("src.server.app.portfolio.sharesight_client", configured_client):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    symbols = [h["symbol"] for h in body["holdings"]]
    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert "GOOG" in symbols
