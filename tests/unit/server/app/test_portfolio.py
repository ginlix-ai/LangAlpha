"""
Tests for the Portfolio API router (src/server/app/portfolio.py).

Covers listing, adding, getting, updating, and deleting portfolio holdings.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

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
    user_id="test-user-123",
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
        "name": "Apple Inc.",
        "average_cost": Decimal("150.00"),
        "currency": "USD",
        "account_name": None,
        "notes": None,
        "metadata": {},
        "first_purchased_at": None,
        "created_at": NOW,
        "updated_at": NOW,
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


DB = "src.server.app.portfolio"


# ---------------------------------------------------------------------------
# GET /api/v1/users/me/portfolio
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_portfolio(client):
    h = _holding()
    with patch(
        f"{DB}.db_get_user_portfolio",
        new_callable=AsyncMock,
        return_value=[h],
    ):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["holdings"][0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_list_portfolio_empty(client):
    with patch(
        f"{DB}.db_get_user_portfolio",
        new_callable=AsyncMock,
        return_value=[],
    ):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 200
    assert resp.json()["total"] == 0


@pytest.mark.asyncio
async def test_list_portfolio_accepts_non_canonical_instrument_type(client):
    # Regression: a holding with an instrument_type outside the conventional
    # set (e.g. 'cash_management' written by the agent) used to 500 the
    # entire endpoint via Pydantic enum validation on the response model.
    h = _holding(symbol="VMFXX", instrument_type="cash_management")
    with patch(
        f"{DB}.db_get_user_portfolio",
        new_callable=AsyncMock,
        return_value=[h],
    ):
        resp = await client.get("/api/v1/users/me/portfolio")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["holdings"][0]["instrument_type"] == "cash_management"


@pytest.mark.asyncio
async def test_add_portfolio_holding_accepts_non_canonical_instrument_type(client):
    h = _holding(symbol="VMFXX", instrument_type="cash_management")
    upsert = AsyncMock(return_value=(h, None))
    with (
        patch(f"{DB}.db_upsert_portfolio_holding", upsert),
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.post(
            "/api/v1/users/me/portfolio",
            json={
                "symbol": "VMFXX",
                "instrument_type": "Cash_Management",  # mixed case → normalized
                "quantity": 1000,
            },
        )

    assert resp.status_code == 201
    assert resp.json()["instrument_type"] == "cash_management"
    assert upsert.call_args.kwargs["instrument_type"] == "cash_management"


# ---------------------------------------------------------------------------
# POST /api/v1/users/me/portfolio
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_portfolio_holding(client):
    h = _holding()
    with (
        patch(
            f"{DB}.db_upsert_portfolio_holding",
            new_callable=AsyncMock,
            return_value=(h, None),
        ),
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.post(
            "/api/v1/users/me/portfolio",
            json={
                "symbol": "AAPL",
                "instrument_type": "stock",
                "quantity": 10,
            },
        )

    assert resp.status_code == 201
    assert resp.json()["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_add_portfolio_holding_merge(client):
    h = _holding(quantity="30", average_cost=Decimal("193.33"))
    merge_details = {
        "previous": {"quantity": "10", "average_cost": "180.00"},
        "added": {"quantity": "20", "average_cost": "200.00"},
        "result": {"quantity": "30", "average_cost": "193.33"},
    }
    with (
        patch(
            f"{DB}.db_upsert_portfolio_holding",
            new_callable=AsyncMock,
            return_value=(h, merge_details),
        ),
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.post(
            "/api/v1/users/me/portfolio",
            json={
                "symbol": "AAPL",
                "instrument_type": "stock",
                "quantity": 20,
                "average_cost": 200.00,
            },
        )

    assert resp.status_code == 200
    assert resp.json()["quantity"] == "30"


@pytest.mark.asyncio
async def test_add_portfolio_holding_validation(client):
    """Missing required fields should return 422."""
    resp = await client.post(
        "/api/v1/users/me/portfolio",
        json={"symbol": "AAPL"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/v1/users/me/portfolio/{holding_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_portfolio_holding(client):
    h = _holding()
    with patch(
        f"{DB}.db_get_portfolio_holding",
        new_callable=AsyncMock,
        return_value=h,
    ):
        resp = await client.get(
            f"/api/v1/users/me/portfolio/{HOLDING_ID}"
        )

    assert resp.status_code == 200
    assert resp.json()["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_get_portfolio_holding_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{DB}.db_get_portfolio_holding",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.get(f"/api/v1/users/me/portfolio/{fake_id}")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# PUT /api/v1/users/me/portfolio/{holding_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_portfolio_holding(client):
    h = _holding()
    updated = {**h, "notes": "Long hold"}
    with patch(
        f"{DB}.db_update_portfolio_holding",
        new_callable=AsyncMock,
        return_value=updated,
    ):
        resp = await client.put(
            f"/api/v1/users/me/portfolio/{HOLDING_ID}",
            json={"notes": "Long hold"},
        )

    assert resp.status_code == 200
    assert resp.json()["notes"] == "Long hold"


@pytest.mark.asyncio
async def test_update_portfolio_holding_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{DB}.db_update_portfolio_holding",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.put(
            f"/api/v1/users/me/portfolio/{fake_id}",
            json={"notes": "X"},
        )

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /api/v1/users/me/portfolio/{holding_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_portfolio_holding(client):
    with patch(
        f"{DB}.db_delete_portfolio_holding",
        new_callable=AsyncMock,
        return_value=True,
    ):
        resp = await client.delete(
            f"/api/v1/users/me/portfolio/{HOLDING_ID}"
        )

    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_delete_portfolio_holding_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{DB}.db_delete_portfolio_holding",
        new_callable=AsyncMock,
        return_value=False,
    ):
        resp = await client.delete(f"/api/v1/users/me/portfolio/{fake_id}")

    assert resp.status_code == 404
