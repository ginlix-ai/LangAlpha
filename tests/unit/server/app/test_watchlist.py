"""
Tests for the Watchlist API router (src/server/app/watchlist.py).

Covers watchlist CRUD and watchlist-item CRUD, including the special
"default" watchlist_id resolution.
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = datetime.now(timezone.utc)
WL_ID = str(uuid.uuid4())
ITEM_ID = str(uuid.uuid4())


def _watchlist(
    watchlist_id=None,
    user_id="test-user-123",
    name="My Watchlist",
    is_default=False,
    display_order=0,
):
    return {
        "watchlist_id": watchlist_id or WL_ID,
        "user_id": user_id,
        "name": name,
        "description": None,
        "is_default": is_default,
        "display_order": display_order,
        "created_at": NOW,
        "updated_at": NOW,
    }


def _item(
    watchlist_item_id=None,
    watchlist_id=None,
    user_id="test-user-123",
    symbol="AAPL",
    instrument_type="stock",
):
    return {
        "watchlist_item_id": watchlist_item_id or ITEM_ID,
        "watchlist_id": watchlist_id or WL_ID,
        "user_id": user_id,
        "symbol": symbol,
        "instrument_type": instrument_type,
        "exchange": "NASDAQ",
        "name": "Apple Inc.",
        "notes": None,
        "alert_settings": {},
        "metadata": {},
        "created_at": NOW,
        "updated_at": NOW,
    }


@pytest_asyncio.fixture
async def client():
    from src.server.app.watchlist import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


# Module path prefix for patching
DB = "src.server.app.watchlist"


# ---------------------------------------------------------------------------
# Watchlist CRUD
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_watchlists(client):
    wl = _watchlist()
    with patch(
        f"{DB}.db_get_user_watchlists",
        new_callable=AsyncMock,
        return_value=[wl],
    ):
        resp = await client.get("/api/v1/users/me/watchlists")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["watchlists"][0]["name"] == "My Watchlist"


@pytest.mark.asyncio
async def test_create_watchlist(client):
    wl = _watchlist(name="New WL")
    with patch(
        f"{DB}.db_create_watchlist",
        new_callable=AsyncMock,
        return_value=wl,
    ):
        resp = await client.post(
            "/api/v1/users/me/watchlists",
            json={"name": "New WL"},
        )

    assert resp.status_code == 201
    assert resp.json()["name"] == "New WL"


@pytest.mark.asyncio
async def test_create_watchlist_duplicate_409(client):
    with patch(
        f"{DB}.db_create_watchlist",
        new_callable=AsyncMock,
        side_effect=ValueError("duplicate name"),
    ):
        resp = await client.post(
            "/api/v1/users/me/watchlists",
            json={"name": "Dup"},
        )

    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_get_watchlist_with_items(client):
    wl = _watchlist()
    items = [_item()]
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist",
            new_callable=AsyncMock,
            return_value=wl,
        ),
        patch(
            f"{DB}.db_get_watchlist_items",
            new_callable=AsyncMock,
            return_value=items,
        ),
    ):
        resp = await client.get(f"/api/v1/users/me/watchlists/{WL_ID}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_get_watchlist_default_alias(client):
    """Using 'default' should resolve via db_get_or_create_default_watchlist."""
    wl = _watchlist(is_default=True)
    with (
        patch(
            f"{DB}.db_get_or_create_default_watchlist",
            new_callable=AsyncMock,
            return_value=wl,
        ),
        patch(
            f"{DB}.db_get_watchlist",
            new_callable=AsyncMock,
            return_value=wl,
        ),
        patch(
            f"{DB}.db_get_watchlist_items",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        resp = await client.get("/api/v1/users/me/watchlists/default")

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_get_watchlist_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{DB}.db_get_watchlist",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.get(f"/api/v1/users/me/watchlists/{fake_id}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_watchlist(client):
    wl = _watchlist()
    updated = {**wl, "name": "Renamed"}
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_update_watchlist",
            new_callable=AsyncMock,
            return_value=updated,
        ),
    ):
        resp = await client.put(
            f"/api/v1/users/me/watchlists/{WL_ID}",
            json={"name": "Renamed"},
        )

    assert resp.status_code == 200
    assert resp.json()["name"] == "Renamed"


@pytest.mark.asyncio
async def test_update_watchlist_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{DB}.db_update_watchlist",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.put(
            f"/api/v1/users/me/watchlists/{fake_id}",
            json={"name": "X"},
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_watchlist(client):
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_delete_watchlist",
            new_callable=AsyncMock,
            return_value=True,
        ),
    ):
        resp = await client.delete(f"/api/v1/users/me/watchlists/{WL_ID}")

    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_delete_watchlist_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{DB}.db_delete_watchlist",
        new_callable=AsyncMock,
        return_value=False,
    ):
        resp = await client.delete(f"/api/v1/users/me/watchlists/{fake_id}")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Watchlist Items
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_watchlist_items(client):
    wl = _watchlist()
    items = [_item()]
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist",
            new_callable=AsyncMock,
            return_value=wl,
        ),
        patch(
            f"{DB}.db_get_watchlist_items",
            new_callable=AsyncMock,
            return_value=items,
        ),
    ):
        resp = await client.get(f"/api/v1/users/me/watchlists/{WL_ID}/items")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1


@pytest.mark.asyncio
async def test_list_watchlist_items_watchlist_not_found(client):
    fake_id = str(uuid.uuid4())
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await client.get(
            f"/api/v1/users/me/watchlists/{fake_id}/items"
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_add_watchlist_item(client):
    item = _item()
    with (
        patch(
            f"{DB}.db_get_or_create_default_watchlist",
            new_callable=AsyncMock,
        ),
        patch(
            f"{DB}.db_create_watchlist_item",
            new_callable=AsyncMock,
            return_value=item,
        ),
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.post(
            f"/api/v1/users/me/watchlists/{WL_ID}/items",
            json={
                "symbol": "AAPL",
                "instrument_type": "stock",
            },
        )

    assert resp.status_code == 201
    assert resp.json()["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_list_watchlist_items_accepts_non_canonical_instrument_type(client):
    # Regression companion to the portfolio version: the watchlist read
    # path previously enforced the same closed enum on instrument_type
    # and 500'd on agent-written rows with non-canonical values.
    wl = _watchlist()
    items = [_item(symbol="VMFXX", instrument_type="cash_management")]
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(f"{DB}.db_get_watchlist", new_callable=AsyncMock, return_value=wl),
        patch(
            f"{DB}.db_get_watchlist_items",
            new_callable=AsyncMock,
            return_value=items,
        ),
    ):
        resp = await client.get(f"/api/v1/users/me/watchlists/{WL_ID}/items")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["instrument_type"] == "cash_management"


@pytest.mark.asyncio
async def test_add_watchlist_item_accepts_non_canonical_instrument_type(client):
    item = _item(symbol="VMFXX", instrument_type="cash_management")
    create = AsyncMock(return_value=item)
    with (
        patch(
            f"{DB}.db_get_or_create_default_watchlist",
            new_callable=AsyncMock,
        ),
        patch(f"{DB}.db_create_watchlist_item", create),
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.post(
            f"/api/v1/users/me/watchlists/{WL_ID}/items",
            json={
                "symbol": "VMFXX",
                "instrument_type": "Cash_Management",  # mixed case → normalized
            },
        )

    assert resp.status_code == 201
    assert resp.json()["instrument_type"] == "cash_management"
    assert create.call_args.kwargs["instrument_type"] == "cash_management"


@pytest.mark.asyncio
async def test_add_watchlist_item_duplicate_409(client):
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_create_watchlist_item",
            new_callable=AsyncMock,
            side_effect=ValueError("already exists"),
        ),
    ):
        resp = await client.post(
            f"/api/v1/users/me/watchlists/{WL_ID}/items",
            json={"symbol": "AAPL", "instrument_type": "stock"},
        )

    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_add_watchlist_item_watchlist_not_found_404(client):
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_create_watchlist_item",
            new_callable=AsyncMock,
            side_effect=ValueError("Watchlist not found"),
        ),
    ):
        resp = await client.post(
            f"/api/v1/users/me/watchlists/{WL_ID}/items",
            json={"symbol": "AAPL", "instrument_type": "stock"},
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_watchlist_item(client):
    item = _item()
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist_item",
            new_callable=AsyncMock,
            return_value=item,
        ),
    ):
        resp = await client.get(
            f"/api/v1/users/me/watchlists/{WL_ID}/items/{ITEM_ID}"
        )

    assert resp.status_code == 200
    assert resp.json()["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_get_watchlist_item_not_found(client):
    fake_item = str(uuid.uuid4())
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist_item",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await client.get(
            f"/api/v1/users/me/watchlists/{WL_ID}/items/{fake_item}"
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_watchlist_item_wrong_watchlist(client):
    other_wl = str(uuid.uuid4())
    item = _item(watchlist_id=other_wl)
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist_item",
            new_callable=AsyncMock,
            return_value=item,
        ),
    ):
        resp = await client.get(
            f"/api/v1/users/me/watchlists/{WL_ID}/items/{ITEM_ID}"
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_watchlist_item(client):
    item = _item()
    updated = {**item, "notes": "Great stock"}
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist_item",
            new_callable=AsyncMock,
            return_value=item,
        ),
        patch(
            f"{DB}.db_update_watchlist_item",
            new_callable=AsyncMock,
            return_value=updated,
        ),
    ):
        resp = await client.put(
            f"/api/v1/users/me/watchlists/{WL_ID}/items/{ITEM_ID}",
            json={"notes": "Great stock"},
        )

    assert resp.status_code == 200
    assert resp.json()["notes"] == "Great stock"


@pytest.mark.asyncio
async def test_update_watchlist_item_not_found(client):
    fake_item = str(uuid.uuid4())
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist_item",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await client.put(
            f"/api/v1/users/me/watchlists/{WL_ID}/items/{fake_item}",
            json={"notes": "X"},
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_watchlist_item(client):
    item = _item()
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist_item",
            new_callable=AsyncMock,
            return_value=item,
        ),
        patch(
            f"{DB}.db_delete_watchlist_item",
            new_callable=AsyncMock,
            return_value=True,
        ),
    ):
        resp = await client.delete(
            f"/api/v1/users/me/watchlists/{WL_ID}/items/{ITEM_ID}"
        )

    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_delete_watchlist_item_not_found(client):
    fake_item = str(uuid.uuid4())
    with (
        patch(f"{DB}.db_get_or_create_default_watchlist", new_callable=AsyncMock),
        patch(
            f"{DB}.db_get_watchlist_item",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await client.delete(
            f"/api/v1/users/me/watchlists/{WL_ID}/items/{fake_item}"
        )

    assert resp.status_code == 404
