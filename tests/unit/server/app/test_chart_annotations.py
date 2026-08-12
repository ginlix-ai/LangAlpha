"""Tests for the Chart Annotations API router (src/server/app/chart_annotations.py).

The router is read / bulk-delete only (writes go through the agent tool). These
tests cover the ownership guards (404 / 403), symbol validation (400 / 422), and
the happy-path mapping into the response models for both verbs.
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

OWNER = "test-user-123"  # what create_test_app injects as the current user


def _ws(user_id=OWNER, workspace_id=None):
    """Minimal workspace row — require_workspace_owner only reads user_id."""
    return {"workspace_id": workspace_id or str(uuid.uuid4()), "user_id": user_id}


@pytest_asyncio.fixture
async def client():
    from src.server.app.chart_annotations import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def _url(workspace_id: str) -> str:
    return f"/api/v1/workspaces/{workspace_id}/chart-annotations"


# ---------------------------------------------------------------------------
# GET — list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_success_maps_charts(client):
    ws = _ws()
    chart = {
        "chart_id": "AAPL:1day",
        "symbol": "AAPL",
        "timeframe": "1day",
        "annotations": [{"annotation_id": "a1", "type": "price_line", "price": 205}],
    }
    with (
        patch(
            "src.server.app.chart_annotations.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.chart_annotations.list_charts",
            new_callable=AsyncMock,
            return_value=[chart],
        ) as mock_list,
    ):
        resp = await client.get(_url(ws["workspace_id"]), params={"symbol": "aapl"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["workspace_id"] == ws["workspace_id"]
    assert body["charts"] == [chart]
    # Symbol is uppercased before hitting the store; timeframe omitted -> None.
    mock_list.assert_awaited_once_with(ws["workspace_id"], "AAPL", None)


@pytest.mark.asyncio
async def test_list_passes_timeframe_filter(client):
    ws = _ws()
    with (
        patch(
            "src.server.app.chart_annotations.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.chart_annotations.list_charts",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_list,
    ):
        resp = await client.get(
            _url(ws["workspace_id"]), params={"symbol": "nvda", "timeframe": "1hour"}
        )

    assert resp.status_code == 200
    mock_list.assert_awaited_once_with(ws["workspace_id"], "NVDA", "1hour")


@pytest.mark.asyncio
async def test_list_not_found(client):
    wid = str(uuid.uuid4())
    with patch(
        "src.server.app.chart_annotations.db_get_workspace",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.get(_url(wid), params={"symbol": "AAPL"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_forbidden(client):
    ws = _ws(user_id="someone-else")
    with patch(
        "src.server.app.chart_annotations.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.get(_url(ws["workspace_id"]), params={"symbol": "AAPL"})
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_list_empty_symbol_400_after_owner_check(client):
    """Owner check passes, then a blank symbol is rejected with 400 (not 422)."""
    ws = _ws()
    with (
        patch(
            "src.server.app.chart_annotations.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.chart_annotations.list_charts",
            new_callable=AsyncMock,
        ) as mock_list,
    ):
        resp = await client.get(_url(ws["workspace_id"]), params={"symbol": "   "})
    assert resp.status_code == 400
    mock_list.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_missing_symbol_422(client):
    """symbol is a required query param — omitting it is a validation error."""
    resp = await client.get(_url(str(uuid.uuid4())))
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# DELETE — clear
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_success_uppercases_chart_id(client):
    ws = _ws()
    with (
        patch(
            "src.server.app.chart_annotations.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.chart_annotations.clear_chart",
            new_callable=AsyncMock,
            return_value=3,
        ) as mock_clear,
    ):
        resp = await client.delete(
            _url(ws["workspace_id"]), params={"symbol": "aapl", "timeframe": "1hour"}
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["cleared"] == 3
    assert body["chart_id"] == "AAPL:1hour"
    mock_clear.assert_awaited_once_with(ws["workspace_id"], "AAPL:1hour")


@pytest.mark.asyncio
async def test_clear_defaults_timeframe_to_1day(client):
    ws = _ws()
    with (
        patch(
            "src.server.app.chart_annotations.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.chart_annotations.clear_chart",
            new_callable=AsyncMock,
            return_value=0,
        ) as mock_clear,
    ):
        resp = await client.delete(_url(ws["workspace_id"]), params={"symbol": "AAPL"})

    assert resp.status_code == 200
    assert resp.json()["chart_id"] == "AAPL:1day"
    mock_clear.assert_awaited_once_with(ws["workspace_id"], "AAPL:1day")


@pytest.mark.asyncio
async def test_clear_not_found(client):
    wid = str(uuid.uuid4())
    with patch(
        "src.server.app.chart_annotations.db_get_workspace",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.delete(_url(wid), params={"symbol": "AAPL"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_clear_forbidden(client):
    ws = _ws(user_id="someone-else")
    with patch(
        "src.server.app.chart_annotations.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.delete(_url(ws["workspace_id"]), params={"symbol": "AAPL"})
    assert resp.status_code == 403
