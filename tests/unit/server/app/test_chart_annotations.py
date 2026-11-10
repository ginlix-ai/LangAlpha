"""Tests for the chart layers API router (src/server/app/chart_annotations.py).

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
        "layers": [
            {
                "workspace_id": ws["workspace_id"], "chart_id": "AAPL:1day", "symbol": "AAPL",
                "timeframe": "1day", "layer_id": "a", "layer": {"id": "a"},
            }
        ],
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


# ---------------------------------------------------------------------------
# DELETE — one chart layer
# ---------------------------------------------------------------------------


def _item_url(workspace_id: str, layer_id: str) -> str:
    return f"{_url(workspace_id)}/items/{layer_id}"


@pytest.mark.asyncio
@pytest.mark.parametrize("spelled", ["BRK.B", "brk-b"])
async def test_remove_item_class_share_hits_the_stored_chart(client, spelled):
    """BRK.B and BRK-B are one chart: the delete lands on the id rows store."""
    ws = _ws()
    with (
        patch(
            "src.server.app.chart_annotations.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.chart_annotations.remove_layers",
            new_callable=AsyncMock,
            return_value=["a"],
        ) as mock_remove,
    ):
        resp = await client.delete(
            _item_url(ws["workspace_id"], "a"),
            params={"symbol": spelled, "timeframe": "1day"},
        )

    assert resp.status_code == 200
    assert resp.json()["chart_id"] == "BRK-B:1day"
    mock_remove.assert_awaited_once_with(ws["workspace_id"], "BRK-B:1day", ["a"])


@pytest.mark.asyncio
@pytest.mark.parametrize("removed", [["nvda-setup"], []])
async def test_remove_item_deletes_one_row_and_a_repeat_succeeds(client, removed):
    ws = _ws()
    with (
        patch(
            "src.server.app.chart_annotations.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.chart_annotations.remove_layers",
            new_callable=AsyncMock,
            return_value=removed,
        ) as mock_remove,
    ):
        resp = await client.delete(
            _item_url(ws["workspace_id"], "nvda-setup"),
            params={"symbol": "nvda", "timeframe": "1hour"},
        )

    assert resp.status_code == 200
    assert resp.json() == {
        "workspace_id": ws["workspace_id"],
        "chart_id": "NVDA:1hour",
        "layer_id": "nvda-setup",
        "removed": len(removed),
    }
    mock_remove.assert_awaited_once_with(ws["workspace_id"], "NVDA:1hour", ["nvda-setup"])


@pytest.mark.asyncio
async def test_remove_item_forbidden_deletes_nothing(client):
    ws = _ws(user_id="someone-else")
    with (
        patch(
            "src.server.app.chart_annotations.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.chart_annotations.remove_layers",
            new_callable=AsyncMock,
        ) as mock_remove,
    ):
        resp = await client.delete(
            _item_url(ws["workspace_id"], "nvda-setup"), params={"symbol": "NVDA"}
        )
    assert resp.status_code == 403
    mock_remove.assert_not_awaited()


# ---------------------------------------------------------------------------
# The store's list: layers grouped by chart
# ---------------------------------------------------------------------------


class _Rows:
    """A pooled connection whose one query answers ``rows``, keeping what it was asked."""

    def __init__(self, rows):
        self.rows = rows
        self.queries: list[tuple[str, list]] = []

    def cursor(self, **_):
        outer = self

        class _Cursor:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, sql, params):
                outer.queries.append((sql, params))

            async def fetchall(self):
                return outer.rows

        return _Cursor()


@pytest.mark.asyncio
async def test_list_charts_groups_layer_rows_by_chart():
    from contextlib import asynccontextmanager

    from src.server.database import chart_annotation as store

    layer = {"id": "a"}
    rows = [
        {"workspace_id": "ws", "chart_id": cid, "symbol": "NVDA", "timeframe": tf,
         "annotation_id": "a", "payload": layer, "matched": 2}
        for cid, tf in (("NVDA:1day", "1day"), ("NVDA:1hour", "1hour"))
    ]
    db = _Rows(rows)

    @asynccontextmanager
    async def conn():
        yield db

    with patch.object(store, "get_db_connection", conn):
        charts = await store.list_charts("ws", "NVDA")

    # Each layer's address comes from its row's columns; the payload is the layer.
    assert charts == [
        {
            "chart_id": cid, "symbol": "NVDA", "timeframe": tf,
            "layers": [
                {"workspace_id": "ws", "chart_id": cid, "symbol": "NVDA", "timeframe": tf,
                 "layer_id": "a", "layer": layer}
            ],
        }
        for cid, tf in (("NVDA:1day", "1day"), ("NVDA:1hour", "1hour"))
    ]
    [(sql, params)] = db.queries
    assert params[:2] == ["ws", "NVDA"]
