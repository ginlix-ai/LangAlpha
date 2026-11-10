"""Chart layer reads against a real Postgres.

A worker of the build before chart layers can still write a legacy
annotation (``ann_<hex>``, a flat drawing) during a rolling deploy, after
migration 117 converted the rest: no read, delete or count meets one.
"""

from __future__ import annotations

import json
import uuid

import pytest
import pytest_asyncio

from src.server.database import chart_annotation as store
from src.server.models.chart_layer import ChartRef

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

LEGACY = {"annotation_id": "ann_" + "0" * 32, "type": "price_line", "price": 205.0, "symbol": "NVDA"}


@pytest_asyncio.fixture(loop_scope="session")
async def workspace(patched_get_db_connection, test_db_pool):
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    user_id = f"layers-test-{uuid.uuid4().hex[:12]}"
    await create_user(user_id=user_id, email=f"{user_id}@example.com", name="Test")
    row = await create_workspace(user_id=user_id, name="Layers", status="running")
    workspace_id = str(row["workspace_id"])
    try:
        yield workspace_id
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (workspace_id,))
            await conn.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


async def test_a_legacy_row_written_after_115_never_reaches_a_read(workspace, test_db_pool):
    chart = ChartRef.of(workspace, "NVDA", "1day")
    await store.add_layers(
        [(chart.layer("setup"), {"version": 1, "id": "setup", "title": "Setup", "symbol": "NVDA", "timeframe": "1day"})]
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO chart_annotations (workspace_id, chart_id, symbol, timeframe, annotation_id, payload)"
            " VALUES (%s, %s, 'NVDA', '1day', %s, %s::jsonb)",
            (workspace, chart.chart_id, LEGACY["annotation_id"], json.dumps(LEGACY)),
        )

    assert [r["layer_id"] for r in await store.list_layers(workspace, chart.chart_id)] == ["setup"]
    [listed] = await store.list_charts(workspace, "NVDA")
    assert [r["layer_id"] for r in listed["layers"]] == ["setup"]
    assert await store.remove_layers(workspace, chart.chart_id, [LEGACY["annotation_id"]]) == []
    assert await store.clear_chart(workspace, chart.chart_id) == 1

    async with test_db_pool.connection() as conn:
        left = await conn.execute(
            "SELECT annotation_id FROM chart_annotations WHERE workspace_id = %s", (workspace,)
        )
        # Kept for the conversion to turn into a layer.
        assert [r["annotation_id"] for r in await left.fetchall()] == [LEGACY["annotation_id"]]


async def test_a_capped_read_keeps_the_newest_layers_oldest_first(workspace, monkeypatch):
    chart = ChartRef.of(workspace, "NVDA", "1day")
    ids = [f"layer-{i}" for i in range(5)]
    for i in ids:  # one publish each, so each row has its own created_at
        body = {"version": 1, "id": i, "title": i, "symbol": "NVDA", "timeframe": "1day", "summary": "x" * 1000}
        await store.add_layers([(chart.layer(i), body)])
    size = len(json.dumps(body))

    monkeypatch.setattr(store, "_MAX_LAYER_BYTES", 3 * size + size // 2)
    assert [r["layer_id"] for r in await store.list_layers(workspace, chart.chart_id)] == ids[2:]
    [listed] = await store.list_charts(workspace, "NVDA")
    assert [r["layer_id"] for r in listed["layers"]] == ids[2:]

    monkeypatch.setattr(store, "_MAX_LAYER_ROWS", 2)
    assert [r["layer_id"] for r in await store.list_layers(workspace, chart.chart_id)] == ids[3:]

    # Republished, the oldest layer is the newest publish: it is read, and keeps its place.
    await store.add_layers([(chart.layer(ids[0]), {**body, "id": ids[0], "title": "again"})])
    assert [r["layer_id"] for r in await store.list_layers(workspace, chart.chart_id)] == [ids[0], ids[4]]
