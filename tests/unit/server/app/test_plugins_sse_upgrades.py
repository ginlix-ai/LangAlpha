"""An sse upgrade answers a lost turn at the plugin fan-out like install and update."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app


@pytest_asyncio.fixture
async def client():
    from src.server.app.plugins import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_an_upgrade_that_waited_out_another_fan_out_is_a_conflict(client):
    plugin = {"user_plugin_id": "p1", "name": "demo", "mcp_document": {"mcpServers": {}}}
    busy = ValueError("Another plugin install or update is still running; try again once it finishes")
    with patch(
        "src.server.app.plugins.get_plugin", new=AsyncMock(return_value=plugin)
    ), patch(
        "src.server.app.plugins.apply_sse_upgrades", new=AsyncMock(side_effect=busy)
    ):
        resp = await client.post("/api/v1/plugins/demo/sse-upgrades", json={"keys": ["crm"]})
    assert resp.status_code == 409
    assert "still running" in resp.text
