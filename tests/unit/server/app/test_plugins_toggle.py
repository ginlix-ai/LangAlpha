"""The plugin-level enable toggle (app/plugins.py).

Disabling a plugin withdraws every owned component through the delivery join
predicate, which decides only what the NEXT acquire sees. A sandbox holding a
relay JWT keeps reaching an already-minted server for hours, so the toggle owes
its components the same egress cut a per-server disable performs — this pins
that the plugin path does it too, for every server it owns.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

PLUGIN_ID = "22222222-2222-2222-2222-222222222222"


@pytest_asyncio.fixture
async def client():
    from src.server.app.plugins import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def _patches(*, enabled: bool, servers, connection):
    revoke = AsyncMock()
    stack = [
        patch(
            "src.server.app.plugins.set_plugin_enabled",
            new=AsyncMock(
                return_value={
                    "user_plugin_id": PLUGIN_ID,
                    "name": "demo",
                    "enabled": enabled,
                }
            ),
        ),
        patch(
            "src.server.app.plugins.list_plugin_server_names",
            new=AsyncMock(return_value=servers),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.database.egress_grants.revoke_grants_for_connection",
            new=revoke,
        ),
    ]
    return revoke, stack


async def _toggle(client, *, enabled, servers, connection):
    revoke, stack = _patches(
        enabled=enabled, servers=servers, connection=connection
    )
    for p in stack:
        p.start()
    try:
        resp = await client.patch(
            "/api/v1/plugins/demo/enabled", json={"enabled": enabled}
        )
    finally:
        for p in reversed(stack):
            p.stop()
    return resp, revoke


@pytest.mark.asyncio
async def test_disable_revokes_grants_for_every_owned_server(client):
    resp, revoke = await _toggle(
        client,
        enabled=False,
        servers=[{"name": "a", "plugin_server_key": "a"},
                 {"name": "b", "plugin_server_key": "b"}],
        connection=MagicMock(connection_id="c-1"),
    )
    assert resp.status_code == 200
    # One per owned server: a plugin disable is N server disables at once, and
    # missing any one of them leaves that server reachable.
    assert revoke.await_count == 2


@pytest.mark.asyncio
async def test_enable_does_not_revoke(client):
    resp, revoke = await _toggle(
        client,
        enabled=True,
        servers=[{"name": "a", "plugin_server_key": "a"}],
        connection=MagicMock(connection_id="c-1"),
    )
    assert resp.status_code == 200
    revoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_disable_with_no_owned_servers_is_a_noop(client):
    resp, revoke = await _toggle(
        client, enabled=False, servers=[], connection=None
    )
    assert resp.status_code == 200
    revoke.assert_not_awaited()
