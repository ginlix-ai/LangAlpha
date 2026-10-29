"""A disconnect the user asks for switches the server off.

A server that needs a sign-in is on only while its connection stands, so giving
the connection up also takes the server out of every workspace. Only this door
does it: ``disconnect_server`` is also the teardown under a row drop and a moved
address, and an expired token leaves the row on, showing reconnect.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.server.app import mcp_oauth as mod


@pytest.mark.asyncio
async def test_a_disconnect_switches_the_server_off_once_the_connection_is_revoked(
    monkeypatch,
):
    calls: list[tuple] = []

    async def _disconnect(user_id, name):
        calls.append(("disconnect", user_id, name))
        return True

    async def _switch(user_id, name, enabled):
        calls.append(("switch", user_id, name, enabled))
        return {"name": name, "enabled": enabled}, None

    monkeypatch.setattr(mod, "disconnect_server", _disconnect)
    monkeypatch.setattr(mod, "apply_catalog_enabled", _switch)

    assert await mod.oauth_disconnect("srv", "user-1") == {"ok": True}
    assert calls == [
        ("disconnect", "user-1", "srv"),
        ("switch", "user-1", "srv", False),
    ]


@pytest.mark.asyncio
async def test_no_connection_to_give_up_switches_nothing(monkeypatch):
    switch = AsyncMock()
    monkeypatch.setattr(mod, "disconnect_server", AsyncMock(return_value=False))
    monkeypatch.setattr(mod, "apply_catalog_enabled", switch)

    with pytest.raises(HTTPException) as caught:
        await mod.oauth_disconnect("srv", "user-1")

    assert caught.value.status_code == 404
    switch.assert_not_awaited()

