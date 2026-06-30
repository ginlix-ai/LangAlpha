"""Unit tests for the Robinhood connect router's pure + state-bound logic.

The full initiate/callback flow needs network + DB (covered by local e2e); here
we lock the security-critical defaults (the registered server gates the trade
tools) and the auth-less callback's state binding.
"""

from __future__ import annotations

import pytest
from fastapi.responses import HTMLResponse

from src.server.app import robinhood as rh


def test_server_blob_gates_trade_tools_by_default():
    blob = rh._robinhood_server_blob()
    assert blob["name"] == rh.ROBINHOOD_SERVER_NAME
    assert blob["transport"] == "http"
    assert blob["url"] == rh.ROBINHOOD_MCP_URL
    # The header is the exact vault ref (model only allows ${vault:NAME}); the
    # "Bearer " prefix lives inside the stored secret value.
    assert blob["headers"]["Authorization"] == "${vault:ROBINHOOD_TOKEN}"
    # Trade tools are denied by default; reads/preview are not.
    assert set(blob["tool_deny"]) == {"place_equity_order", "cancel_equity_order"}
    assert "get_portfolio" not in blob["tool_deny"]
    assert "review_equity_order" not in blob["tool_deny"]
    assert blob["discovery_uses_secrets"] is True


def test_redirect_uri_shape():
    uri = rh._redirect_uri("ws-123")
    assert uri.endswith("/api/v1/workspaces/ws-123/robinhood/callback")
    assert uri.startswith("http")


@pytest.mark.asyncio
async def test_callback_error_param_returns_close_page():
    resp = await rh.callback(
        workspace_id="w", state="", code="", error="access_denied",
        error_description="user said no",
    )
    assert isinstance(resp, HTMLResponse)
    assert resp.status_code == 200
    body = resp.body.decode()
    assert "oauth-complete" in body
    assert "user said no" in body


@pytest.mark.asyncio
async def test_callback_unknown_state_is_expired(monkeypatch):
    async def fake_lookup(state):
        return None

    monkeypatch.setattr(rh.ro_db, "get_pending_by_state", fake_lookup)
    resp = await rh.callback(workspace_id="w", state="bogus", code="c")
    assert isinstance(resp, HTMLResponse)
    assert "expired" in resp.body.decode()


@pytest.mark.asyncio
async def test_callback_rejects_workspace_mismatch(monkeypatch):
    """A state row for another workspace must not connect this one."""
    async def fake_lookup(state):
        return {"workspace_id": "other-ws", "user_id": "u"}

    monkeypatch.setattr(rh.ro_db, "get_pending_by_state", fake_lookup)
    resp = await rh.callback(workspace_id="w", state="s", code="c")
    assert "expired" in resp.body.decode()
