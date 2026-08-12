"""Vault-mutation → MCP cache invalidation.

The discovery fingerprint hashes ``${vault:NAME}`` ref strings, never secret
values, so a value change alone can't churn any config hash. These tests pin
the explicit invalidation instead: a secret change that touches a referencing
workspace MCP server bumps ``mcp_config_version``, purges the discovery
snapshots of secret-using referencing servers, and schedules a proactive
apply — and a change to an un-referenced secret does none of that.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import src.server.app.mcp_servers as mcp_servers_mod
import src.server.database.mcp_servers as mcp_db
from src.server.app.vault import _invalidate_mcp_for_secret


def _ws_row(name: str, config: dict) -> dict:
    return {"name": name, "source": "workspace", "enabled": True, "config": config}


@pytest.fixture
def patched(monkeypatch):
    purge_bump = AsyncMock()
    bump = AsyncMock()
    sched = MagicMock()
    monkeypatch.setattr(mcp_db, "delete_tool_schemas_and_bump", purge_bump)
    monkeypatch.setattr(mcp_db, "bump_workspace_mcp_version", bump)
    monkeypatch.setattr(mcp_servers_mod, "_schedule_proactive_apply", sched)
    return purge_bump, bump, sched


@pytest.mark.asyncio
async def test_secret_change_purges_and_bumps_for_secret_using_server(
    monkeypatch, patched
):
    purge_bump, bump, sched = patched
    rows = [
        # Remote server authenticating via the changed secret: its discovery
        # runs WITH secrets, so its cached tools/list may depend on the value.
        _ws_row("authy", {
            "transport": "http",
            "url": "https://api.example.com/mcp",
            "headers": {"Authorization": "${vault:API_KEY}"},
        }),
        # References a DIFFERENT secret — untouched.
        _ws_row("other", {
            "transport": "stdio",
            "command": "npx",
            "env": {"TOKEN": "${vault:OTHER_KEY}"},
        }),
    ]
    monkeypatch.setattr(
        mcp_db, "list_workspace_servers", AsyncMock(return_value=rows)
    )

    await _invalidate_mcp_for_secret("ws-1", "user-1", "API_KEY")

    # Purge and version bump ride ONE atomic call; no separate bump.
    purge_bump.assert_awaited_once_with("ws-1", ["authy"])
    bump.assert_not_awaited()
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_stdio_env_ref_bumps_without_purge(monkeypatch, patched):
    """A stdio server's discovery runs secret-less, so its snapshot can't
    depend on the value — no purge, but the bump still re-resolves the live
    session (covers the needs_secret → ready transition)."""
    purge_bump, bump, sched = patched
    rows = [
        _ws_row("plain", {
            "transport": "stdio",
            "command": "npx",
            "env": {"TOKEN": "${vault:API_KEY}"},
        }),
    ]
    monkeypatch.setattr(
        mcp_db, "list_workspace_servers", AsyncMock(return_value=rows)
    )

    await _invalidate_mcp_for_secret("ws-1", "user-1", "API_KEY")

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("ws-1")
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_unreferenced_secret_is_a_noop(monkeypatch, patched):
    purge_bump, bump, sched = patched
    rows = [
        _ws_row("authy", {
            "transport": "http",
            "url": "https://api.example.com/mcp",
            "headers": {"Authorization": "${vault:API_KEY}"},
        }),
    ]
    monkeypatch.setattr(
        mcp_db, "list_workspace_servers", AsyncMock(return_value=rows)
    )

    await _invalidate_mcp_for_secret("ws-1", "user-1", "UNRELATED")

    purge_bump.assert_not_awaited()
    bump.assert_not_awaited()
    sched.assert_not_called()


@pytest.mark.asyncio
async def test_invalidation_failure_never_raises(monkeypatch, patched):
    """Best-effort: a DB failure during invalidation must not fail the vault
    mutation that triggered it."""
    monkeypatch.setattr(
        mcp_db, "list_workspace_servers", AsyncMock(side_effect=RuntimeError("db down"))
    )

    await _invalidate_mcp_for_secret("ws-1", "user-1", "API_KEY")  # no raise
