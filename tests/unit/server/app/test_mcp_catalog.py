"""Tests for the user MCP catalog router (app/mcp_catalog.py).

Covers list/get/create/update/delete, 409 on duplicate, 404 on missing, the
name-mismatch guard on PUT, and that the owner-scoped responses echo the stored
env/header maps verbatim so an edit round-trips them.
"""

from __future__ import annotations

from contextlib import ExitStack, asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.services.vault_invalidation import USER_TIER
from tests.conftest import create_test_app


def _row(name="remote_server", **overrides):
    base = {
        # The plugin LEFT JOIN is part of every catalog SELECT, so a real row
        # always carries these two, NULL when it has no plugin owner.
        "plugin_name": None,
        "plugin_enabled": None,
        "user_mcp_server_id": "11111111-1111-1111-1111-111111111111",
        "user_id": "test-user-123",
        "name": name,
        "transport": "http",
        "command": None,
        "args": [],
        "url": "https://api.example.com/mcp",
        "env": {},
        "headers": {"Authorization": "${vault:API_KEY}", "X-Trace": "literal-value"},
        "description": "d",
        "instruction": "i",
        "tool_exposure_mode": "summary",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    base.update(overrides)
    return base


@pytest_asyncio.fixture
async def client():
    from src.server.app.mcp_catalog import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


STORED_HEADERS = {"Authorization": "${vault:API_KEY}", "X-Trace": "literal-value"}


@pytest.mark.asyncio
async def test_list_echoes_stored_maps_and_reports_max(client):
    with patch(
        "src.server.app.mcp_catalog.list_catalog_servers",
        new=AsyncMock(return_value=[_row()]),
    ):
        resp = await client.get("/api/v1/mcp/servers")
    assert resp.status_code == 200
    body = resp.json()
    assert body["servers"][0]["headers"] == STORED_HEADERS
    assert body["servers"][0]["header_refs"] == ["API_KEY"]
    assert body["max_servers"] == 50


@pytest.mark.asyncio
async def test_list_reports_hash_gated_tool_counts(client):
    """tool_count mirrors the workspace rule: only an ok snapshot discovered
    under the server's CURRENT fingerprint counts; stale/error/missing ⇒ null
    (never 0 — the UI hides null, and 0 would claim a discovery that isn't
    current)."""
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    rows = [_row(), _row(name="stale_server"), _row(name="never_discovered")]
    current_fp = mcp_discovery_fingerprint(user_row_to_server_config(rows[0]))
    schemas = [
        {"server_name": "remote_server", "status": "ok", "error": "",
         "config_hash": current_fp,
         "tools": [{"name": "a"}, {"name": "b"}]},
        {"server_name": "stale_server", "status": "ok", "error": "",
         "config_hash": "not-the-current-fingerprint",
         "tools": [{"name": "a"}]},
    ]
    with (
        patch(
            "src.server.app.mcp_catalog.list_catalog_servers",
            new=AsyncMock(return_value=rows),
        ),
        patch(
            "src.server.app.mcp_catalog.get_user_tool_schemas",
            new=AsyncMock(return_value=schemas),
        ),
    ):
        resp = await client.get("/api/v1/mcp/servers")
    assert resp.status_code == 200
    by_name = {s["name"]: s for s in resp.json()["servers"]}
    assert by_name["remote_server"]["tool_count"] == 2
    assert by_name["stale_server"]["tool_count"] is None
    assert by_name["never_discovered"]["tool_count"] is None


@pytest.mark.asyncio
async def test_all_scopes_asks_for_live_workspaces_only(client):
    """The shared query defaults to every workspace a user ever had, because
    vault invalidation has to sweep snapshots a soft-deleted workspace left
    behind. This view renders scopes to a person, so it wants the live ones."""
    local = AsyncMock(return_value=[])
    with (
        patch(
            "src.server.app.mcp_catalog.list_catalog_servers",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.app.mcp_catalog.list_scope_markers_for_user",
            new=AsyncMock(return_value=[]),
        ),
        patch("src.server.app.mcp_catalog.list_local_servers_for_user", new=local),
    ):
        resp = await client.get("/api/v1/mcp/servers?all_scopes=true")
    assert resp.status_code == 200
    assert local.await_args.kwargs["live_only"] is True


@pytest.mark.asyncio
async def test_create_happy(client):
    with (
        patch(
            "src.server.app.mcp_catalog.create_catalog_server",
            new=AsyncMock(return_value=_row(name="new_server")),
        ),
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=None),
        ),
    ):
        resp = await client.post(
            "/api/v1/mcp/servers",
            json={
                "name": "new_server",
                "transport": "http",
                "url": "https://api.example.com/mcp",
                "headers": {"Authorization": "${vault:API_KEY}"},
            },
        )
    assert resp.status_code == 201
    assert resp.json()["name"] == "new_server"


@pytest.mark.asyncio
async def test_create_duplicate_409(client):
    with patch(
        "src.server.app.mcp_catalog.create_catalog_server",
        new=AsyncMock(side_effect=ValueError("already exists")),
    ):
        resp = await client.post(
            "/api/v1/mcp/servers",
            json={"name": "dup", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_create_rejects_bash(client):
    resp = await client.post(
        "/api/v1/mcp/servers",
        json={"name": "evil", "transport": "stdio", "command": "bash"},
    )
    assert resp.status_code == 422
    # 422 detail is a flat string, not FastAPI's default list shape.
    assert isinstance(resp.json()["detail"], str)


@pytest.mark.asyncio
async def test_create_over_cap_409(client):
    with patch(
        "src.server.app.mcp_catalog.create_catalog_server",
        new=AsyncMock(
            side_effect=ValueError(
                "Maximum of 50 MCP catalog servers per user reached"
            )
        ),
    ):
        resp = await client.post(
            "/api/v1/mcp/servers",
            json={"name": "over_cap", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409
    assert "Maximum of 50" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_invalid_body_422_string_detail(client):
    resp = await client.put(
        "/api/v1/mcp/servers/remote_server",
        json={"name": "remote_server", "transport": "stdio", "command": "bash"},
    )
    assert resp.status_code == 422
    assert isinstance(resp.json()["detail"], str)


@pytest.mark.asyncio
async def test_get_404(client):
    with patch(
        "src.server.app.mcp_catalog.get_catalog_server",
        new=AsyncMock(return_value=None),
    ):
        resp = await client.get("/api/v1/mcp/servers/ghost")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_echoes_stored_maps(client):
    """The edit form hydrates from these maps; only vault refs would leave it
    blank, and the PUT that follows replaces the whole row."""
    with patch(
        "src.server.app.mcp_catalog.get_catalog_server",
        new=AsyncMock(return_value=_row()),
    ):
        resp = await client.get("/api/v1/mcp/servers/remote_server")
    assert resp.status_code == 200
    assert resp.json()["headers"] == STORED_HEADERS


@pytest.mark.asyncio
async def test_update_response_echoes_the_written_maps(client):
    """A PUT answers with what it stored, so the form the user is still looking
    at re-submits the same config rather than an emptied one."""
    written = {"Authorization": "${vault:API_KEY}", "X-Tenant": "acme"}
    with (
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row(headers=written)),
        ),
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.schedule_post_edit_rediscovery"
        ),
    ):
        resp = await client.put(
            "/api/v1/mcp/servers/remote_server",
            json={
                "name": "remote_server",
                "transport": "http",
                "url": "https://api.example.com/mcp",
                "headers": written,
                "description": "edited",
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["headers"] == written
    assert body["header_refs"] == ["API_KEY"]


@pytest.mark.asyncio
async def test_update_name_mismatch_409(client):
    resp = await client.put(
        "/api/v1/mcp/servers/remote_server",
        json={"name": "different", "transport": "stdio", "command": "npx"},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_update_missing_404(client):
    with (
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=None),
        ),
    ):
        resp = await client.put(
            "/api/v1/mcp/servers/remote_server",
            json={"name": "remote_server", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "edit, revokes",
    [
        ({"url": "https://moved.example.com/mcp"}, True),
        ({"url": "https://API.example.com:443/mcp/"}, False),
        ({"transport": "stdio", "command": "npx"}, True),
    ],
)
async def test_update_revokes_when_consent_moves(client, edit, revokes):
    """PUT delegates the consent transition to the lifecycle helper: a token
    consented for the old endpoint must not survive an edit that moves it."""
    from types import SimpleNamespace

    from src.server.database.mcp_oauth import ConnectionStatus

    connection = SimpleNamespace(
        connection_id="c-1",
        server_url="https://api.example.com/mcp",
        status=ConnectionStatus.CONNECTED,
    )
    disconnect = AsyncMock(return_value=True)
    body = {"name": "remote_server", "transport": "http", **edit}
    with (
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        # Pre-update read, then the committed read the consent check runs on.
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(side_effect=[_row(), _row(**edit)]),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.services.mcp_oauth.lifecycle.disconnect_server",
            new=disconnect,
        ),
    ):
        resp = await client.put("/api/v1/mcp/servers/remote_server", json=body)
    assert resp.status_code == 200
    if revokes:
        disconnect.assert_awaited_once_with("test-user-123", "remote_server")
    else:
        disconnect.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_consent_check_reads_the_committed_row(client):
    """Two PUTs racing on one row: this one wrote a moved URL, the other
    committed the consented URL back before this consent check — a separate
    transaction — ran. Reading the row rather than trusting this request's own
    values is what keeps the survivor's connection alive."""
    disconnect = AsyncMock(return_value=True)
    scheduled = MagicMock()
    moved = "https://moved.example.com/mcp"
    with (
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row(url=moved)),
        ),
        # Pre-update read, then the restored row the racing PUT committed.
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(side_effect=[_row(), _row()]),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=_connected()),
        ),
        patch(
            "src.server.services.mcp_oauth.lifecycle.disconnect_server",
            new=disconnect,
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.schedule_post_edit_rediscovery",
            new=scheduled,
        ),
    ):
        resp = await client.put(
            "/api/v1/mcp/servers/remote_server",
            json={"name": "remote_server", "transport": "http", "url": moved},
        )
    assert resp.status_code == 200
    disconnect.assert_not_awaited()
    scheduled.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, warned",
    [
        ("connected", True),
        # A binding awaiting re-auth still owns the Authorization header.
        ("needs_reauth", True),
        ("revoked", False),
        (None, False),
    ],
)
async def test_update_warns_when_headers_meet_oauth(client, status, warned):
    """Headers and OAuth are independently settable, but the OAuth path sends
    only its own Authorization — the write says so rather than dropping them."""
    from types import SimpleNamespace

    from src.server.database.mcp_oauth import ConnectionStatus

    connection = (
        None
        if status is None
        else SimpleNamespace(
            connection_id="c-1",
            server_url="https://api.example.com/mcp",
            status=ConnectionStatus(status),
        )
    )
    body = {
        "name": "remote_server",
        "transport": "http",
        "url": "https://api.example.com/mcp",
        "headers": {"X-Tenant": "acme"},
    }
    with (
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        # The consent-move check reads through its own late import.
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
    ):
        resp = await client.put("/api/v1/mcp/servers/remote_server", json=body)
    assert resp.status_code == 200
    warnings = resp.json()["warnings"]
    if warned:
        assert any("OAuth-connected" in w for w in warnings)
    else:
        assert warnings is None


@pytest.mark.asyncio
async def test_update_without_headers_never_warns(client):
    """The warning is about headers being dropped — no headers, nothing dropped."""
    from types import SimpleNamespace

    from src.server.database.mcp_oauth import ConnectionStatus

    connection = SimpleNamespace(
        connection_id="c-1",
        server_url="https://api.example.com/mcp",
        status=ConnectionStatus.CONNECTED,
    )
    lookup = AsyncMock(return_value=connection)
    with (
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        patch("src.server.app.mcp_catalog.get_connection", new=lookup),
        patch("src.server.database.mcp_oauth.get_connection", new=AsyncMock(
            return_value=connection
        )),
    ):
        resp = await client.put(
            "/api/v1/mcp/servers/remote_server",
            json={
                "name": "remote_server",
                "transport": "http",
                "url": "https://api.example.com/mcp",
            },
        )
    assert resp.status_code == 200
    assert resp.json()["warnings"] is None
    # Short-circuited before the lookup: no query for a server that has none.
    lookup.assert_not_awaited()


async def _drain_rediscovery_tasks():
    """Await any background rediscovery the PUT scheduled."""
    import asyncio

    from src.server.services.mcp_oauth import discovery as mc

    pending = list(mc._rediscovery_tasks)
    if pending:
        await asyncio.gather(*pending)
    for _ in range(3):
        await asyncio.sleep(0)


def _connected(url="https://api.example.com/mcp"):
    from types import SimpleNamespace

    from src.server.database.mcp_oauth import ConnectionStatus

    return SimpleNamespace(
        connection_id="c-1", server_url=url, status=ConnectionStatus.CONNECTED
    )


@pytest.mark.asyncio
async def test_update_rediscovers_when_fingerprint_moves_and_consent_stays(client):
    """A consent-preserving edit that moves the discovery fingerprint orphans
    the user-tier snapshot (it serves only under the CURRENT fingerprint) and
    nothing else re-discovers a host-side OAuth server — the PUT must kick the
    refresh itself, then resync live sandboxes."""
    refresh = AsyncMock(return_value={"status": "ok"})
    resync = AsyncMock()
    connection = _connected()
    with (
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row(headers={"X-New": "1"})),
        ),
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.refresh_user_tool_schemas",
            new=refresh,
        ),
        patch(
            "src.server.services.mcp_oauth.connect._resync_live_sandboxes",
            new=resync,
        ),
    ):
        resp = await client.put(
            "/api/v1/mcp/servers/remote_server",
            json={
                "name": "remote_server",
                "transport": "http",
                "url": "https://api.example.com/mcp",
                "headers": {"X-New": "1"},
            },
        )
        await _drain_rediscovery_tasks()
    assert resp.status_code == 200
    refresh.assert_awaited_once_with("test-user-123", "remote_server")
    resync.assert_awaited_once_with("test-user-123")


@pytest.mark.asyncio
async def test_update_skips_rediscovery_when_fingerprint_is_unchanged(client):
    """Prompt-only edits (description) leave the fingerprint alone — the cached
    snapshot still serves, so no discovery round-trip is spent."""
    refresh = AsyncMock()
    connection = _connected()
    with (
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row(description="edited")),
        ),
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.refresh_user_tool_schemas",
            new=refresh,
        ),
    ):
        resp = await client.put(
            "/api/v1/mcp/servers/remote_server",
            json={
                "name": "remote_server",
                "transport": "http",
                "url": "https://api.example.com/mcp",
                "description": "edited",
            },
        )
        await _drain_rediscovery_tasks()
    assert resp.status_code == 200
    refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_skips_rediscovery_when_the_edit_revoked_consent(client):
    """An edit that moved consent already forced a reconnect; the reconnect's
    own discovery covers it, and refreshing a just-revoked connection could
    only 409."""
    refresh = AsyncMock()
    connection = _connected()
    moved = "https://moved.example.com/mcp"
    with (
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(side_effect=[_row(), _row(url=moved)]),
        ),
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row(url=moved)),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.services.mcp_oauth.lifecycle.disconnect_server",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.refresh_user_tool_schemas",
            new=refresh,
        ),
    ):
        resp = await client.put(
            "/api/v1/mcp/servers/remote_server",
            json={
                "name": "remote_server",
                "transport": "http",
                "url": "https://moved.example.com/mcp",
            },
        )
        await _drain_rediscovery_tasks()
    assert resp.status_code == 200
    refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_rediscovery_swallows_an_unusable_connection(client):
    """The refresh gate answers 'not an OAuth server / not servable' as
    TokenUnavailable; the background task treats that as nothing-to-do — no
    resync, no error."""
    from src.server.services.mcp_oauth.lifecycle import TokenUnavailable

    refresh = AsyncMock(side_effect=TokenUnavailable("unknown_connection"))
    resync = AsyncMock()
    connection = _connected()
    with (
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=_row()),
        ),
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=_row(headers={"X-New": "1"})),
        ),
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.refresh_user_tool_schemas",
            new=refresh,
        ),
        patch(
            "src.server.services.mcp_oauth.connect._resync_live_sandboxes",
            new=resync,
        ),
    ):
        resp = await client.put(
            "/api/v1/mcp/servers/remote_server",
            json={
                "name": "remote_server",
                "transport": "http",
                "url": "https://api.example.com/mcp",
                "headers": {"X-New": "1"},
            },
        )
        await _drain_rediscovery_tasks()
    assert resp.status_code == 200
    refresh.assert_awaited_once()
    resync.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_warns_when_recreate_lands_on_a_live_connection(client):
    """A new name has no connection, but a recreate can land on one that
    outlived the old catalog row."""
    from types import SimpleNamespace

    from src.server.database.mcp_oauth import ConnectionStatus

    connection = SimpleNamespace(
        connection_id="c-1",
        server_url="https://api.example.com/mcp",
        status=ConnectionStatus.CONNECTED,
    )
    with (
        patch(
            "src.server.app.mcp_catalog.create_catalog_server",
            new=AsyncMock(return_value=_row(name="remote_server")),
        ),
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=connection),
        ),
    ):
        resp = await client.post(
            "/api/v1/mcp/servers",
            json={
                "name": "remote_server",
                "transport": "http",
                "url": "https://api.example.com/mcp",
                "headers": {"X-Tenant": "acme"},
            },
        )
    assert resp.status_code == 201
    assert any("OAuth-connected" in w for w in resp.json()["warnings"])


@pytest.mark.asyncio
async def test_delete_happy_and_404(client):
    # Delete must revoke any OAuth connection + its grants (no catalog FK), so
    # the handler wraps the drop in oauth_fence: a disconnect before it, and
    # again after, to catch a callback that landed between the two transactions
    # and left a connected row behind a deleted catalog entry.
    disconnect = AsyncMock(return_value=True)
    with (
        patch(
            "src.server.services.mcp_oauth.lifecycle.disconnect_server",
            new=disconnect,
        ),
        patch(
            "src.server.app.mcp_catalog.delete_catalog_server",
            new=AsyncMock(return_value=True),
        ),
    ):
        ok = await client.delete("/api/v1/mcp/servers/remote_server")
    assert ok.status_code == 200 and ok.json() == {"ok": True}
    assert disconnect.await_args_list == [
        call("test-user-123", "remote_server"),
        call("test-user-123", "remote_server"),
    ]

    # 404 path: the revoke is a deliberate side effect either way (a connection
    # can outlive its catalog row). The fence closes on exit regardless of what
    # the body found — an unconditional second pass is what makes it impossible
    # to forget, and it costs one lookup that returns nothing.
    disconnect_missing = AsyncMock(return_value=False)
    with (
        patch(
            "src.server.services.mcp_oauth.lifecycle.disconnect_server",
            new=disconnect_missing,
        ),
        patch(
            "src.server.app.mcp_catalog.delete_catalog_server",
            new=AsyncMock(return_value=False),
        ),
    ):
        missing = await client.delete("/api/v1/mcp/servers/ghost")
    assert missing.status_code == 404
    assert disconnect_missing.await_args_list == [
        call("test-user-123", "ghost"),
        call("test-user-123", "ghost"),
    ]


# ---------------------------------------------------------------------------
# PATCH enabled — disable must bite live grants now, not at next acquire
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _toggle_patches(*, connection):
    revoke = AsyncMock()
    with (
        patch(
            "src.server.app.mcp_catalog.set_catalog_server_enabled",
            new=AsyncMock(return_value=True),
        ),
        # Patched at the source modules: the revoke lives in
        # mcp_oauth.lifecycle.revoke_live_grants, which both this route and the
        # plugin-level toggle call, and which imports these lazily.
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.database.egress_grants.revoke_grants_for_connection",
            new=revoke,
        ),
        patch(
            "src.server.app.mcp_catalog._relay_execution_warning",
            new=AsyncMock(return_value=None),
        ),
    ):
        yield revoke


@pytest.mark.asyncio
async def test_disable_revokes_live_grants(client):
    """An idle sandbox holds its grant_id and a relay JWT for hours, and the
    relay never consults the catalog row — the toggle itself must revoke."""
    connection = MagicMock(connection_id="c-1")
    async with _toggle_patches(connection=connection) as revoke:
        resp = await client.patch(
            "/api/v1/mcp/servers/remote_server/enabled", json={"enabled": False}
        )
    assert resp.status_code == 200
    revoke.assert_awaited_once_with("c-1")


@pytest.mark.asyncio
async def test_disable_without_connection_skips_revocation(client):
    async with _toggle_patches(connection=None) as revoke:
        resp = await client.patch(
            "/api/v1/mcp/servers/remote_server/enabled", json={"enabled": False}
        )
    assert resp.status_code == 200
    revoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_enable_does_not_revoke(client):
    connection = MagicMock(connection_id="c-1")
    async with _toggle_patches(connection=connection) as revoke:
        resp = await client.patch(
            "/api/v1/mcp/servers/remote_server/enabled", json={"enabled": True}
        )
    assert resp.status_code == 200
    revoke.assert_not_awaited()


# ---------------------------------------------------------------------------
# POST import — created secrets must converge like the vault routes' do
# ---------------------------------------------------------------------------


@pytest.fixture
def _import_txn():
    """Stub the per-entry import transaction — the writers are what's asserted."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _txn():
        yield None

    conn = MagicMock(name="conn")
    conn.transaction = _txn

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch("src.server.services.mcp_import.get_db_connection", new=_connection):
        yield conn


@asynccontextmanager
async def _import_patches(**extra):
    patches = {
        "list_catalog_servers": AsyncMock(return_value=[]),
        "get_user_secret_names": AsyncMock(return_value=set()),
        "create_user_secret": AsyncMock(),
        "create_catalog_server": AsyncMock(side_effect=lambda u, n, **kw: _row(n)),
        **extra,
    }
    with ExitStack() as stack:
        for attr, mock in patches.items():
            stack.enter_context(
                patch(f"src.server.app.mcp_catalog.{attr}", new=mock)
            )
        yield patches


@pytest.mark.asyncio
async def test_import_converges_each_created_secret(client, _import_txn):
    """Imported SERVERS are inert, but an imported SECRET can complete a
    dangling ${vault:NAME} ref on a connector that is already enabled — and
    nothing else on this path purges its snapshot, bumps the version, or
    reaches a live sandbox."""
    after = AsyncMock()
    async with _import_patches(after_secret_change=after) as mocks:
        resp = await client.post(
            "/api/v1/mcp/servers/import",
            json={
                "mcpServers": {
                    "srv-one": {
                        "type": "http",
                        "url": "https://api.example.com/a",
                        "headers": {"Authorization": "EXAMPLE-OPAQUE-TOKEN-AAAAAAAAAA"},
                    },
                    "srv-two": {
                        "type": "http",
                        "url": "https://api.example.com/b",
                        "headers": {"Authorization": "EXAMPLE-OPAQUE-TOKEN-BBBBBBBBBB"},
                    },
                }
            },
        )

    assert resp.status_code == 200
    created = resp.json()["secrets_created"]
    assert len(created) == 2
    assert mocks["create_user_secret"].await_count == 2
    # One convergence per created name, entered with the USER tier exactly as
    # the dedicated secret routes enter it.
    assert after.await_args_list == [
        call(USER_TIER, "test-user-123", name, user_id="test-user-123")
        for name in created
    ]


@pytest.mark.asyncio
async def test_import_without_created_secrets_skips_convergence(client, _import_txn):
    after = AsyncMock()
    async with _import_patches(after_secret_change=after) as mocks:
        resp = await client.post(
            "/api/v1/mcp/servers/import",
            json={"mcpServers": {"plain": {"command": "npx", "args": ["-y", "@foo/bar"]}}},
        )

    assert resp.status_code == 200
    assert resp.json()["created"] == 1
    assert resp.json()["secrets_created"] == []
    mocks["create_user_secret"].assert_not_awaited()
    after.assert_not_awaited()


def test_catalog_fields_match_the_writable_column_set():
    """``update_catalog_server`` now REJECTS unknown keys instead of dropping
    them, so a field added to ``to_catalog_fields`` without a matching column
    would 500 every PUT. Lock the two together."""
    from src.server.database.mcp_servers import CATALOG_COLUMNS
    from src.server.models.mcp_server import McpServerInput

    server = McpServerInput(
        name="remote_server", transport="http", url="https://api.example.com/mcp"
    )
    assert set(server.to_catalog_fields()) == set(CATALOG_COLUMNS)
