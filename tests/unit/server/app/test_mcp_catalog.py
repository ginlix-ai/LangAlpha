"""Tests for the user MCP catalog router (app/mcp_catalog.py).

Covers list/get/create/update/delete, 409 on duplicate, 404 on missing, the
name-mismatch guard on PUT, and that the owner-scoped responses echo the stored
env/header maps verbatim so an edit round-trips them.
"""

from __future__ import annotations

from contextlib import ExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.database.mcp_servers import MAX_CATALOG_SERVERS_PER_USER
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
    # Bound to the constant, not its value: this asserts the cap reaches the
    # wire, which is what the page needs, and stays true when the cap moves.
    assert body["max_servers"] == MAX_CATALOG_SERVERS_PER_USER


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
async def test_create_accepts_any_command(client):
    # A shell and a container runtime are both ordinary ways to launch a
    # published MCP server, and the sandbox they land in already runs whatever
    # the agent asks it to.
    for command in ("bash", "docker"):
        with patch(
            "src.server.app.mcp_catalog.create_catalog_server",
            new=AsyncMock(side_effect=ValueError("already exists")),
        ):
            resp = await client.post(
                "/api/v1/mcp/servers",
                json={"name": "srv", "transport": "stdio", "command": command},
            )
        # 409, not 422: it got past validation and reached the writer.
        assert resp.status_code == 409


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
        json={"name": "remote_server", "transport": "stdio", "command": ""},
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
async def _toggle_patches(*, connection, row=True):
    revoke = AsyncMock()
    with (
        patch(
            "src.server.app.mcp_catalog.set_catalog_server_enabled",
            new=AsyncMock(return_value=row),
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


# ---------------------------------------------------------------------------
# Brokerages — shipped connectors, off until the user turns one on
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_brokerages_are_offered_without_touching_the_database(client):
    """The list is what this build ships, so it is the same for everybody.

    Nothing per-user belongs in it: whether one is configured is the catalog's
    answer, and mixing the two would give the page two places to disagree with
    itself about the same row.
    """
    resp = await client.get("/api/v1/mcp/brokerages")
    assert resp.status_code == 200
    by_name = {b["name"]: b for b in resp.json()["brokerages"]}
    assert set(by_name) == {"robinhood", "ibkr", "moomoo", "webull"}
    assert by_name["robinhood"]["native_callback_only"] is True
    assert by_name["ibkr"]["exclusive_connection"] is True
    assert by_name["ibkr"]["label"] == "Interactive Brokers"
    # The two quirks are independent, and Robinhood carries both. Asserted
    # because it is the one row where a reader could take the first flag as the
    # whole story, and because dropping this one silently costs the confirm
    # that stands between a connect here and the user's other AI platform.
    assert by_name["robinhood"]["exclusive_connection"] is True
    # A vendor whose authorization server takes the spec as written needs
    # neither quirk, so the flags stay off and every surface treats it as the
    # ordinary case. Asserted rather than left implicit: both defaults are
    # False, so a flag set here by mistake would otherwise read as intent.
    assert by_name["moomoo"]["native_callback_only"] is False
    assert by_name["moomoo"]["exclusive_connection"] is False


@pytest.mark.asyncio
async def test_enabling_an_unconfigured_brokerage_creates_it_and_switches_it_on(client):
    """First enable writes the row at OUR address, then goes through the switch.

    Created inert and then toggled, never created live: one thing decides a
    row's enabled state, and it is the one that already knows what each
    direction owes an OAuth connection. The user still sees it land on.
    """
    created = AsyncMock(return_value=_row(name="robinhood"))
    live = _row(name="robinhood", enabled=True)
    async with _toggle_patches(connection=None, row=live):
        with (
            patch(
                "src.server.app.mcp_catalog.get_catalog_server",
                new=AsyncMock(return_value=None),
            ),
            patch("src.server.app.mcp_catalog.create_catalog_server", new=created),
        ):
            resp = await client.patch(
                "/api/v1/mcp/brokerages/robinhood/enabled", json={"enabled": True}
            )
    assert resp.status_code == 200
    assert resp.json()["enabled"] is True
    kwargs = created.await_args.kwargs
    assert kwargs["url"] == "https://agent.robinhood.com/mcp/trading"
    assert kwargs["transport"] == "http"
    # Not created live: the switch below is what turns it on.
    assert "enabled" not in kwargs


@pytest.mark.asyncio
async def test_enabling_a_configured_brokerage_never_rewrites_it(client):
    """An existing row is toggled and left alone.

    Once it is the user's, its URL is theirs to edit — including a row they
    built themselves under this name. Restoring our address on every enable
    would undo a deliberate edit at the moment they were only reaching for the
    switch.
    """
    stored = _row(name="robinhood", url="https://edited.example.com/mcp")
    toggled = AsyncMock(return_value={**stored, "enabled": True})
    created = AsyncMock()
    with (
        patch(
            "src.server.app.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=stored),
        ),
        patch("src.server.app.mcp_catalog.set_catalog_server_enabled", new=toggled),
        patch("src.server.app.mcp_catalog.create_catalog_server", new=created),
    ):
        resp = await client.patch(
            "/api/v1/mcp/brokerages/robinhood/enabled", json={"enabled": True}
        )
    assert resp.status_code == 200
    assert resp.json()["url"] == "https://edited.example.com/mcp"
    created.assert_not_awaited()
    assert toggled.await_args.args[1:] == ("robinhood", True)


@pytest.mark.asyncio
async def test_disabling_a_configured_brokerage_goes_through_the_same_route(client):
    """One route for both directions, so the page never has to know which."""
    stored = _row(name="ibkr")
    async with _toggle_patches(connection=None, row={**stored, "enabled": False}):
        with patch(
            "src.server.app.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=stored),
        ):
            resp = await client.patch(
                "/api/v1/mcp/brokerages/ibkr/enabled", json={"enabled": False}
            )
    assert resp.status_code == 200
    assert resp.json()["enabled"] is False


@pytest.mark.asyncio
async def test_disabling_a_brokerage_revokes_its_grants(client):
    """The same bite as every other switch, and here it is the only one there is.

    A brokerage is listed under its own header and so never appears among the
    servers the user added — this route is the whole of how one gets turned
    off. A disable that only flipped the row would leave an idle sandbox
    trading through a relay JWT for hours, on the rows that can place orders.
    """
    stored = _row(name="robinhood")
    connection = MagicMock(connection_id="c-1")
    async with _toggle_patches(
        connection=connection, row={**stored, "enabled": False}
    ) as revoke:
        with patch(
            "src.server.app.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=stored),
        ):
            resp = await client.patch(
                "/api/v1/mcp/brokerages/robinhood/enabled", json={"enabled": False}
            )
    assert resp.status_code == 200
    revoke.assert_awaited_once_with("c-1")


@pytest.mark.asyncio
async def test_disabling_one_that_was_never_configured_creates_nothing(client):
    """There is nothing to turn off, and inventing a row to turn off would
    consume a catalog slot to reach the state it already had."""
    created = AsyncMock()
    with (
        patch(
            "src.server.app.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=None),
        ),
        patch("src.server.app.mcp_catalog.create_catalog_server", new=created),
    ):
        resp = await client.patch(
            "/api/v1/mcp/brokerages/ibkr/enabled", json={"enabled": False}
        )
    assert resp.status_code == 404
    created.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_brokerage_name_cannot_be_claimed_by_a_hand_written_row(client):
    """Reserved the way a builtin's name is, and for a sharper reason.

    A row under one of these names is joined to the shipped definition by name
    and shown wearing it: the vendor's label, its tile, its description and its
    warnings. Whoever owns the row owns where Connect sends the user, so leaving
    the name free let anything at all be presented as Robinhood.
    """
    created = AsyncMock()
    with patch("src.server.app.mcp_catalog.create_catalog_server", new=created):
        resp = await client.post(
            "/api/v1/mcp/servers",
            json={
                "name": "robinhood",
                "transport": "http",
                "url": "https://not-robinhood.example.com/mcp",
            },
        )
    assert resp.status_code == 409
    assert "reserved" in resp.json()["detail"]
    created.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_brokerage_name_cannot_be_claimed_through_import_either(client, _import_txn):
    """The reservation belongs to the catalog, not to the door it was built at.

    Import mints exactly the same row the create route does, and it was still
    reserving builtins only: a file naming ``robinhood`` was accepted, pointed
    anywhere the author liked, and then presented under the vendor's identity by
    a page that joins on name. The entry is skipped rather than failing the
    whole import, which is what every other collision on this path does.
    """
    async with _import_patches() as mocks:
        resp = await client.post(
            "/api/v1/mcp/servers/import",
            json={
                "mcpServers": {
                    "robinhood": {
                        "type": "http",
                        "url": "https://not-robinhood.example.com/mcp",
                    },
                    "srv_ok": {"type": "http", "url": "https://api.example.com/a"},
                }
            },
        )

    assert resp.status_code == 200
    by_name = {r["name"]: r for r in resp.json()["results"]}
    assert by_name["robinhood"]["status"] == "skipped"
    assert "reserves" in by_name["robinhood"]["reason"]
    # The rest of the file still lands: one bad name is not a failed import.
    assert by_name["srv_ok"]["status"] == "created"
    created = [c.args[1] for c in mocks["create_catalog_server"].await_args_list]
    assert "robinhood" not in created


@pytest.mark.asyncio
async def test_a_plugins_row_is_not_adopted_as_a_brokerage(client):
    """A plugin's row under a brokerage name is not the user's own edit.

    New installs cannot claim these names any more, but one installed before
    they were reserved still holds it, and adopting it here would hand it the
    vendor's identity while Connect went to whatever address the plugin chose.
    """
    stored = _row(name="robinhood", url="https://plugin-chose-this.example.com/mcp")
    stored["plugin_id"] = "user-plugin-7"
    toggled = AsyncMock()
    with (
        patch(
            "src.server.app.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=stored),
        ),
        patch("src.server.app.mcp_catalog.set_catalog_server_enabled", new=toggled),
    ):
        resp = await client.patch(
            "/api/v1/mcp/brokerages/robinhood/enabled", json={"enabled": True}
        )
    assert resp.status_code == 409
    assert "plugin" in resp.json()["detail"]
    toggled.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_brokerage_is_not_a_way_to_create_a_row(client):
    """The name is looked up in the shipped registry before anything else, so
    the route cannot be used to write an arbitrary server."""
    created = AsyncMock()
    with patch("src.server.app.mcp_catalog.create_catalog_server", new=created):
        resp = await client.patch(
            "/api/v1/mcp/brokerages/not_a_broker/enabled", json={"enabled": True}
        )
    assert resp.status_code == 404
    created.assert_not_awaited()


@pytest.mark.asyncio
async def test_brokerage_create_reports_the_catalog_cap(client):
    """The cap is the DB layer's to enforce; this route must not swallow it."""
    with (
        patch(
            "src.server.app.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "src.server.app.mcp_catalog.create_catalog_server",
            new=AsyncMock(side_effect=ValueError("Maximum of 50 ... reached")),
        ),
    ):
        resp = await client.patch(
            "/api/v1/mcp/brokerages/robinhood/enabled", json={"enabled": True}
        )
    assert resp.status_code == 409
    assert "Maximum" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_every_shipped_brokerage_survives_the_user_url_policy():
    """Our own definitions go through the validator every user row passes.

    A shipped address is the one payload nobody reviews at write time, so it
    must not also be the one that skips the https/SSRF policy — a definition
    that could not be typed in by hand should not be shippable either.
    """
    from src.server.models.mcp_server import McpServerInput
    from src.server.services.brokerages import BROKERAGES

    for b in BROKERAGES:
        server = McpServerInput(
            name=b.name, transport="http", url=b.url, description=b.description
        )
        assert server.url == b.url


class TestBuiltinToolsSeparateEmptyFromUnknown:
    """One worker's gap must not be reported as the server's shape.

    ``connect_all`` drops a builtin whose startup connect failed and the
    registry is then frozen, so that worker has no snapshot for it and never
    retries while its siblings answer normally. The route reads process-local
    state, which is the one thing it can honestly report, so it reports which
    of the two it is rather than flattening both to an empty list.
    """

    @staticmethod
    def _registry(**connectors):
        return SimpleNamespace(connectors=dict(connectors))

    @pytest.mark.asyncio
    async def test_a_connected_builtin_reports_its_tools(self):
        from src.server.app.mcp_catalog import get_builtin_server_tools

        tool = SimpleNamespace(name="quote", description="d", input_schema={})
        registry = self._registry(price=SimpleNamespace(tools=[tool]))
        with patch("src.server.app.mcp_catalog.builtin_names", return_value={"price"}), \
             patch("ptc_agent.core.mcp_registry.get_global_registry", return_value=registry):
            out = await get_builtin_server_tools("price", "u-1")
        assert out["connected"] is True
        assert [t["name"] for t in out["tools"]] == ["quote"]

    @pytest.mark.asyncio
    async def test_a_connected_builtin_with_no_tools_is_still_connected(self):
        # The genuinely empty case. It has to stay distinguishable from the one
        # below or the fix is pointless.
        from src.server.app.mcp_catalog import get_builtin_server_tools

        registry = self._registry(price=SimpleNamespace(tools=[]))
        with patch("src.server.app.mcp_catalog.builtin_names", return_value={"price"}), \
             patch("ptc_agent.core.mcp_registry.get_global_registry", return_value=registry):
            out = await get_builtin_server_tools("price", "u-1")
        assert out["connected"] is True
        assert out["tools"] == []

    @pytest.mark.asyncio
    async def test_a_builtin_this_worker_never_connected_says_so(self):
        from src.server.app.mcp_catalog import get_builtin_server_tools

        # Configured (so not a 404) but absent from the registry: this is what
        # a dropped connector looks like from here.
        with patch("src.server.app.mcp_catalog.builtin_names", return_value={"price"}), \
             patch("ptc_agent.core.mcp_registry.get_global_registry",
                   return_value=self._registry()):
            out = await get_builtin_server_tools("price", "u-1")
        assert out["connected"] is False
        assert out["tools"] == []

    @pytest.mark.asyncio
    async def test_no_registry_at_all_is_also_unknown_not_empty(self):
        from src.server.app.mcp_catalog import get_builtin_server_tools

        with patch("src.server.app.mcp_catalog.builtin_names", return_value={"price"}), \
             patch("ptc_agent.core.mcp_registry.get_global_registry", return_value=None):
            out = await get_builtin_server_tools("price", "u-1")
        assert out["connected"] is False

    @pytest.mark.asyncio
    async def test_an_unknown_name_is_still_a_404(self):
        from fastapi import HTTPException

        from src.server.app.mcp_catalog import get_builtin_server_tools

        with patch("src.server.app.mcp_catalog.builtin_names", return_value={"price"}):
            with pytest.raises(HTTPException) as exc:
                await get_builtin_server_tools("nope", "u-1")
        assert exc.value.status_code == 404


class TestCapabilitiesInForceAndCapabilitiesRemembered:
    """Two fields because a dead connection answers the two differently.

    The grant has to disappear with the connection, or a revoked broker keeps
    its "can place orders" badge. The choice must not, because reconnecting is
    the only way to change a selection and the dialog opens on it -- seeded
    from the grant, a repair after a token expiry re-proposed every group the
    user had declined.
    """

    @staticmethod
    def _decorate(status: str, granted):
        from src.server.app.mcp_catalog import _decorated

        return _decorated(
            _row(), {"status": status, "granted_capabilities": granted}
        )

    @pytest.mark.parametrize("status", ["connected", "refresh_ambiguous"])
    def test_a_servable_connection_answers_both_the_same_way(self, status):
        response = self._decorate(status, ["market_data"])

        assert response.granted_capabilities == ["market_data"]
        assert response.remembered_capabilities == ["market_data"]

    @pytest.mark.parametrize("status", ["needs_reauth", "revoked"])
    def test_a_dead_connection_keeps_the_choice_and_drops_the_grant(self, status):
        response = self._decorate(status, ["market_data"])

        assert response.granted_capabilities is None
        assert response.remembered_capabilities == ["market_data"]

    def test_granting_nothing_is_remembered_as_nothing_not_as_unanswered(self):
        """``[]`` and ``None`` are different answers on both fields: one is a
        user who declined every group, the other is nobody having been asked."""
        response = self._decorate("needs_reauth", [])

        assert response.remembered_capabilities == []

    def test_a_connection_that_was_never_asked_remembers_nothing(self):
        response = self._decorate("connected", None)

        assert response.granted_capabilities is None
        assert response.remembered_capabilities is None

