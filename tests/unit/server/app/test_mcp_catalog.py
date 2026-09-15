"""Tests for the user MCP catalog router (app/mcp_catalog.py) and the three
routers that share its prefix (builtins, brokerages, icons).

Covers list/get/create/update/delete, 409 on duplicate, 404 on missing, the
name-mismatch guard on PUT, and that the owner-scoped responses echo the stored
env/header maps verbatim so an edit round-trips them.
"""

from __future__ import annotations

import asyncio
from contextlib import ExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

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
        # Every catalog SELECT projects it, and background discovery now reads
        # it before it dials: a row without it is a row nothing probes.
        "enabled": True,
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


@pytest.fixture(autouse=True)
def _probe_kick_always_claimed():
    """Background discovery claims its kick in Postgres before dialling, and
    these tests have no pool. The claim answers with the stamp it wrote, which
    the probe then fences its write on; the throttle itself is pinned in
    test_mcp_discovery_schedule.py."""
    with patch(
        "src.server.services.mcp_oauth.discovery.claim_probe_kick",
        new=AsyncMock(return_value=datetime.now(UTC)),
    ):
        yield


@pytest_asyncio.fixture
async def client():
    from src.server.app.mcp_brokerages import router as brokerages
    from src.server.app.mcp_builtin import router as builtin
    from src.server.app.mcp_catalog import router
    from src.server.app.mcp_icons import router as icons

    app = create_test_app(router, builtin, brokerages, icons)
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


@pytest.mark.parametrize("enabled,kicks", [(False, 0), (True, 1)])
@pytest.mark.asyncio
async def test_create_kicks_the_probe_only_for_a_row_that_lands_on(
    client, enabled, kicks
):
    """Background discovery refuses a row that is switched off, so a kick for
    one spends its throttle stamp and returns nothing, while the page reads
    the stamp as a check in flight and shows it for the whole window. The
    enable toggle is what earns such a row its first probe.
    """
    from src.server.app import mcp_catalog

    with (
        patch.object(
            mcp_catalog,
            "create_catalog_server",
            new=AsyncMock(return_value=_row(name="new_server", enabled=enabled)),
        ),
        patch.object(
            mcp_catalog, "get_connection", new=AsyncMock(return_value=None)
        ),
        patch.object(mcp_catalog, "schedule_catalog_discovery") as sched,
    ):
        resp = await client.post(
            "/api/v1/mcp/servers",
            json={
                "name": "new_server",
                "transport": "http",
                "url": "https://api.example.com/mcp",
            },
        )

    assert resp.status_code == 201
    assert sched.call_count == kicks


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

    pending = list(mc._discovery_tasks)
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
            "src.server.services.mcp_oauth.discovery.discover_catalog_server",
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
    refresh.assert_awaited_once_with("test-user-123", "remote_server", claimed_at=ANY)
    resync.assert_awaited_once_with("test-user-123")


@pytest.mark.asyncio
async def test_update_skips_rediscovery_when_fingerprint_is_unchanged(client):
    """Prompt-only edits (description) leave the fingerprint alone — the cached
    snapshot still serves, so no discovery round-trip is spent."""
    refresh = AsyncMock(return_value={"status": "ok"})
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
            "src.server.services.mcp_oauth.discovery.discover_catalog_server",
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
            "src.server.services.mcp_oauth.discovery.discover_catalog_server",
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
            "src.server.services.mcp_oauth.discovery.discover_catalog_server",
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
async def _toggle_patches(*, connection, row=None):
    row = {"name": "remote_server", "transport": "http", "enabled": True} if row is None else row
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


@pytest.mark.asyncio
async def test_enable_kicks_the_probe_an_inert_row_never_got(client):
    """The other half of the switch gate: nothing dials a row before the user
    switches it on, so the switch is what earns the row its first verdict.

    Unthrottled on purpose. The kick the create or the import spent left a
    ``probe_kicked_at`` stamp and no snapshot, and a throttled kick lands
    inside that stamp's window, so the row would stay blank until a self-heal
    two minutes later.
    """
    from src.server.app import mcp_catalog

    async with _toggle_patches(connection=None):
        with patch.object(mcp_catalog, "schedule_catalog_discovery") as sched:
            resp = await client.patch(
                "/api/v1/mcp/servers/remote_server/enabled", json={"enabled": True}
            )
    assert resp.status_code == 200
    assert sched.call_args_list == [
        call("test-user-123", "remote_server", reason="enable")
    ]


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


@pytest.mark.asyncio
async def test_import_refuses_a_value_the_vault_rules_reject(client, _import_txn):
    """An extracted literal never passes through ``CreateSecretRequest``.

    The model sees the ``${vault:NAME}`` ref the extraction put in its place,
    so without the check here a value no sink can hold lands in the vault and
    every server it feeds afterwards fails at its own boundary instead.
    """
    async with _import_patches() as mocks:
        resp = await client.post(
            "/api/v1/mcp/servers/import",
            json={
                "mcpServers": {
                    "srv-one": {
                        "type": "http",
                        "url": "https://api.example.com/a",
                        "headers": {
                            "Authorization": "EXAMPLE-OPAQUE-TOKEN-AAAAAAAAAA\x00"
                        },
                    }
                }
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] == 0
    assert body["secrets_created"] == []
    assert body["results"][0]["status"] == "error"
    mocks["create_user_secret"].assert_not_awaited()


@pytest.mark.asyncio
async def test_import_keeps_a_pasted_trailing_newline(client, _import_txn):
    """Storage no longer rules on what a header can frame.

    A key pasted with its newline is the key, and the header that carries it
    trims the newline at resolution; refusing the whole import here cost the
    user an entry over a byte nothing downstream would have sent.
    """
    async with _import_patches() as mocks:
        resp = await client.post(
            "/api/v1/mcp/servers/import",
            json={
                "mcpServers": {
                    "srv-one": {
                        "type": "http",
                        "url": "https://api.example.com/a",
                        "headers": {
                            "Authorization": "EXAMPLE-OPAQUE-TOKEN-AAAAAAAAAA\n"
                        },
                    }
                }
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] == 1
    assert len(body["secrets_created"]) == 1
    assert mocks["create_user_secret"].await_count == 1


async def _settle_discovery() -> None:
    """Await whatever background discovery the request just scheduled.

    ``schedule_catalog_discovery`` registers its task synchronously, so every
    kick a request made is in ``_in_flight`` by the time its response is back.
    """
    from src.server.services.mcp_oauth import discovery

    for task in list(discovery._in_flight.values()):
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("enabled,dials", [(False, 0), (True, 1)])
@pytest.mark.asyncio
async def test_import_dials_only_a_row_the_user_switched_on(
    client, _import_txn, enabled, dials
):
    """The discovery pass refuses to dial a row that is switched off.

    An inert row never reaches the wire: the pass reads it and refuses, so the
    credential this same import just wrote into the vault does not travel to
    the address the pasted file named. The enabled half is the identical code
    path and is what says the refusal is the switch rather than a kick that
    never fires: there the resolved value goes out.

    The writer here hands back an enabled row, so the import route's own gate
    passes and the refusal under test is the pass's own; the route gate is
    pinned separately below.
    """
    from src.server.services.mcp_oauth import discovery

    probe = AsyncMock()
    secret = "EXAMPLE-OPAQUE-TOKEN-AAAAAAAAAA"
    with (
        patch.object(
            discovery,
            "get_catalog_server",
            new=AsyncMock(return_value=_row("srv-one", enabled=enabled)),
        ),
        patch.object(discovery, "get_connection", new=AsyncMock(return_value=None)),
        patch.object(
            discovery,
            "get_user_secrets_decrypted",
            new=AsyncMock(return_value={"API_KEY": secret}),
        ),
        patch.object(discovery, "bounded_probe", new=probe),
    ):
        async with _import_patches(after_secret_change=AsyncMock()):
            resp = await client.post(
                "/api/v1/mcp/servers/import",
                json={
                    "mcpServers": {
                        "srv-one": {
                            "type": "http",
                            "url": "https://api.example.com/a",
                            "headers": {"Authorization": secret},
                        }
                    }
                },
            )
            assert resp.status_code == 200
            assert resp.json()["created"] == 1
            await _settle_discovery()

    assert probe.await_count == dials
    if dials:
        assert probe.await_args.args[1]["Authorization"] == secret


@pytest.mark.asyncio
async def test_import_does_not_kick_rows_that_land_switched_off(client, _import_txn):
    """The other end of the same gate: an imported row lands inert, nothing
    will dial it, and the kick would only stamp the throttle clock and leave
    the page reporting a check nothing is making."""
    from src.server.app import mcp_catalog

    created = AsyncMock(side_effect=lambda u, n, **kw: _row(n, enabled=False))
    async with _import_patches(create_catalog_server=created):
        with patch.object(mcp_catalog, "schedule_catalog_discovery") as sched:
            resp = await client.post(
                "/api/v1/mcp/servers/import",
                json={
                    "mcpServers": {
                        "srv-one": {"type": "http", "url": "https://api.example.com/a"},
                        "srv-two": {"command": "npx", "args": ["-y", "@foo/bar"]},
                    }
                },
            )

    assert resp.status_code == 200
    assert resp.json()["created"] == 2
    sched.assert_not_called()


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


def test_the_catalog_select_projects_the_probe_kick_clock():
    """``_catalog_row_to_dict`` indexes it, so a SELECT that stopped naming the
    column would KeyError every catalog read rather than quietly returning
    null."""
    from src.server.database.mcp_servers import _CATALOG_SELECT

    assert "s.probe_kicked_at" in _CATALOG_SELECT


class TestSelfHealKick:
    """Which rows the list route re-probes, and which it leaves alone."""

    def _kicked(self, rows, snapshots) -> list[str]:
        from src.server.app import mcp_catalog

        with patch.object(mcp_catalog, "schedule_catalog_discovery") as sched:
            mcp_catalog._kick_unprobed("u1", rows, snapshots)
        return [c.args[1] for c in sched.call_args_list]

    def test_a_backfilled_empty_verdict_still_counts_as_unprobed(self):
        """Migration 044 backfills ``last_probe`` with ``{}``, which reads back
        as no verdict at all. Without this the row has a snapshot, so it never
        earned one after deploy and its header grant was never issued."""
        rows = [_row(name="acme")]

        assert self._kicked(rows, {"acme": {"last_probe": {}}}) == ["acme"]

    def test_an_unreachable_verdict_is_retried(self):
        rows = [_row(name="acme")]
        snapshots = {"acme": {"last_probe": {"verdict": "unreachable"}}}

        assert self._kicked(rows, snapshots) == ["acme"]

    def test_a_verdict_the_server_gave_is_left_alone(self):
        rows = [_row(name="acme")]
        snapshots = {"acme": {"last_probe": {"verdict": "needs_credential"}}}

        assert self._kicked(rows, snapshots) == []

    def test_a_switched_off_row_is_left_alone(self):
        """An inert template has no business on the wire, and the pass refuses
        it anyway: kicking from here spends a task and the row's throttle
        stamp, and the stamp is what the switch's own kick then lands inside.
        """
        rows = [_row(name="acme", enabled=False)]

        assert self._kicked(rows, {}) == []

    def test_a_row_of_a_disabled_plugin_is_left_alone(self):
        """The plugin switch withholds the row from every runtime without
        touching the row's own flag, so it is as inert as a switched-off row
        and its credential has the same claim to stay off the wire."""
        rows = [_row(name="acme", plugin_name="bundle", plugin_enabled=False)]

        assert self._kicked(rows, {}) == []

    def test_a_row_kicked_seconds_ago_is_not_kicked_again(self):
        """The pre-check in front of ``claim_probe_kick``: a polling list must
        not spawn a task per remote row just to be told no in Postgres."""
        recent = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
        rows = [_row(name="acme", probe_kicked_at=recent)]

        assert self._kicked(rows, {}) == []

    def test_a_stale_kick_clock_does_not_hold_the_row_back(self):
        old = (datetime.now(UTC) - timedelta(seconds=600)).isoformat()
        rows = [_row(name="acme", probe_kicked_at=old)]

        assert self._kicked(rows, {}) == ["acme"]

    def test_an_sse_row_has_no_host_side_path(self):
        """The probe dials streamable HTTP, so a legacy row would only ever
        earn an error snapshot here; it keeps its in-sandbox discovery."""
        rows = [_row(name="legacy", transport="sse")]

        assert self._kicked(rows, {}) == []


# ---------------------------------------------------------------------------
# POST probe: the add form's answer, computed once on the host
# ---------------------------------------------------------------------------


def _probe_patches(outcome=None, secrets=None):
    """Patch the caller's vault and the socket, but nothing between them.

    The seam is the dial itself, so the gate, the ``sent_credential`` flag the
    verdict turns on, and the route's own header resolution all still run.
    """
    from src.server.services.mcp_probe import ProbeOutcome

    probe = AsyncMock(
        return_value=outcome or ProbeOutcome(ok=True, auth="none", tools=[])
    )
    stack = ExitStack()
    stack.enter_context(
        patch(
            "src.server.app.mcp_catalog.effective_secrets_for_probe",
            new=AsyncMock(return_value=secrets or {}),
        )
    )
    stack.enter_context(patch("src.server.services.mcp_probe._probe", new=probe))
    return stack, probe


@pytest.mark.asyncio
async def test_probe_answers_with_the_full_verdict_shape(client):
    """The wire contract the add form and the catalog row both read. Every key
    is present on every answer, so a client never has to tell "absent" from
    "false"."""
    stack, _probe = _probe_patches()
    with stack:
        resp = await client.post(
            "/api/v1/mcp/servers/probe", json={"url": "https://api.example.com/mcp"}
        )

    assert resp.status_code == 200
    assert set(resp.json()) == {
        "verdict", "tools", "server_info", "error",
        "http_status", "missing_secrets", "probed_at",
    }


@pytest.mark.asyncio
async def test_probe_previews_the_tools_it_saw(client):
    from src.server.services.mcp_probe import ProbeOutcome

    stack, _probe = _probe_patches(
        ProbeOutcome(
            ok=True, auth="none",
            tools=[{"name": "quote", "description": "One quote", "input_schema": {}}],
        )
    )
    with stack:
        resp = await client.post(
            "/api/v1/mcp/servers/probe", json={"url": "https://api.example.com/mcp"}
        )

    body = resp.json()
    assert body["verdict"] == "ok"
    assert body["tools"] == [{"name": "quote", "description": "One quote"}]


@pytest.mark.asyncio
async def test_probe_tells_a_rejected_key_from_an_unconnected_server(client):
    """Both are a bare 401 on the wire. Only this process knows a credential was
    sent, which is why the verdict is computed here and not in the client."""
    from src.server.services.mcp_probe import ProbeOutcome

    challenge = ProbeOutcome(ok=False, auth="credential", http_status=401)

    stack, _ = _probe_patches(challenge)
    with stack:
        anonymous = await client.post(
            "/api/v1/mcp/servers/probe", json={"url": "https://api.example.com/mcp"}
        )
    stack, _ = _probe_patches(challenge, secrets={"API_KEY": "sk-live"})
    with stack:
        credentialed = await client.post(
            "/api/v1/mcp/servers/probe",
            json={
                "url": "https://api.example.com/mcp",
                "headers": {"Authorization": "Bearer ${vault:API_KEY}"},
            },
        )

    assert anonymous.json()["verdict"] == "needs_credential"
    assert credentialed.json()["verdict"] == "credential_rejected"


@pytest.mark.asyncio
async def test_probe_resolves_a_vault_ref_before_dialling(client):
    stack, probe = _probe_patches(secrets={"API_KEY": "sk-live"})
    with stack:
        await client.post(
            "/api/v1/mcp/servers/probe",
            json={
                "url": "https://api.example.com/mcp",
                "headers": {"Authorization": "Bearer ${vault:API_KEY}"},
            },
        )

    assert probe.await_args.args[1] == {"Authorization": "Bearer sk-live"}


@pytest.mark.asyncio
async def test_probe_names_a_missing_secret_instead_of_dialling(client):
    """Sending the literal ``${vault:...}`` string would come back a rejected
    key, so a ref with no value is answered without a round trip."""
    stack, probe = _probe_patches()
    with stack:
        resp = await client.post(
            "/api/v1/mcp/servers/probe",
            json={
                "url": "https://api.example.com/mcp",
                "headers": {"Authorization": "Bearer ${vault:API_KEY}"},
            },
        )

    body = resp.json()
    assert body["verdict"] == "missing_secrets"
    assert body["missing_secrets"] == ["API_KEY"]
    assert probe.await_count == 0


@pytest.mark.asyncio
async def test_probe_decrypts_only_the_secrets_its_headers_name(client):
    """Each vault row is a full S2K derivation, and the header-free probe a
    URL edit fires names none of them. The route asks for the referenced
    names, not the vault, and asks for nothing when there are none."""
    from src.server.services import mcp_probe

    user_vault = AsyncMock(return_value={"API_KEY": "sk-abc"})
    with (
        patch(
            "src.server.database.user_vault_secrets.get_user_secrets_decrypted",
            new=user_vault,
        ),
        patch("src.server.services.mcp_probe._probe", new=AsyncMock(
            return_value=mcp_probe.ProbeOutcome(ok=True, auth="none", tools=[])
        )),
    ):
        resp = await client.post(
            "/api/v1/mcp/servers/probe", json={"url": "https://api.example.com/mcp"}
        )
        assert resp.status_code == 200
        user_vault.assert_not_awaited()

        resp = await client.post(
            "/api/v1/mcp/servers/probe",
            json={
                "url": "https://api.example.com/mcp",
                "headers": {"Authorization": "Bearer ${vault:API_KEY}", "X-B": "${vault:B}"},
            },
        )
        assert resp.status_code == 200
    user_vault.assert_awaited_once_with("test-user-123", ["API_KEY", "B"])


@pytest.mark.asyncio
async def test_a_probe_whose_caller_hung_up_lets_go_of_its_gate_slot():
    """The form aborts a superseded probe; a dropped connection cancels nothing
    on its own. Polled disconnect is what stops the abandoned dial from holding
    a slot for its whole budget."""
    from fastapi import HTTPException

    from src.server.app.mcp_catalog import _unless_gone

    released = asyncio.Event()

    async def _slow():
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            released.set()
            raise

    class _Gone:
        """Present for the first poll, so the dial is on the wire when the
        caller goes; gone from the second."""

        polls = 0

        async def is_disconnected(self):
            self.polls += 1
            return self.polls > 1

    with patch("src.server.app.mcp_catalog.DISCONNECT_POLL_S", 0.01):
        with pytest.raises(HTTPException) as exc:
            await _unless_gone(_Gone(), _slow())
    assert exc.value.status_code == 499
    await asyncio.wait_for(released.wait(), 1)


@pytest.mark.asyncio
async def test_a_probe_whose_caller_stayed_answers_normally():
    from src.server.app.mcp_catalog import _unless_gone

    async def _quick():
        return "answer"

    class _Here:
        async def is_disconnected(self):
            return False

    assert await _unless_gone(_Here(), _quick()) == "answer"


@pytest.mark.asyncio
async def test_probe_trims_a_pasted_newline_off_a_resolved_header(client):
    """The vault keeps what was pasted; the header sink decides what it can send.

    A literal header value never reaches here with a newline in it, so this is
    the one path that has to make the call.
    """
    stack, probe = _probe_patches(secrets={"API_KEY": "sk-live\n"})
    with stack:
        await client.post(
            "/api/v1/mcp/servers/probe",
            json={
                "url": "https://api.example.com/mcp",
                "headers": {"Authorization": "Bearer ${vault:API_KEY}"},
            },
        )

    assert probe.await_args.args[1] == {"Authorization": "Bearer sk-live"}


@pytest.mark.asyncio
async def test_probe_refuses_a_resolved_header_that_would_split_the_request(client):
    """An embedded newline is a second header, not a credential.

    Refused before the socket, in the same words httpx would have produced a
    layer down, so the form reads the same whichever side caught it.
    """
    from src.server.services.mcp_probe import INVALID_HEADER_VALUE

    stack, probe = _probe_patches(secrets={"API_KEY": "sk-live\nX-Injected: 1"})
    with stack:
        resp = await client.post(
            "/api/v1/mcp/servers/probe",
            json={
                "url": "https://api.example.com/mcp",
                "headers": {"Authorization": "Bearer ${vault:API_KEY}"},
            },
        )

    body = resp.json()
    assert body["verdict"] == "unreachable"
    assert body["error"] == INVALID_HEADER_VALUE
    assert probe.await_count == 0


@pytest.mark.asyncio
async def test_probe_refuses_a_transport(client):
    """Dropped from the input: the probe dials streamable HTTP whatever the
    form intends to save, so a client still sending one is describing a choice
    this endpoint does not have."""
    stack, _probe = _probe_patches()
    with stack:
        resp = await client.post(
            "/api/v1/mcp/servers/probe",
            json={"url": "https://api.example.com/mcp", "transport": "sse"},
        )

    assert resp.status_code == 422


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
                "src.server.app.mcp_brokerages.get_catalog_server",
                new=AsyncMock(return_value=None),
            ),
            patch("src.server.app.mcp_brokerages.create_catalog_server", new=created),
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
            "src.server.app.mcp_brokerages.get_catalog_server",
            new=AsyncMock(return_value=stored),
        ),
        patch("src.server.app.mcp_catalog.set_catalog_server_enabled", new=toggled),
        patch("src.server.app.mcp_brokerages.create_catalog_server", new=created),
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
            "src.server.app.mcp_brokerages.get_catalog_server",
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
            "src.server.app.mcp_brokerages.get_catalog_server",
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
            "src.server.app.mcp_brokerages.get_catalog_server",
            new=AsyncMock(return_value=None),
        ),
        patch("src.server.app.mcp_brokerages.create_catalog_server", new=created),
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
            "src.server.app.mcp_brokerages.get_catalog_server",
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
    with patch("src.server.app.mcp_brokerages.create_catalog_server", new=created):
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
            "src.server.app.mcp_brokerages.get_catalog_server",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "src.server.app.mcp_brokerages.create_catalog_server",
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
        from src.server.app.mcp_builtin import get_builtin_server_tools

        tool = SimpleNamespace(name="quote", description="d", input_schema={})
        registry = self._registry(price=SimpleNamespace(tools=[tool]))
        with patch("src.server.app.mcp_builtin.builtin_names", return_value={"price"}), \
             patch("ptc_agent.core.mcp_registry.get_global_registry", return_value=registry):
            out = await get_builtin_server_tools("price", "u-1")
        assert out["connected"] is True
        assert [t["name"] for t in out["tools"]] == ["quote"]

    @pytest.mark.asyncio
    async def test_a_connected_builtin_with_no_tools_is_still_connected(self):
        # The genuinely empty case. It has to stay distinguishable from the one
        # below or the fix is pointless.
        from src.server.app.mcp_builtin import get_builtin_server_tools

        registry = self._registry(price=SimpleNamespace(tools=[]))
        with patch("src.server.app.mcp_builtin.builtin_names", return_value={"price"}), \
             patch("ptc_agent.core.mcp_registry.get_global_registry", return_value=registry):
            out = await get_builtin_server_tools("price", "u-1")
        assert out["connected"] is True
        assert out["tools"] == []

    @pytest.mark.asyncio
    async def test_a_builtin_this_worker_never_connected_says_so(self):
        from src.server.app.mcp_builtin import get_builtin_server_tools

        # Configured (so not a 404) but absent from the registry: this is what
        # a dropped connector looks like from here.
        with patch("src.server.app.mcp_builtin.builtin_names", return_value={"price"}), \
             patch("ptc_agent.core.mcp_registry.get_global_registry",
                   return_value=self._registry()):
            out = await get_builtin_server_tools("price", "u-1")
        assert out["connected"] is False
        assert out["tools"] == []

    @pytest.mark.asyncio
    async def test_no_registry_at_all_is_also_unknown_not_empty(self):
        from src.server.app.mcp_builtin import get_builtin_server_tools

        with patch("src.server.app.mcp_builtin.builtin_names", return_value={"price"}), \
             patch("ptc_agent.core.mcp_registry.get_global_registry", return_value=None):
            out = await get_builtin_server_tools("price", "u-1")
        assert out["connected"] is False

    @pytest.mark.asyncio
    async def test_an_unknown_name_is_still_a_404(self):
        from fastapi import HTTPException

        from src.server.app.mcp_builtin import get_builtin_server_tools

        with patch("src.server.app.mcp_builtin.builtin_names", return_value={"price"}):
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
        from src.server.app.mcp_catalog import decorated

        return decorated(
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

    def test_a_group_whose_requirement_was_declined_is_not_drawn_granted(self):
        """The badges read the grant, so it has to be the one the relay enforces:
        live orders stored without account access are refused, and drawn off."""
        from src.server.app.mcp_catalog import decorated
        from src.server.services.brokerages import brokerage_by_name

        response = decorated(
            _row(),
            {
                "status": "connected",
                "server_url": brokerage_by_name("moomoo").url,
                "granted_capabilities": ["market_data", "trading"],
            },
        )

        assert response.granted_capabilities == ["market_data"]
        assert response.remembered_capabilities == ["market_data", "trading"]



# ---------------------------------------------------------------------------
# PATCH binding: refuse only what the request asks for, heal what it carries
# ---------------------------------------------------------------------------

MOOMOO_URL = "https://mcp.moomoo.com/mcp"
ROBINHOOD_URL = "https://agent.robinhood.com/mcp/trading"


@asynccontextmanager
async def _binding_patches(
    *, row, connection=None, read=None, lock=None, rewrite=None
):
    """The write is captured rather than performed; ``update`` records the
    ``updates`` the handler decided on, which is the whole contract here.

    ``read`` replaces the catalog read (it receives the handler's call, so it
    can answer by whether ``conn`` was passed); ``lock`` replaces the egress
    lock; ``rewrite`` replaces the header-grant rewrite. All default to no-ops
    that return ``row``."""

    @asynccontextmanager
    async def _txn():
        yield None

    db = MagicMock(name="db")
    db.transaction = _txn

    @asynccontextmanager
    async def _connection():
        yield db

    async def _update(user_id, name, *, updates, conn=None):
        return {**row, **updates}

    update = AsyncMock(side_effect=_update)
    with (
        patch(
            "src.server.app.mcp_catalog.get_catalog_server",
            new=AsyncMock(side_effect=read or (lambda *a, **k: row)),
        ),
        patch("src.server.app.mcp_catalog.update_catalog_server", new=update),
        patch(
            "src.server.database.egress_grants.lock_user_egress_state",
            new=lock or AsyncMock(),
        ),
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch("src.server.app.mcp_catalog.get_db_connection", new=_connection),
        patch(
            "src.server.database.egress_grants.apply_consent_to_active_grants",
            new=AsyncMock(),
        ),
        patch(
            "src.server.database.egress_grants.apply_binding_to_active_header_grants",
            new=rewrite or AsyncMock(),
        ),
        patch(
            "src.server.app.mcp_catalog._oauth_by_server",
            new=AsyncMock(return_value={}),
        ),
    ):
        yield update


def _written(update) -> dict:
    return update.await_args.kwargs["updates"]


@pytest.mark.asyncio
@pytest.mark.parametrize("asked", ["ptc", "both"])
async def test_binding_refuses_a_live_order_override_off_the_direct_path(client, asked):
    """A live order is a tool call and nothing else, so the write path refuses
    the two answers that would put a wrapper back in the sandbox, and says
    what the tool may be instead."""
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={})
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"tool_binding_set": {"trading_order_place": asked}},
        )
    assert resp.status_code == 422
    assert "direct tool call" in resp.json()["detail"]
    update.assert_not_awaited()


@pytest.mark.asyncio
async def test_binding_accepts_a_live_order_override_that_says_direct(client):
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={})
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"tool_binding_set": {"trading_order_place": "direct"}},
        )
    assert resp.status_code == 200, resp.json()
    assert _written(update) == {"tool_binding": {"trading_order_place": "direct"}}


@pytest.mark.asyncio
async def test_binding_a_retained_disallowed_entry_does_not_lock_the_row(client):
    """A map stored before the clamp existed is not this request's doing: an
    unrelated edit goes through, and the write it produces drops the entry so
    the row stops carrying a setting the resolver reports as ``policy``."""
    row = _row(
        "moomoo",
        url=MOOMOO_URL,
        tool_binding={"trading_order_place": "ptc", "quote_stock_quote": "direct"},
    )
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"binding_preset": "ptc_only"},
        )
    assert resp.status_code == 200, resp.json()
    assert _written(update) == {
        "binding_preset": "ptc_only",
        "tool_binding": {"quote_stock_quote": "direct"},
    }


@pytest.mark.asyncio
async def test_binding_an_edit_heals_a_retained_entry_it_never_mentions(client):
    """A delta naming one tool still heals the row: the clamped entry the
    request never mentions is stripped, and the edit it asked for lands."""
    row = _row(
        "moomoo",
        url=MOOMOO_URL,
        tool_binding={"trading_order_place": "ptc", "quote_stock_quote": "direct"},
    )
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"tool_binding_set": {"quote_stock_quote": "both"}},
        )
    assert resp.status_code == 200, resp.json()
    assert _written(update)["tool_binding"] == {"quote_stock_quote": "both"}


@pytest.mark.asyncio
async def test_binding_an_edit_keeps_another_tool_a_concurrent_write_added(client):
    """Two tabs, two tools. The second write is judged against the row as it
    stands when the lock is taken, so the first tab's edit survives it. The
    body carries no map, so there is nothing stale for it to write back."""
    # The row already carries the other tab's edit by the time this one reads.
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={"quote_stock_quote": "direct"})
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"tool_binding_set": {"quote_cur_kline": "both"}},
        )
    assert resp.status_code == 200, resp.json()
    assert _written(update)["tool_binding"] == {
        "quote_stock_quote": "direct",
        "quote_cur_kline": "both",
    }


@pytest.mark.asyncio
async def test_binding_refuses_a_request_naming_more_tools_than_a_server_has(client):
    from src.server.services.mcp_discovery import MAX_TOOLS_PER_SERVER

    # Not a brokerage row: at a curated vendor a name no group carries is
    # refused for that reason instead, and the cap would go untested.
    row = _row("remote_server")
    async with _binding_patches(row=row):
        resp = await client.patch(
            "/api/v1/mcp/servers/remote_server/binding",
            json={
                "tool_binding_set": {
                    f"quote_t{i}": "ptc" for i in range(MAX_TOOLS_PER_SERVER + 1)
                }
            },
        )
    assert resp.status_code == 422, resp.json()


@pytest.mark.asyncio
async def test_binding_refuses_a_delta_that_grows_the_row_past_the_cap(client):
    """The body is a delta, so the per-request cap alone bounds nothing: a run
    of small writes would accumulate a map no server could ever match."""
    from src.server.services.mcp_discovery import MAX_TOOLS_PER_SERVER

    stored = {f"quote_s{i}": "ptc" for i in range(MAX_TOOLS_PER_SERVER)}
    row = _row("remote_server", tool_binding=stored)
    async with _binding_patches(row=row):
        resp = await client.patch(
            "/api/v1/mcp/servers/remote_server/binding",
            json={"tool_binding_set": {"quote_one_more": "ptc"}},
        )
    assert resp.status_code == 422, resp.json()


@pytest.mark.asyncio
async def test_binding_still_lets_an_oversized_row_be_edited_down(client):
    """The cap is judged on growth, so a row already over the line is not
    frozen out of the write that would shrink it."""
    from src.server.services.mcp_discovery import MAX_TOOLS_PER_SERVER

    stored = {f"quote_s{i}": "ptc" for i in range(MAX_TOOLS_PER_SERVER + 5)}
    row = _row("remote_server", tool_binding=stored)
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/remote_server/binding",
            json={"tool_binding_unset": ["quote_s0"]},
        )
    assert resp.status_code == 200, resp.json()
    written = _written(update)["tool_binding"]
    assert "quote_s0" not in written
    assert len(written) == MAX_TOOLS_PER_SERVER + 4


@pytest.mark.asyncio
async def test_binding_unset_clears_one_tool_and_leaves_the_rest(client):
    row = _row(
        "moomoo",
        url=MOOMOO_URL,
        tool_binding={"quote_stock_quote": "direct", "quote_cur_kline": "both"},
    )
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"tool_binding_unset": ["quote_stock_quote"]},
        )
    assert resp.status_code == 200, resp.json()
    assert _written(update)["tool_binding"] == {"quote_cur_kline": "both"}


@pytest.mark.asyncio
async def test_binding_a_clean_row_is_not_rewritten_for_an_unrelated_edit(client):
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={"quote_stock_quote": "direct"})
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"binding_preset": "ptc_only"},
        )
    assert resp.status_code == 200, resp.json()
    assert _written(update) == {"binding_preset": "ptc_only"}


@pytest.mark.asyncio
async def test_binding_refuses_the_retired_order_direct_preset(client):
    """The switch's off position is a cleared column, not a word. A value the
    resolver would only fall through is refused at the body rather than stored
    and echoed back as if it were a setting."""
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={})
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"binding_preset": "order_direct"},
        )
    assert resp.status_code == 422, resp.json()
    assert not update.await_count


@pytest.mark.asyncio
async def test_binding_heals_from_the_row_read_under_the_lock(client):
    """Two workers, one row. A reads the row for a preset-only edit and stalls;
    B stores an override and commits; A resumes. The map A heals from has to
    be the one B left, or A's write puts the row back to what it saw."""
    before = {"trading_order_place": "ptc"}
    after = {"quote_stock_quote": "ptc"}
    calls: list[str] = []

    async def read(user_id, name, *, conn=None, **_):
        calls.append("read:locked" if conn is not None else "read:unlocked")
        return _row("moomoo", url=MOOMOO_URL, tool_binding=after if conn else before)

    async def lock(conn, user_id):
        calls.append(f"lock:{user_id}")

    row = _row("moomoo", url=MOOMOO_URL, tool_binding=before)
    async with _binding_patches(row=row, read=read, lock=lock) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"binding_preset": None},
        )
    assert resp.status_code == 200, resp.json()
    # B's map is already clean, so a preset-only request writes no map at all.
    assert _written(update) == {"binding_preset": None}
    # The healing read happened under the user's egress lock, and nothing was
    # read off the row before the lock was held.
    assert calls == ["lock:test-user-123", "read:locked"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, expected",
    [
        # A servable connection's address is the identity the token was
        # issued for, so its vendor's rules apply even when the row moved.
        ("connected", 422),
        ("refresh_ambiguous", 422),
        # A dead connection may belong to the host the row used to point at;
        # the row's own address says whose rules apply now.
        ("needs_reauth", 422),
        ("revoked", 422),
    ],
)
async def test_binding_validates_against_the_row_when_the_connection_is_dead(
    client, status, expected
):
    """Row repointed to moomoo, still holding a robinhood connection. Under
    robinhood's curation the moomoo tool name is unknown and would pass; only
    an active connection may say the vendor is still robinhood."""
    from src.server.database.mcp_oauth import ConnectionStatus

    row = _row("my_broker", url=MOOMOO_URL, tool_binding={})
    connection = SimpleNamespace(
        connection_id="c-1",
        server_url=ROBINHOOD_URL if status in ("needs_reauth", "revoked") else MOOMOO_URL,
        status=ConnectionStatus(status),
    )
    async with _binding_patches(row=row, connection=connection):
        resp = await client.patch(
            "/api/v1/mcp/servers/my_broker/binding",
            json={"tool_binding_set": {"trading_order_place": "ptc"}},
        )
    assert resp.status_code == expected, resp.json()


@pytest.mark.asyncio
async def test_binding_a_live_connection_outranks_the_row_url(client):
    """The converse: the row says moomoo but the token in force is robinhood's,
    so robinhood's live-order names are what the write must refuse."""
    from src.server.database.mcp_oauth import ConnectionStatus

    row = _row("my_broker", url=MOOMOO_URL, tool_binding={})
    connection = SimpleNamespace(
        connection_id="c-1",
        server_url=ROBINHOOD_URL,
        status=ConnectionStatus.CONNECTED,
    )
    async with _binding_patches(row=row, connection=connection):
        resp = await client.patch(
            "/api/v1/mcp/servers/my_broker/binding",
            json={"tool_binding_set": {"place_equity_order": "ptc"}},
        )
    assert resp.status_code == 422, resp.json()


@pytest.mark.asyncio
async def test_binding_rewrites_the_rows_own_header_grants(client):
    """A header-authenticated row has no connection, so the consent rewrite
    reaches none of its grants -- and the relay reads the grant, so a sandbox
    already holding one would keep calling a tool this request just moved onto
    the direct path."""
    row = _row("fund_desk", tool_binding={})
    rewrite = AsyncMock()
    async with _binding_patches(row=row, connection=None, rewrite=rewrite):
        resp = await client.patch(
            "/api/v1/mcp/servers/fund_desk/binding",
            json={"tool_binding_set": {"desk_quote": "direct"}},
        )
    assert resp.status_code == 200, resp.json()
    rewrite.assert_awaited_once()
    assert rewrite.await_args.args[:2] == ("test-user-123", "fund_desk")
    # On the caller's connection, so the map and the policy enforcing it land
    # in one transaction.
    assert "conn" in rewrite.await_args.kwargs


@pytest.mark.asyncio
async def test_binding_refuses_direct_on_a_legacy_sse_row(client):
    """The relay dials streamable HTTP, so an ``sse`` row has no address it can
    reach: a tool bound direct there would leave the sandbox set and never earn
    a grant, so the write path refuses it rather than storing a binding that
    takes the tool away from both agents."""
    row = _row("legacy_desk", transport="sse", tool_binding={})
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/legacy_desk/binding",
            json={"tool_binding_set": {"desk_quote": "direct"}},
        )
    assert resp.status_code == 422
    assert "sandbox wrapper" in resp.json()["detail"]
    update.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [True, False])
async def test_binding_writes_the_order_approval_switch(client, value):
    """The resolver reads the column now, so the write is stored rather than
    refused: it is the one setting that decides whether an order stops."""
    row = _row(
        "moomoo",
        url=MOOMOO_URL,
        tool_binding={},
        order_approval={"live": not value},
    )
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"order_approval": {"live": value}, "binding_preset": "ptc_only"},
        )
    assert resp.status_code == 200
    assert update.await_args.kwargs["updates"]["order_approval"] == {"live": value}


@pytest.mark.asyncio
async def test_binding_stores_only_the_modes_someone_set(client):
    """A default nobody chose is not written down, so a later change to it
    reaches this row."""
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={}, order_approval={})
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"order_approval": {"paper": True}},
        )
    assert resp.status_code == 200, resp.json()
    assert update.await_args.kwargs["updates"]["order_approval"] == {"paper": True}


@pytest.mark.asyncio
async def test_binding_merges_one_mode_into_the_stored_map(client):
    """The body is a delta over the modes, so a page flipping paper cannot put
    a live answer another tab stored back to what this worker last read. The
    merge is under the same lock as the tool map, and for the same reason."""
    row = _row(
        "moomoo",
        url=MOOMOO_URL,
        tool_binding={},
        order_approval={"live": False, "paper": False, "staged": True},
    )
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"order_approval": {"paper": True}},
        )
    assert resp.status_code == 200, resp.json()
    assert update.await_args.kwargs["updates"]["order_approval"] == {
        "live": False, "paper": True, "staged": True
    }


@pytest.mark.asyncio
async def test_binding_folds_the_boolean_the_column_used_to_hold(client):
    """A row untouched since the column was one switch names no modes at all.
    The old value is carried as the live answer, and nothing is written for
    the modes it never named."""
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={}, order_approval=False)
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"order_approval": {"paper": True}},
        )
    assert resp.status_code == 200, resp.json()
    assert update.await_args.kwargs["updates"]["order_approval"] == {
        "live": False, "paper": True
    }


@pytest.mark.asyncio
async def test_binding_refuses_a_mode_nothing_declares(client):
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={})
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"order_approval": {"futures": True}},
        )
    assert resp.status_code == 422, resp.json()
    update.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_binding_write_that_says_nothing_about_approval_leaves_it_alone(client):
    """Absent is not False. The body omitting the switches must not rewrite the
    column, or every unrelated binding edit would silently re-arm the gate."""
    row = _row(
        "moomoo", url=MOOMOO_URL, tool_binding={}, order_approval={"live": False}
    )
    async with _binding_patches(row=row) as update:
        resp = await client.patch(
            "/api/v1/mcp/servers/moomoo/binding",
            json={"binding_preset": "ptc_only"},
        )
    assert resp.status_code == 200
    assert "order_approval" not in update.await_args.kwargs["updates"]


# ---------------------------------------------------------------------------
# GET tools: the page reads which paths a tool may take, not a copy of policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tools_carry_the_paths_each_may_take(client):
    """``allowed`` is the set the write path accepts, so the page disables
    exactly what a PATCH would refuse instead of keeping its own list."""
    row = _row("moomoo", url=MOOMOO_URL, tool_binding={}, binding_preset="ptc_only")
    snapshot = {
        "tools": [
            {"name": "trading_order_place", "description": "", "input_schema": {}},
            {"name": "sim_trade_input_order", "description": "", "input_schema": {}},
            {"name": "quote_stock_quote", "description": "", "input_schema": {}},
            {"name": "new_vendor_tool", "description": "", "input_schema": {}},
        ],
        "discovered_at": "2026-01-01T00:00:00+00:00",
    }
    with (
        patch(
            "src.server.app.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=row),
        ),
        patch(
            "src.server.app.mcp_catalog.get_user_tool_schemas",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.mcp_discovery.ToolSnapshotIndex.ok",
            return_value=snapshot,
        ),
    ):
        resp = await client.get("/api/v1/mcp/servers/moomoo/tools")
    assert resp.status_code == 200, resp.json()
    body = resp.json()
    assert body["order_modes"] == ["live", "paper"]
    by_name = {t["name"]: t for t in body["tools"]}
    live = by_name["trading_order_place"]
    assert live["capability"] == "trading"
    assert (live["binding"], live["binding_source"]) == ("direct", "policy")
    assert live["allowed"] == ["direct"]
    assert live["approval"] is True
    assert live["order"] == {"action": "place", "mode": "live"}

    # Same pin, different mode, and the mode is what decides the gate.
    paper = by_name["sim_trade_input_order"]
    assert (paper["binding"], paper["binding_source"]) == ("direct", "policy")
    assert paper["allowed"] == ["direct"]
    assert paper["approval"] is False
    assert paper["order"] == {"action": "place", "mode": "paper"}

    # A curated read keeps every path; one no group names keeps the sandbox.
    assert by_name["quote_stock_quote"]["allowed"] == ["both", "direct", "ptc"]
    assert by_name["new_vendor_tool"]["allowed"] == ["ptc"]
    for name in ("quote_stock_quote", "new_vendor_tool"):
        assert by_name[name]["order"] is None, name
        assert (by_name[name]["binding"], by_name[name]["binding_source"]) == (
            "ptc",
            "preset",
        )


class TestHasDirectTools:
    """Whether the row says it can reach Flash.

    Flash has no sandbox, so a row is reachable from it only through a tool on
    the direct path. The catalog answers it from the snapshot the list already
    holds, so the page does not have to ask per row. A row with no servable
    connection authenticates with its own headers, and the grant for those
    hangs on the probe verdict, so the offer has to hang on it too.
    """

    def _snapshot(self, *names, verdict=None):
        snapshot = {"tools": [{"name": n} for n in names]}
        if verdict is not None:
            snapshot["last_probe"] = {"verdict": verdict}
        return snapshot

    def test_a_row_with_a_directly_bound_tool_says_so(self):
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {"transport": "http", "tool_binding": {"quote_kline": "direct"}}
        conn = {
            "status": "connected",
            "server_url": "https://example.com/mcp",
            "granted_capabilities": [],
        }
        assert _has_direct_tools(row, conn, self._snapshot("quote_kline")) is True

    def test_a_ptc_only_row_does_not(self):
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {"transport": "http", "tool_binding": {}}
        conn = {
            "status": "connected",
            "server_url": "https://example.com/mcp",
            "granted_capabilities": [],
        }
        assert _has_direct_tools(row, conn, self._snapshot("quote_kline")) is False

    def test_a_stdio_row_never_does_whatever_the_map_asks(self):
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {"transport": "stdio", "tool_binding": {"quote_kline": "direct"}}
        conn = {
            "status": "connected",
            "server_url": None,
            "granted_capabilities": [],
        }
        assert _has_direct_tools(row, conn, self._snapshot("quote_kline")) is False

    def test_a_header_authenticated_row_whose_probe_passed_does(self):
        # No connection is not "no credential": a remote row authenticates
        # with its own headers, and the sync grants it the same way once a
        # verdict says those headers work.
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {
            "transport": "http",
            "url": "https://example.com/mcp",
            "tool_binding": {"quote_kline": "direct"},
        }
        snapshot = self._snapshot("quote_kline", verdict="ok_authed")
        assert _has_direct_tools(row, None, snapshot) is True

    @pytest.mark.parametrize(
        "verdict", ["credential_rejected", "missing_secrets", "unreachable"]
    )
    def test_a_header_row_whose_probe_was_refused_does_not(self, verdict):
        """The snapshot upsert never downgrades: a row that listed once keeps
        its tools and its ``ok`` status when a later probe is turned away, and
        only ``last_probe`` moves. Reading the tools alone offered the Flash
        toggle for a row whose header grant the sync then refused to write.
        """
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {
            "transport": "http",
            "url": "https://example.com/mcp",
            "tool_binding": {"quote_kline": "direct"},
        }
        snapshot = self._snapshot("quote_kline", verdict=verdict)
        assert _has_direct_tools(row, None, snapshot) is False

    def test_a_header_row_with_no_verdict_yet_does_not(self):
        """A snapshot written before the verdict had a column reads back as
        nothing having reached the address, which is what the resolver clamps
        on, so the offer waits for the probe rather than leading it."""
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {
            "transport": "http",
            "url": "https://example.com/mcp",
            "tool_binding": {"quote_kline": "direct"},
        }
        assert _has_direct_tools(row, None, self._snapshot("quote_kline")) is False

    def test_an_unconnected_row_with_no_address_does_not(self):
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {"transport": "stdio", "tool_binding": {"quote_kline": "direct"}}
        assert _has_direct_tools(row, None, self._snapshot("quote_kline")) is False

    def test_a_revoked_connection_leaves_the_row_to_its_own_headers(self):
        """The relay and the resolver both read a revoked record as history
        rather than a claim on the row: the user disconnected, and the headers
        the row carries may authenticate it now. Answering no here hid the
        Flash toggle for a row the sync was granting."""
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {
            "transport": "http",
            "url": "https://example.com/mcp",
            "tool_binding": {"quote_kline": "direct"},
        }
        conn = {
            "status": "revoked",
            "server_url": "https://example.com/mcp",
            "granted_capabilities": [],
        }
        snapshot = self._snapshot("quote_kline", verdict="ok_authed")
        assert _has_direct_tools(row, conn, snapshot) is True

    def test_a_connection_needing_repair_still_does_not(self):
        """Unlike a revoked one: the row is still claimed, the headers are not
        sent while it is, and the sync binds no grant until it is reconnected.
        """
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {
            "transport": "http",
            "url": "https://example.com/mcp",
            "tool_binding": {"quote_kline": "direct"},
        }
        conn = {
            "status": "needs_reauth",
            "server_url": "https://example.com/mcp",
            "granted_capabilities": [],
        }
        snapshot = self._snapshot("quote_kline", verdict="ok_authed")
        assert _has_direct_tools(row, conn, snapshot) is False

    def test_a_map_naming_a_tool_the_server_never_published_does_not(self):
        # The plan carries every name curation or the map grants; only a
        # published schema can actually be bound, so the offer follows the
        # snapshot rather than the plan.
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {"transport": "http", "tool_binding": {"quote_kline": "direct"}}
        conn = {
            "status": "connected",
            "server_url": "https://example.com/mcp",
            "granted_capabilities": [],
        }
        assert _has_direct_tools(row, conn, self._snapshot("something_else")) is False

    def test_a_row_with_no_snapshot_yet_does_not(self):
        from src.server.app.mcp_catalog import _has_direct_tools

        row = {"transport": "http", "tool_binding": {"quote_kline": "direct"}}
        conn = {
            "status": "connected",
            "server_url": "https://example.com/mcp",
            "granted_capabilities": [],
        }
        assert _has_direct_tools(row, conn, None) is False


class TestRelayExecutionWarning:
    """Activation is the moment to say a deployment cannot run this row.

    The relay carries two shapes: an OAuth connection, whose token only it
    spends, and an ``http`` row with a tool on the direct path. Warning on the
    connection alone left the second reporting a clean save while the sync
    bound nothing, so the tool was gone from both agents with nothing said.
    """

    @asynccontextmanager
    async def _patches(self, *, row=None, connection=None, secret=""):
        with (
            patch("src.server.app.setup.agent_config", MagicMock()),
            patch("src.config.env.EGRESS_RELAY_SECRET", secret),
            patch(
                "src.server.app.mcp_catalog.get_connection",
                new=AsyncMock(return_value=connection),
            ),
            patch(
                "src.server.app.mcp_catalog.get_catalog_server",
                new=AsyncMock(return_value=row),
            ),
        ):
            yield

    async def _warning(self, **kwargs):
        from src.server.app.mcp_catalog import _relay_execution_warning

        async with self._patches(**kwargs):
            return await _relay_execution_warning("test-user-123", "fund_desk")

    @pytest.mark.asyncio
    async def test_a_header_row_with_a_direct_tool_warns(self):
        row = _row("fund_desk", tool_binding={"desk_quote": "direct"})
        warning = await self._warning(row=row)
        assert warning and "EGRESS_RELAY_SECRET" in warning

    @pytest.mark.asyncio
    async def test_a_header_row_with_nothing_on_the_direct_path_says_nothing(self):
        row = _row("fund_desk", tool_binding={"desk_quote": "ptc"})
        assert await self._warning(row=row) is None

    @pytest.mark.asyncio
    async def test_a_legacy_sse_row_says_nothing(self):
        """Its stored entry is clamped back to the sandbox, so the relay is
        not what its tools run through and the warning is not its answer."""
        row = _row("fund_desk", transport="sse", tool_binding={"desk_quote": "direct"})
        assert await self._warning(row=row) is None

    @pytest.mark.asyncio
    async def test_a_revoked_connection_leaves_the_row_to_its_own_binding(self):
        from src.server.database.mcp_oauth import ConnectionStatus

        revoked = MagicMock(status=ConnectionStatus.REVOKED)
        ptc_only = _row("fund_desk", tool_binding={"desk_quote": "ptc"})
        assert await self._warning(row=ptc_only, connection=revoked) is None
        direct = _row("fund_desk", tool_binding={"desk_quote": "direct"})
        warning = await self._warning(row=direct, connection=revoked)
        assert warning and "EGRESS_RELAY_SECRET" in warning

    @pytest.mark.asyncio
    async def test_an_oauth_connection_warns_without_reading_the_row(self):
        warning = await self._warning(row=None, connection=MagicMock())
        assert warning and "EGRESS_RELAY_SECRET" in warning
