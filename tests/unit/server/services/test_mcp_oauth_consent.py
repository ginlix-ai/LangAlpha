"""The consent binding between a stored token and the URL it may be sent to.

Two halves of one rule: ``revoke_if_consent_moved`` drops the connection when a
catalog write moves the server, and ``refresh_user_tool_schemas`` re-checks the
binding at the moment the bearer meets the URL — the write paths and the
refresh are not atomic with each other, so the read side cannot assume.

The same non-atomicity runs the other way at the END of a discovery: a
disconnect can commit while we are on the network, so the final section pins
that a write the database guard refused is never dressed up as a fresh
snapshot, and above all never fans one out.
"""

from __future__ import annotations

from contextlib import ExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.mcp_oauth import ConnectionStatus
from src.server.database.mcp_tool_schemas import SchemaWrite
from src.server.services.mcp_oauth.discovery import refresh_user_tool_schemas
from src.server.utils.egress_guard import PinnedTarget
from src.server.services.mcp_oauth.lifecycle import (
    TokenUnavailable,
    revoke_if_consent_moved,
)

USER = "user-1"
SERVER = "remote_server"
CONSENTED = "https://api.example.com/mcp"


def _connection(server_url=CONSENTED, status=ConnectionStatus.CONNECTED):
    return SimpleNamespace(
        connection_id="c-1", server_url=server_url, status=status
    )


def _catalog_row(url=CONSENTED):
    return {
        "name": SERVER,
        "transport": "http",
        "command": None,
        "args": [],
        "url": url,
        "env": {},
        "headers": {},
        "description": "d",
        "instruction": "i",
        "tool_exposure_mode": "summary",
    }


class _FakeTxn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@asynccontextmanager
async def _fake_db_conn(conn=None):
    """The write path opens one transaction to fence on the catalog row."""
    yield SimpleNamespace(transaction=_FakeTxn)


_DB = ("src.server.services.mcp_oauth.discovery.get_db_connection", _fake_db_conn)


# ---------------------------------------------------------------------------
# revoke_if_consent_moved — the write side
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, transport, url, expected",
    [
        (None, "http", "https://moved.example.com/mcp", False),
        (
            _connection(status=ConnectionStatus.REVOKED),
            "http",
            "https://moved.example.com/mcp",
            False,
        ),
        (_connection(), "http", CONSENTED, False),
        # Canonicalization: default port and trailing slash are the same host.
        (_connection(), "http", "https://API.example.com:443/mcp/", False),
        (_connection(), "http", "https://moved.example.com/mcp", True),
        (_connection(), "sse", "https://moved.example.com/mcp", True),
        # A corrupted port must compare (unequal ⇒ revoke; equal-garbage ⇒
        # keep), never raise — one bad row would otherwise 500 every consumer.
        (_connection(), "http", "https://api.example.com:99999/mcp", True),
        (
            _connection("https://api.example.com:99999/mcp"),
            "http",
            "https://api.example.com:99999/mcp",
            False,
        ),
        # No remote endpoint left to serve — consent cannot survive it.
        (_connection(), "stdio", None, True),
        # A needs_reauth connection is still live enough to be worth revoking.
        (
            _connection(status=ConnectionStatus.NEEDS_REAUTH),
            "http",
            "https://moved.example.com/mcp",
            True,
        ),
    ],
)
async def test_revoke_if_consent_moved(connection, transport, url, expected):
    disconnect = AsyncMock(return_value=True)
    with (
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.services.mcp_oauth.lifecycle.disconnect_server",
            new=disconnect,
        ),
    ):
        moved = await revoke_if_consent_moved(
            USER, SERVER, transport=transport, url=url
        )
    assert moved is expected
    assert disconnect.await_count == (1 if expected else 0)


# ---------------------------------------------------------------------------
# refresh_user_tool_schemas — the read side
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_rejects_a_moved_url_before_using_the_token():
    """The bearer was issued for the consented host: a row that has since moved
    must fail closed (409 needs_reauth), not carry the token to the new one."""
    token = AsyncMock()
    with (
        patch(
            "src.server.services.mcp_oauth.discovery.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row("https://moved.example.com/mcp")),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.get_connection",
            new=AsyncMock(return_value=_connection()),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.ensure_fresh_access_token",
            new=token,
        ),
    ):
        with pytest.raises(TokenUnavailable) as e:
            await refresh_user_tool_schemas(USER, SERVER)
    assert e.value.reason == "needs_reauth"
    token.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", [ConnectionStatus.REVOKED, ConnectionStatus.NEEDS_REAUTH]
)
async def test_refresh_refuses_a_non_servable_connection(status):
    """A connection the user has to repair is the caller's answer (409).

    Letting it through to record an ``error`` snapshot instead would report a
    credential problem as a broken tool surface, and the row it writes cannot
    be un-written by anything short of a successful re-discovery.
    """
    token = AsyncMock()
    upsert = AsyncMock()
    with (
        patch(
            "src.server.services.mcp_oauth.discovery.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row()),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.get_connection",
            new=AsyncMock(return_value=_connection(status=status)),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.ensure_fresh_access_token",
            new=token,
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.upsert_user_tool_schemas",
            new=upsert,
        ),
    ):
        with pytest.raises(TokenUnavailable) as e:
            await refresh_user_tool_schemas(USER, SERVER)
    assert e.value.reason == status.value
    token.assert_not_awaited()
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_still_runs_for_an_ambiguous_connection():
    # refresh_ambiguous is servable: the old access token keeps working until
    # it expires, so re-discovery is still worth attempting.
    upsert = AsyncMock(
        return_value=SchemaWrite({"server_name": SERVER, "status": "error"})
    )
    with (
        patch(_DB[0], new=_DB[1]),
        patch(
            "src.server.services.mcp_oauth.discovery.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row()),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.get_connection",
            new=AsyncMock(
                return_value=_connection(status=ConnectionStatus.REFRESH_AMBIGUOUS)
            ),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.ensure_fresh_access_token",
            new=AsyncMock(side_effect=TokenUnavailable("expired")),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.upsert_user_tool_schemas",
            new=upsert,
        ),
    ):
        row = await refresh_user_tool_schemas(USER, SERVER)
    assert row["status"] == "error"


@pytest.mark.asyncio
async def test_refresh_accepts_the_consented_url_in_any_spelling():
    """The guard must not fire on canonicalization noise — it stops at the
    token step here (stubbed unavailable), which is past the consent check."""
    upsert = AsyncMock(
        return_value=SchemaWrite({"server_name": SERVER, "status": "error"})
    )
    with (
        patch(_DB[0], new=_DB[1]),
        patch(
            "src.server.services.mcp_oauth.discovery.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row("https://API.example.com:443/mcp/")),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.get_connection",
            new=AsyncMock(return_value=_connection()),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.ensure_fresh_access_token",
            new=AsyncMock(side_effect=TokenUnavailable("expired")),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.upsert_user_tool_schemas",
            new=upsert,
        ),
    ):
        row = await refresh_user_tool_schemas(USER, SERVER)
    assert row["status"] == "error"
    assert "token unavailable: expired" in upsert.await_args.kwargs["error"]


# ---------------------------------------------------------------------------
# refresh_user_tool_schemas — a disconnect that lands mid-discovery
# ---------------------------------------------------------------------------


class _FakeTool:
    name = "search"
    description = "d"
    input_schema = {"type": "object"}


class _FakeClient:
    """Stands in for the SDK session: one tool, no network."""

    def __init__(self, transport):
        self._transport = transport

    async def __aenter__(self):
        return SimpleNamespace(
            list_tools=AsyncMock(return_value=SimpleNamespace(tools=[_FakeTool()])),
            # A real session always exposes this, so the stand-in does too.
            server_info=None,
        )

    async def __aexit__(self, *exc):
        return False


@asynccontextmanager
async def _fake_http_client(*args, **kwargs):
    yield SimpleNamespace(follow_redirects=False)


def _network_returning_one_tool(*, upsert, bump, catalog=None):
    """Everything from the entry gates to the write stubbed out, so the write
    and the fan-out are the only observable effects of a successful run."""
    stack = ExitStack()
    for target, new in (
        _DB,
        ("src.server.services.mcp_oauth.discovery.get_catalog_server",
         catalog or AsyncMock(return_value=_catalog_row())),
        ("src.server.services.mcp_oauth.discovery.get_connection",
         AsyncMock(return_value=_connection())),
        ("src.server.services.mcp_oauth.discovery.ensure_fresh_access_token",
         AsyncMock(return_value=SimpleNamespace(header=lambda: "Bearer t"))),
        # Function-scope imports in discovery read these module attributes at
        # call time, so patching the source modules covers both.
        ("src.server.utils.egress_guard.pin_public_url",
         AsyncMock(return_value=PinnedTarget(
             url="https://203.0.113.7/mcp", host="mcp.example.com",
             ip="203.0.113.7", authority="mcp.example.com",
         ))),
        ("src.server.services.mcp_oauth.http.pinned_discovery_client",
         _fake_http_client),
        ("src.server.services.mcp_oauth.discovery.streamable_http_client",
         lambda *a, **k: object()),
        ("src.server.services.mcp_oauth.discovery.Client", _FakeClient),
        ("src.server.services.mcp_oauth.discovery.get_user_tool_schemas",
         AsyncMock(return_value=[])),
        ("src.server.services.mcp_oauth.discovery.bump_user_workspaces_mcp_version",
         bump),
        ("src.server.services.mcp_oauth.discovery.upsert_user_tool_schemas",
         upsert),
    ):
        stack.enter_context(patch(target, new=new))
    return stack


@pytest.mark.asyncio
async def test_refresh_never_bumps_when_the_snapshot_write_was_refused():
    """The whole point of the guard: a disconnect committed during the ~40s
    network phase already purged both tiers, so this write would resurrect an
    ``ok`` snapshot — and the bump is what pushes it to every workspace."""
    upsert = AsyncMock(return_value=SchemaWrite(None, "revoked"))
    bump = AsyncMock()
    with _network_returning_one_tool(upsert=upsert, bump=bump):
        with pytest.raises(TokenUnavailable) as e:
            await refresh_user_tool_schemas(USER, SERVER)

    assert e.value.reason == "revoked"
    bump.assert_not_awaited()
    assert upsert.await_args.kwargs["connection_id"] == "c-1"


@pytest.mark.asyncio
async def test_refresh_reports_a_vanished_connection_as_unknown():
    """No connection row left (the catalog-delete variant): the database layer
    has no status to hand back, and 409 "unknown_connection" is the same answer
    the entry gate gives."""
    upsert = AsyncMock(return_value=SchemaWrite(None, None))
    bump = AsyncMock()
    with _network_returning_one_tool(upsert=upsert, bump=bump):
        with pytest.raises(TokenUnavailable) as e:
            await refresh_user_tool_schemas(USER, SERVER)

    assert e.value.reason == "unknown_connection"
    bump.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_writes_and_bumps_when_the_connection_survives():
    """The guard is conditional, not a brake: an uncontended discovery still
    caches its snapshot and fans the version bump out."""
    cached = {"server_name": SERVER, "status": "ok", "tools": [], "schema_digest": "d"}
    upsert = AsyncMock(return_value=SchemaWrite(cached))
    bump = AsyncMock()
    with _network_returning_one_tool(upsert=upsert, bump=bump):
        row = await refresh_user_tool_schemas(USER, SERVER)

    assert row is cached
    bump.assert_awaited_once()
    kwargs = upsert.await_args.kwargs
    assert kwargs["connection_id"] == "c-1"
    assert kwargs["status"] == "ok"
    assert [t["name"] for t in kwargs["tools"]] == ["search"]


@pytest.mark.asyncio
async def test_refresh_guards_the_error_write_too():
    """The ``error`` write has the same resurrection power: it re-creates a row
    at the current fingerprint, which is enough to make the sandbox dial the
    vendor directly once the connection is gone."""
    upsert = AsyncMock(return_value=SchemaWrite(None, "revoked"))
    with (
        patch(_DB[0], new=_DB[1]),
        patch(
            "src.server.services.mcp_oauth.discovery.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row()),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.get_connection",
            new=AsyncMock(return_value=_connection()),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.ensure_fresh_access_token",
            new=AsyncMock(side_effect=TokenUnavailable("expired")),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery.upsert_user_tool_schemas",
            new=upsert,
        ),
    ):
        with pytest.raises(TokenUnavailable) as e:
            await refresh_user_tool_schemas(USER, SERVER)

    assert e.value.reason == "revoked"
    assert upsert.await_args.kwargs["connection_id"] == "c-1"


# ---------------------------------------------------------------------------
# refresh_user_tool_schemas — an edit that lands mid-discovery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_discards_a_write_after_the_config_moved():
    """A slow discovery finishing after a consent-preserving edit must not
    land: keyed to the old fingerprint, its write would delete the current
    config's snapshot (the edit's own rediscovery) and resurrect a dead one."""
    moved = _catalog_row()
    moved["headers"] = {"X-Extra": "1"}  # fingerprint moves, consent does not
    catalog = AsyncMock(side_effect=[_catalog_row(), moved])
    upsert = AsyncMock()
    bump = AsyncMock()
    with _network_returning_one_tool(upsert=upsert, bump=bump, catalog=catalog):
        with pytest.raises(TokenUnavailable) as e:
            await refresh_user_tool_schemas(USER, SERVER)

    assert e.value.reason == "superseded"
    upsert.assert_not_awaited()
    bump.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_discards_a_write_after_the_row_vanished():
    """Catalog delete mid-discovery: nothing current to fence against, so the
    write is dropped the same way rather than recreating the server's rows."""
    catalog = AsyncMock(side_effect=[_catalog_row(), None])
    upsert = AsyncMock()
    bump = AsyncMock()
    with _network_returning_one_tool(upsert=upsert, bump=bump, catalog=catalog):
        with pytest.raises(TokenUnavailable) as e:
            await refresh_user_tool_schemas(USER, SERVER)

    assert e.value.reason == "superseded"
    upsert.assert_not_awaited()
    bump.assert_not_awaited()


def test_ipv6_hosts_keep_their_brackets():
    """``[H]:8443`` vs ``[H:8443]`` are different endpoints — bracket loss
    would collapse them and let an edit between the two keep the old consent."""
    from src.server.services.mcp_config import same_consented_url

    assert not same_consented_url(
        "https://[2001:db8::1]:8443/mcp", "https://[2001:db8::1:8443]/mcp"
    )
    assert same_consented_url(
        "https://[2001:db8::1]/mcp", "https://[2001:DB8::1]:443/mcp/"
    )
