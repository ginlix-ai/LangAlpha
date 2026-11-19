"""Host-side discovery of organization connectors: one probe per member,
connector and configuration per window across workers, nothing the probe sent
kept in what is stored, and a snapshot dropped when the connector changed
while the probe was out."""

from __future__ import annotations

import json
import logging
from contextlib import ExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.org_keys import org_key
from src.server.services.account_contract import OrgConnector
from src.server.services.mcp_probe import ProbeOutcome
from tests.unit.server.test_org_connector_relay import PLANTED, _AuthService

OD = "src.server.services.mcp_oauth.org_discovery"
OC = "src.server.services.org_connectors"

CRM = OrgConnector(
    id="c-crm", name="crm", url="https://crm.example.test/mcp", auth="shared",
    revision="r7",
)
WIKI = OrgConnector(
    id="c-wiki", name="wiki", url="https://wiki.example.test/mcp", auth="member",
    revision="r1",
)


class _Write(SimpleNamespace):
    pass


@asynccontextmanager
async def _conn():
    conn = MagicMock()

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def cursor():
        yield MagicMock()

    conn.transaction = transaction
    conn.cursor = cursor
    yield conn


def _store():
    """The schema upsert, recording what it was asked to keep."""
    upsert = AsyncMock(
        return_value=_Write(
            row={"status": "ok"}, replaced_digest="", replaced_verdict=None,
            connection_status=None,
        )
    )
    return upsert, (
        patch("src.server.services.mcp_oauth.discovery.upsert_user_tool_schemas", upsert),
        patch(f"{OD}.get_db_connection", lambda: _conn()),
        patch("src.server.services.mcp_oauth.discovery.bump_user_versions", AsyncMock()),
    )


def _probe_answers(outcome):
    """The wire, below the probe's own scrub, answering with ``outcome``."""
    return patch("src.server.services.mcp_probe._probe", AsyncMock(return_value=outcome))


@pytest.mark.asyncio
async def test_nothing_the_probe_sent_is_kept(caplog):
    from src.server.services.mcp_oauth.org_discovery import discover_org_connector

    caplog.set_level(logging.DEBUG)
    echoed = ProbeOutcome(
        ok=True, auth="none",
        tools=[{"name": "lookup", "description": f"called with Bearer {PLANTED}",
                "input_schema": {}}],
        identity={"name": "crm", "version": PLANTED},
    )
    upsert, store = _store()
    with ExitStack() as stack:
        for p in (*store, *_AuthService().patches()):
            stack.enter_context(p)
        stack.enter_context(
            patch(f"{OC}.org_connectors_for", AsyncMock(return_value=(CRM,)))
        )
        stack.enter_context(_probe_answers(echoed))
        await discover_org_connector("u", CRM)
        # And a failure whose message quotes the credential back.
        stack.enter_context(
            _probe_answers(
                ProbeOutcome(ok=False, auth="credential",
                             error=f"401: token Bearer {PLANTED} rejected")
            )
        )
        await discover_org_connector("u", CRM)
    assert [c.args[1] for c in upsert.await_args_list] == [org_key("c-crm")] * 2
    stored = json.dumps([c.kwargs for c in upsert.await_args_list], default=str)
    assert PLANTED not in stored
    assert "[redacted]" in stored
    assert PLANTED not in caplog.text


@pytest.mark.asyncio
async def test_a_refused_credential_is_recorded_without_a_dial():
    from src.server.services.mcp_oauth.org_discovery import discover_org_connector

    upsert, store = _store()
    probe = AsyncMock()
    with ExitStack() as stack:
        for p in (*store, *_AuthService({"detail": "no"}, status=403).patches()):
            stack.enter_context(p)
        stack.enter_context(
            patch(f"{OC}.org_connectors_for", AsyncMock(return_value=(CRM,)))
        )
        stack.enter_context(patch("src.server.services.mcp_probe._probe", probe))
        await discover_org_connector("u", CRM)
    probe.assert_not_awaited()
    assert upsert.await_args.kwargs["status"] == "error"


@pytest.mark.asyncio
async def test_a_connector_changed_mid_probe_is_not_written():
    from dataclasses import replace

    from src.server.services.mcp_oauth.org_discovery import discover_org_connector

    upsert, store = _store()
    with ExitStack() as stack:
        for p in (*store, *_AuthService().patches()):
            stack.enter_context(p)
        stack.enter_context(
            patch(
                f"{OC}.org_connectors_for",
                AsyncMock(return_value=(replace(CRM, revision="r8"),)),
            )
        )
        stack.enter_context(_probe_answers(ProbeOutcome(ok=True, auth="none")))
        assert await discover_org_connector("u", CRM) is None
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_member_who_has_not_signed_in_is_not_probed():
    from src.server.services.mcp_oauth.org_discovery import discover_org_connector

    probe = AsyncMock()
    with (
        patch(f"{OD}.get_connection", AsyncMock(return_value=None)),
        patch("src.server.services.mcp_probe._probe", probe),
    ):
        assert await discover_org_connector("u", WIKI) is None
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_one_probe_per_window_across_workers():
    from src.server.services.mcp_oauth import org_discovery

    held: set[str] = set()

    async def acquire_lock(key, token, ttl_ms):
        assert ttl_ms == org_discovery.ORG_PROBE_THROTTLE_S * 1000
        if key in held:
            return False
        held.add(key)
        return True

    cache = SimpleNamespace(acquire_lock=acquire_lock)
    discover = AsyncMock(return_value=None)
    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch.object(org_discovery, "discover_org_connector", discover),
    ):
        await org_discovery._org_discovery_pass("u", CRM)
        await org_discovery._org_discovery_pass("u", CRM)
        await org_discovery._org_discovery_pass("u2", CRM)
    assert [c.args[0] for c in discover.await_args_list] == ["u", "u2"]
    [key] = [k for k in held if k.startswith("org_connectors:probe:u:")]
    assert key.startswith("org_connectors:probe:u:c-crm:")


def test_an_acquire_kicks_only_connectors_without_tools():
    from src.server.services.computer_manager._mcp import McpSecretsMixin
    from src.server.services.mcp_config import (
        Origin,
        ResolvedMCP,
        ResolvedServer,
        State,
        org_server_config,
    )

    resolved = ResolvedMCP(
        entries=(
            ResolvedServer(org_server_config(CRM), Origin.ORG, State.ACTIVE),
            ResolvedServer(org_server_config(WIKI), Origin.ORG, State.DISABLED),
        ),
        version=1,
        org_connectors=(CRM, WIKI),
    )
    scheduled = MagicMock()
    with patch(f"{OD}.schedule_org_discovery", scheduled):
        McpSecretsMixin._kick_org_discovery(
            "u", resolved, SimpleNamespace(mcp_settled_servers=frozenset())
        )
        McpSecretsMixin._kick_org_discovery(
            "u", resolved, SimpleNamespace(mcp_settled_servers=frozenset({"crm"}))
        )
    assert scheduled.call_args_list[0].args == ("u", [CRM])
    assert scheduled.call_args_list[1].args == ("u", [])


# --- on demand, from a workspace -------------------------------------------


def _org_entry(connector):
    from src.server.services.mcp_config import (
        Origin,
        ResolvedMCP,
        ResolvedServer,
        State,
        org_server_config,
    )

    entry = ResolvedServer(org_server_config(connector), Origin.ORG, State.ACTIVE)
    return ResolvedMCP(entries=(entry,), version=1, org_connectors=(connector,)), entry


def _click_patches(route, *, rows=(), signed_in=True, discover=None, held=None):
    from src.server.services.mcp_oauth import org_discovery

    held = set() if held is None else held

    async def acquire_lock(key, token, ttl_ms):
        if key in held:
            return False
        held.add(key)
        return True

    from src.server.database.mcp_oauth import ConnectionStatus

    connection = (
        SimpleNamespace(
            status=ConnectionStatus.CONNECTED, server_url=WIKI.url,
            connection_id="conn-9",
        )
        if signed_in
        else None
    )
    return (
        patch.object(route, "get_user_tool_schemas", AsyncMock(return_value=list(rows))),
        patch.object(route, "schedule_session_mcp_refresh", MagicMock()),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=SimpleNamespace(acquire_lock=acquire_lock),
        ),
        patch.object(org_discovery, "get_connection", AsyncMock(return_value=connection)),
        patch.object(org_discovery, "discover_org_connector", discover or AsyncMock()),
    )


async def _click(route, connector, patches):
    resolved, entry = _org_entry(connector)
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        return await route._discover_org_connector("ws", "u", resolved, entry)


@pytest.mark.asyncio
async def test_clicks_share_one_probe_per_window_across_workers():
    from fastapi import HTTPException

    from src.server.app import mcp_servers as route

    held: set[str] = set()
    discover = AsyncMock(return_value={"server_name": "crm", "status": "error"})
    await _click(route, CRM, _click_patches(route, discover=discover, held=held))
    with pytest.raises(HTTPException) as checking:
        await _click(route, CRM, _click_patches(route, discover=discover, held=held))
    discover.assert_awaited_once()
    assert checking.value.status_code == 409
    assert "already being checked" in checking.value.detail


@pytest.mark.asyncio
async def test_a_member_who_has_not_signed_in_is_told_so_and_spends_no_window():
    from fastapi import HTTPException

    from src.server.app import mcp_servers as route

    held: set[str] = set()
    discover = AsyncMock()
    with pytest.raises(HTTPException) as refused:
        await _click(
            route, WIKI,
            _click_patches(route, signed_in=False, discover=discover, held=held),
        )
    assert refused.value.status_code == 409
    assert "Sign in" in refused.value.detail
    discover.assert_not_awaited()
    assert held == set()


@pytest.mark.asyncio
async def test_a_connector_that_changed_mid_probe_says_so():
    from fastapi import HTTPException

    from src.server.app import mcp_servers as route

    with pytest.raises(HTTPException) as refused:
        await _click(
            route, CRM, _click_patches(route, discover=AsyncMock(return_value=None))
        )
    assert refused.value.status_code == 409
    assert "changed this connector" in refused.value.detail
