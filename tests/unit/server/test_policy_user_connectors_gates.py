"""The ``user_connectors`` rule where a server is put in use and where it is
called: adding, importing, turning on and connecting one of the user's own
servers is refused (403, 503 while the policy is unknown), a plugin's withheld
servers are skipped while the rest of it lands, and every call to one is
refused at the relay and at the direct path. Turning one off is never asked,
and a brokerage is the ``trading`` rule's."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from tests.unit.server.policy_standing import standing_under
from src.config.models import PolicyLayer
from src.server.models.mcp_server import McpServerInput, ParsedMcpServer
from src.server.services.policy import ALLOW_ALL, USER_CONNECTORS, merge

pytestmark = pytest.mark.asyncio

POLICY = "src.server.services.policy"
NO_OWN = merge(None, PolicyLayer.model_validate({"user_connectors": False}))
OWN_URL = "https://acme.example.test/mcp"
BROKER_URL = "https://mcp.moomoo.com/mcp"
REFUSAL = (
    "No call was made: your organization does not allow the user's own "
    "connectors, so this one is withheld. Tell the user."
)



def _refused(excinfo, status=403):
    assert excinfo.value.status_code == status
    if status == 403:
        assert excinfo.value.detail["rule"] == USER_CONNECTORS
        assert excinfo.value.detail["set_by"] == "organization"


# --- adding one --------------------------------------------------------------


@pytest.mark.parametrize(("policy", "status"), [(NO_OWN, 403), (None, 503)])
@pytest.mark.parametrize(
    "server",
    [
        McpServerInput(name="acme", transport="http", url=OWN_URL),
        McpServerInput(name="local", transport="stdio", command="npx"),
    ],
    ids=["http", "stdio"],
)
async def test_adding_an_own_server_is_refused_before_anything_is_checked(
    policy, status, server
):
    from src.server.services.mcp_saves import CatalogAdd

    placed = AsyncMock()
    with (
        standing_under(policy),
        patch("src.server.services.mcp_saves.refuse_unplaceable", placed),
        pytest.raises(HTTPException) as refused,
    ):
        await CatalogAdd("u", server).refuse()
    _refused(refused, status)
    placed.assert_not_awaited()


async def test_adding_one_at_a_brokerage_or_while_allowed_goes_on():
    from src.server.services.mcp_saves import CatalogAdd

    placed = AsyncMock()
    with patch("src.server.services.mcp_saves.refuse_unplaceable", placed):
        with standing_under(NO_OWN) as asked:
            await CatalogAdd(
                "u", McpServerInput(name="mine", transport="http", url=BROKER_URL)
            ).refuse()
        asked.assert_not_awaited()
        with standing_under(ALLOW_ALL):
            await CatalogAdd(
                "u", McpServerInput(name="acme", transport="http", url=OWN_URL)
            ).refuse()
    assert placed.await_count == 2


# --- importing ---------------------------------------------------------------


@pytest.mark.parametrize(("policy", "status"), [(NO_OWN, 403), (None, 503)])
async def test_an_import_is_refused_whole(policy, status):
    from src.server.services.mcp_import import refuse_user_connector_import

    parsed = [
        ParsedMcpServer("a", "a", False, {"url": BROKER_URL}),
        ParsedMcpServer("b", "b", False, {"command": "npx"}),
    ]
    with standing_under(policy), pytest.raises(HTTPException) as refused:
        await refuse_user_connector_import("u", parsed)
    _refused(refused, status)


async def test_an_import_of_brokerages_alone_reads_nothing():
    from src.server.services.mcp_import import refuse_user_connector_import

    with standing_under(NO_OWN) as asked:
        await refuse_user_connector_import(
            "u", [ParsedMcpServer("a", "a", False, {"url": BROKER_URL})]
        )
    asked.assert_not_awaited()


@pytest.mark.parametrize("route", ["catalog", "workspace"])
async def test_both_import_routes_refuse(route):
    if route == "catalog":
        from src.server.app.mcp_catalog import import_servers

        call = import_servers("u", body={"mcpServers": {"acme": {"url": OWN_URL}}})
    else:
        from src.server.app.mcp_servers import import_servers

        call = import_servers(
            "ws", "u", body={"mcpServers": {"acme": {"url": OWN_URL}}}
        )
    with (
        standing_under(NO_OWN),
        patch(
            "src.server.app.mcp_servers._require_owned_workspace", AsyncMock()
        ),
        patch(
            "src.server.app.mcp_catalog.run_mcp_import", AsyncMock()
        ) as ran_catalog,
        patch(
            "src.server.app.mcp_servers.run_mcp_import", AsyncMock()
        ) as ran_workspace,
        pytest.raises(HTTPException) as refused,
    ):
        await call
    _refused(refused)
    ran_catalog.assert_not_awaited()
    ran_workspace.assert_not_awaited()


# --- turning one on, and off ------------------------------------------------


def _catalog_row(url=OWN_URL, **fields):
    return {"name": "acme", "transport": "http", "url": url, "enabled": False, **fields}


@pytest.mark.parametrize(("policy", "status"), [(NO_OWN, 403), (None, 503)])
async def test_turning_one_on_is_refused_and_off_is_not(policy, status):
    from src.server.services import mcp_catalog

    switch = AsyncMock(return_value={"name": "acme"})
    with (
        standing_under(policy),
        patch.object(mcp_catalog, "get_catalog_server", AsyncMock(return_value=_catalog_row())),
        patch.object(mcp_catalog, "set_catalog_server_enabled", switch),
        patch(
            "src.server.services.mcp_oauth.lifecycle.revoke_live_grants", AsyncMock()
        ),
    ):
        with pytest.raises(HTTPException) as refused:
            await mcp_catalog.apply_catalog_enabled("u", "acme", True)
        switch.assert_not_awaited()
        await mcp_catalog.apply_catalog_enabled("u", "acme", False)
    _refused(refused, status)
    switch.assert_awaited_once_with("u", "acme", False)


async def test_turning_on_a_brokerage_is_the_trading_rules():
    from src.server.services import mcp_catalog

    with (
        standing_under(NO_OWN) as asked,
        patch.object(
            mcp_catalog, "get_catalog_server",
            AsyncMock(return_value=_catalog_row(BROKER_URL)),
        ),
        patch.object(mcp_catalog, "refuse_missing_sign_in", AsyncMock()),
        patch.object(mcp_catalog, "set_catalog_server_enabled", AsyncMock(return_value=None)),
    ):
        assert await mcp_catalog.apply_catalog_enabled("u", "moomoo", True) == (None, None)
    asked.assert_not_awaited()


@pytest.mark.parametrize("state", ["tombstoned", "active"])
async def test_turning_one_on_in_a_workspace_is_refused(state):
    from src.server.app import mcp_servers as route
    from src.server.models.mcp_server import EnabledInput
    from src.server.services.mcp_config import Origin, ServerRef, State

    ref = (
        ServerRef("acme", Origin.USER, State.TOMBSTONED, {"workspace_mcp_server_id": "w1"})
        if state == "tombstoned"
        else ServerRef("acme", Origin.USER, State.ACTIVE, _catalog_row(enabled=True))
    )
    untombstone = AsyncMock()
    with (
        standing_under(NO_OWN),
        patch.object(route, "_require_owned_workspace", AsyncMock(return_value={})),
        patch.object(route, "builtin_names", return_value=set()),
        patch.object(route, "classify_server_name", AsyncMock(return_value=ref)),
        patch.object(
            route, "get_catalog_server", AsyncMock(return_value=_catalog_row(enabled=True))
        ),
        patch.object(route, "untombstone_user_server", untombstone),
        pytest.raises(HTTPException) as refused,
    ):
        await route.set_enabled("ws", "acme", EnabledInput(enabled=True), "u")
    _refused(refused)
    untombstone.assert_not_awaited()


# --- connecting one ----------------------------------------------------------


@pytest.mark.parametrize(("policy", "status"), [(NO_OWN, 403), (None, 503)])
async def test_connecting_one_is_refused_before_anything_is_dialled(policy, status):
    from src.server.services.mcp_oauth import connect

    started = AsyncMock()
    with (
        standing_under(policy),
        patch.object(connect, "get_catalog_server", AsyncMock(return_value=_catalog_row())),
        patch.object(connect, "start_sign_in", started),
        pytest.raises(HTTPException) as refused,
    ):
        await connect.start_connect("u", "acme")
    _refused(refused, status)
    started.assert_not_awaited()


async def test_connecting_a_brokerage_goes_on():
    from src.server.services.mcp_oauth import connect

    started = AsyncMock()
    with (
        standing_under(NO_OWN),
        patch.object(
            connect, "get_catalog_server", AsyncMock(return_value=_catalog_row(BROKER_URL))
        ),
        patch.object(connect, "start_sign_in", started),
    ):
        await connect.start_connect("u", "moomoo")
    started.assert_awaited_once()


# --- a plugin ----------------------------------------------------------------


def _plans():
    from src.server.services.plugins.mcp import _plan_entry

    return [
        _plan_entry("acme", {"type": "streamable-http", "url": OWN_URL}),
        _plan_entry("local", {"type": "stdio", "command": "npx"}),
        _plan_entry("my_broker", {"type": "streamable-http", "url": BROKER_URL}),
    ]


@pytest.mark.parametrize("policy", [NO_OWN, None], ids=["denied", "unknown"])
async def test_a_plugins_own_servers_are_skipped_with_the_reason(policy):
    from src.server.services.plugins.mcp import withhold_own_connectors

    plans = _plans()
    with standing_under(policy):
        await withhold_own_connectors("u", plans)
    assert [p.installable for p in plans] == [False, False, True]
    reasons = {p.skip_reason for p in plans[:2]}
    if policy is NO_OWN:
        assert reasons == {"your organization does not allow your own connectors"}
    else:
        assert reasons and all("could not be read" in r for r in reasons)


async def test_a_plugin_with_no_own_server_reads_nothing():
    from src.server.services.plugins.mcp import withhold_own_connectors

    plans = _plans()[2:]
    with standing_under(NO_OWN) as asked:
        await withhold_own_connectors("u", plans)
    asked.assert_not_awaited()
    with standing_under(ALLOW_ALL):
        plans = _plans()
        await withhold_own_connectors("u", plans)
    assert all(p.installable for p in plans)


async def test_the_fan_out_reports_withheld_servers_and_leaves_the_install_reconcilable(
    monkeypatch,
):
    from src.server.models.plugin import InstallReport
    from src.server.services import mcp_import
    from src.server.services.plugins import server_fanout
    from src.server.services.plugins.checks import CheckedServers

    @asynccontextmanager
    async def connection():
        class _Conn:
            @asynccontextmanager
            async def transaction(self):
                yield

        yield _Conn()

    monkeypatch.setattr("src.server.services.mcp_import.get_db_connection", connection)
    monkeypatch.setattr(server_fanout, "list_catalog_servers", AsyncMock(return_value=[]))
    monkeypatch.setattr(mcp_import, "get_user_secret_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(server_fanout, "schedule_catalog_discovery", lambda *a, **k: None)
    created = AsyncMock(return_value={"enabled": True})
    monkeypatch.setattr(server_fanout, "create_catalog_server", created)

    plans = _plans()
    checked = CheckedServers.of(
        [McpServerInput(name="my_broker", transport="http", url=BROKER_URL)]
    )
    report = InstallReport()
    with standing_under(NO_OWN):
        await server_fanout.fan_out_servers("u", "plug", plans, report, checked=checked)
    by_key = {c.key: c for c in report.components}
    assert (by_key["acme"].status, by_key["acme"].withheld) == ("skipped", True)
    assert (by_key["local"].status, by_key["local"].withheld) == ("skipped", True)
    assert by_key["my_broker"].status == "created", by_key["my_broker"].reason
    assert created.await_count == 1
    # Lifting the rule must still find the package worth reconciling.
    assert not report.landed_whole


# --- every call --------------------------------------------------------------


def _own_grant(kind="header_mcp", url=OWN_URL):
    from tests.unit.server.services.test_egress_order_gate import _grant

    return _grant(
        kind=kind, destination_url=url,
        connection_id=None if kind == "header_mcp" else "conn-1",
        server_name="acme" if kind == "header_mcp" else None,
    )


async def relay_frame(grant, resolve=None):
    """``prepare_relay`` for one ``read`` frame on ``grant``, with nothing
    dialled or resolved."""
    from src.server.services.egress import relay
    from tests.unit.server.services.test_egress_order_gate import _body

    with (
        patch.object(relay, "fetch_grant_for_relay", AsyncMock(return_value=grant)),
        patch.object(relay, "_grant_is_reachable", return_value=True),
        patch.object(relay, "resolve_vendor_credential", resolve or AsyncMock()),
        patch.object(relay, "_pin_destination", AsyncMock()),
    ):
        return await relay.prepare_relay(
            "g", claims=_Claims(grant["user_id"]), raw_body=_body("read", {})
        )


@pytest.mark.parametrize("kind", ["header_mcp", "oauth_mcp"])
@pytest.mark.parametrize("policy", [NO_OWN, None], ids=["denied", "unknown"])
async def test_every_frame_to_an_own_server_is_refused_at_the_relay(kind, policy):
    from src.server.services.egress import RelayError, RelayRejection

    with standing_under(policy), pytest.raises(RelayRejection) as refused:
        await relay_frame(_own_grant(kind))
    assert (refused.value.status, refused.value.code) == (403, RelayError.POLICY_WITHHELD)


async def test_the_relay_refuses_before_any_credential_is_resolved():
    from src.server.services.egress import RelayRejection

    resolve = AsyncMock()
    with standing_under(NO_OWN), pytest.raises(RelayRejection):
        await relay_frame(_own_grant(), resolve)
    resolve.assert_not_awaited()


class _Claims:
    def __init__(self, user_id):
        self.user_id = user_id
        self.caller = None


async def test_a_brokerage_frame_or_an_allowed_one_is_not_refused():
    from src.server.services.egress.policy_gate import PolicyGate

    gate = PolicyGate("u1", via="relay", caller=None)
    with standing_under(NO_OWN) as asked:
        assert await gate.own_server(BROKER_URL, server="b", tool="read") is None
    asked.assert_not_awaited()
    with standing_under(ALLOW_ALL):
        await relay_frame(_own_grant())


async def test_a_direct_call_to_an_own_server_is_refused_with_the_reason(monkeypatch):
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from tests.unit.server.services.test_direct_tool_approval_gate import _header

    _header(monkeypatch, {})
    binding = DirectMCPBinding(user_id="u1")
    with standing_under(NO_OWN):
        reason = await binding.check("acme", "read")
    assert reason == REFUSAL
    with standing_under(None):
        unknown = await binding.check("acme", "read")
    assert unknown.startswith("No call was made:") and "cannot be read" in unknown
    assert "again" not in (reason + unknown).lower()
    with standing_under(ALLOW_ALL):
        assert await binding.check("acme", "read") != REFUSAL


async def test_a_direct_call_to_a_brokerage_is_not_asked(monkeypatch):
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from tests.unit.server.services.test_direct_tool_approval_gate import (
        PAPER_ORDER,
        _connected,
    )

    _connected(monkeypatch, [])
    with standing_under(None):
        assert await DirectMCPBinding(user_id="u1").check("moomoo", PAPER_ORDER, True) is None
