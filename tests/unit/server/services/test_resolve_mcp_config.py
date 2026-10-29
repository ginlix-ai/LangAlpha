"""Tests for resolve_mcp_config() merge precedence.

Covers built-in disable, inherited user servers, tombstones, deterministic
ordering, builtin-collision skip, and the zero-rows short-circuit (returns the
SAME built-in objects).

The DB surface (the single snapshot-consistent get_workspace_servers_and_version
helper) is fully mocked.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config.core import MCPConfig, MCPServerConfig
from src.server.database.account_disables import AccountDisables
from src.server.services.mcp_config import (
    Origin,
    ResolvedMCP,
    State,
    resolve_mcp_config,
    user_row_to_server_config,
)
from src.server.services.mcp_discovery import mcp_discovery_fingerprint
from src.server.services.plugins.bundled import ComponentOwners


def _entries(resolved, origin, state):
    """One partition of the resolved entry table, in resolver order."""
    return [e for e in resolved.entries if e.origin is origin and e.state is state]


def _names(resolved, origin, state):
    return [e.name for e in _entries(resolved, origin, state)]


def _base_config(*servers: MCPServerConfig):
    """Wrap server configs in an object exposing ``.mcp.servers``."""
    return SimpleNamespace(mcp=MCPConfig(servers=list(servers)))


def _ws_row(name, *, source, enabled=False, config=None):
    """Build a workspace_mcp_servers row dict as the DB layer returns it."""
    return {"name": name, "source": source, "enabled": enabled, "config": config}


def _user_row(name, **overrides):
    """Build a user_mcp_servers row dict (flat columns) as the DB layer returns it."""
    row = {
        "name": name,
        "transport": "http",
        "command": None,
        "args": [],
        "url": f"https://{name}.example.test/mcp",
        "env": {},
        "headers": {},
        "description": "",
        "instruction": "",
        "tool_exposure_mode": "summary",
        "discovery_uses_secrets": False,
    }
    row.update(overrides)
    return row


async def _resolve(
    base,
    rows,
    version=0,
    user_rows=None,
    connections=None,
    user_disabled=None,
    disabled_bundles=None,
    bundle_owns=None,
    schemas=None,
):
    """Run resolve_mcp_config with all six DB reads mocked.

    ``bundle_owns`` maps a bundle name to the built-in names it ships, which
    is the only thing the resolver asks the bundle reader for. ``schemas`` is
    the user tier's discovery snapshots, which carry the probe verdict that
    decides whether a header row may bind a tool direct.
    """
    owners = ComponentOwners(
        servers={
            name: bundle
            for bundle, names in (bundle_owns or {}).items()
            for name in names
        },
        skills={},
    )
    with (
        patch(
            "src.server.database.mcp_servers.get_workspace_servers_and_version",
            new=AsyncMock(return_value=(rows, version)),
        ),
        patch(
            "src.server.database.mcp_servers.list_enabled_user_servers",
            new=AsyncMock(return_value=list(user_rows or [])),
        ),
        patch(
            "src.server.database.mcp_oauth.list_connections",
            new=AsyncMock(return_value=list(connections or [])),
        ),
        patch(
            "src.server.database.account_disables.list_account_disables",
            new=AsyncMock(
                return_value=AccountDisables(
                    servers=frozenset(user_disabled or ()),
                    bundles=frozenset(disabled_bundles or ()),
                )
            ),
        ),
        patch(
            "src.server.database.mcp_tool_schemas.get_user_tool_schemas",
            new=AsyncMock(return_value=list(schemas or [])),
        ),
        patch(
            "src.server.database.mcp_descriptions.current_descriptions",
            new=AsyncMock(return_value={}),
        ),
        patch(
            "src.server.services.plugins.bundled.component_owners",
            return_value=owners,
        ),
    ):
        return await resolve_mcp_config(base, "user-1", "ws-1")


# ---------------------------------------------------------------------------
# resolve_mcp_config — merge precedence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestResolveMergePrecedence:
    async def test_zero_rows_returns_identical_builtin_objects(self):
        b1 = MCPServerConfig(name="alpha")
        b2 = MCPServerConfig(name="beta")
        base = _base_config(b1, b2)

        resolved = await _resolve(base, rows=[], version=3)

        assert isinstance(resolved, ResolvedMCP)
        # SAME objects, no copies — byte-identical downstream.
        assert resolved.servers[0] is b1
        assert resolved.servers[1] is b2
        assert _names(resolved, Origin.BUILTIN, State.ACTIVE) == ["alpha", "beta"]
        assert _names(resolved, Origin.USER, State.ACTIVE) == []
        assert resolved.version == 3

    async def test_disabled_builtin_is_removed(self):
        base = _base_config(
            MCPServerConfig(name="alpha"), MCPServerConfig(name="beta")
        )
        rows = [_ws_row("beta", source="builtin")]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert _names(resolved, Origin.BUILTIN, State.ACTIVE) == ["alpha"]
        # Exposed so the API can keep a re-enable toggle visible in the UI.
        assert _names(resolved, Origin.BUILTIN, State.DISABLED) == ["beta"]

    async def test_disabled_builtin_names_empty_when_no_rows(self):
        base = _base_config(MCPServerConfig(name="alpha"))

        resolved = await _resolve(base, rows=[])

        assert _names(resolved, Origin.BUILTIN, State.DISABLED) == []

    async def test_disabled_builtins_excluded_from_builtin_names(self):
        base = _base_config(
            MCPServerConfig(name="alpha"), MCPServerConfig(name="beta")
        )
        rows = [_ws_row("beta", source="builtin")]

        resolved = await _resolve(base, rows, user_rows=[_user_row("gamma")])

        assert [s.name for s in resolved.servers] == ["alpha", "gamma"]
        assert _names(resolved, Origin.BUILTIN, State.ACTIVE) == ["alpha"]
        assert _names(resolved, Origin.USER, State.ACTIVE) == ["gamma"]

    async def test_globally_disabled_builtin_not_in_effective_set(self):
        # A built-in disabled in agent_config.yaml itself is never effective.
        base = _base_config(
            MCPServerConfig(name="alpha"),
            MCPServerConfig(name="beta", enabled=False),
        )
        resolved = await _resolve(base, rows=[])
        assert [s.name for s in resolved.servers] == ["alpha"]


# ---------------------------------------------------------------------------
# resolve_mcp_config — inherited user-level layer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestResolveInheritedLayer:
    async def test_inherited_server_follows_the_builtins(self):
        base = _base_config(MCPServerConfig(name="alpha"))

        resolved = await _resolve(base, rows=[], user_rows=[_user_row("acme")])

        assert [s.name for s in resolved.servers] == ["alpha", "acme"]
        assert _names(resolved, Origin.USER, State.ACTIVE) == ["acme"]
        acme = resolved.servers[1]
        assert acme.source == "user"
        assert acme.oauth_connection_id is None

    async def test_inherited_only_still_resolves_without_workspace_rows(self):
        # user servers alone must defeat the zero-row builtin short-circuit.
        b1 = MCPServerConfig(name="alpha")
        base = _base_config(b1)

        resolved = await _resolve(base, rows=[], user_rows=[_user_row("acme")])

        assert [s.name for s in resolved.servers] == ["alpha", "acme"]

    async def test_inherited_sorted_alphabetically(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        user_rows = [_user_row("zulu"), _user_row("bravo"), _user_row("mike")]

        resolved = await _resolve(base, rows=[], user_rows=user_rows)

        assert [s.name for s in resolved.servers] == [
            "alpha", "bravo", "mike", "zulu",
        ]

    async def test_tombstone_removes_inherited_from_this_workspace(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("acme", source="user")]

        resolved = await _resolve(base, rows, user_rows=[_user_row("acme")])

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert _names(resolved, Origin.USER, State.ACTIVE) == []
        # Carried (full config) so the UI keeps a re-enable toggle.
        assert _names(resolved, Origin.USER, State.TOMBSTONED) == ["acme"]

    async def test_a_stray_workspace_row_is_ignored(self):
        """A source='workspace' row left over from the retired local servers
        is neither a server nor a selection: it must not run, and it must not
        hide or replace the user server of the same name (a ``robinhood`` row
        here would otherwise stand in for the broker the user connected)."""
        base = _base_config(MCPServerConfig(name="alpha"))
        stray = {"transport": "stdio", "command": "npx"}
        rows = [
            _ws_row("robinhood", source="workspace", enabled=True, config=stray),
            _ws_row("leftover", source="workspace", enabled=True, config=stray),
            _ws_row("off", source="workspace", enabled=False, config=stray),
        ]

        resolved = await _resolve(base, rows, user_rows=[_user_row("robinhood")])

        assert [s.name for s in resolved.servers] == ["alpha", "robinhood"]
        assert resolved.servers[1].source == "user"
        assert resolved.servers[1].transport == "http"
        assert [e.name for e in resolved.entries] == ["alpha", "robinhood"]

    async def test_user_server_colliding_with_builtin_is_skipped(self):
        b1 = MCPServerConfig(name="alpha")
        base = _base_config(b1)

        resolved = await _resolve(base, rows=[], user_rows=[_user_row("alpha")])

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert resolved.servers[0] is b1
        assert _names(resolved, Origin.USER, State.ACTIVE) == []

    async def test_oauth_connection_id_annotated(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        connections = [
            {
                "connection_id": "conn-1",
                "server_name": "acme",
                "status": "connected",
            },
            {
                "connection_id": "conn-2",
                "server_name": "gone",
                "status": "revoked",
            },
        ]

        resolved = await _resolve(
            base,
            rows=[],
            user_rows=[_user_row("acme"), _user_row("gone")],
            connections=connections,
        )

        by_name = {s.name: s for s in resolved.servers}
        assert by_name["acme"].oauth_connection_id == "conn-1"
        # A revoked connection never binds a server to the relay.
        assert by_name["gone"].oauth_connection_id is None

    async def test_a_revoked_connection_leaves_a_plain_header_row(self):
        # Revoked is history, not a claim on the row: it carries no OAuth label
        # either, so the row is served by its own headers and discovered like
        # any header row. Plugins keeps the revoked status and offers the
        # reconnect.
        base = _base_config(MCPServerConfig(name="alpha"))
        connections = [
            {
                "connection_id": "conn-1",
                "server_name": "acme",
                "server_url": "https://acme.example.test/mcp",
                "status": "revoked",
            }
        ]

        resolved = await _resolve(
            base,
            rows=[],
            user_rows=[_user_row("acme", headers={"X-Api-Key": "k"})],
            connections=connections,
        )

        entry = next(e for e in resolved.entries if e.name == "acme")
        assert entry.oauth_status is None
        assert entry.host_side_oauth is False
        assert entry.config.oauth_connection_id is None

    async def test_url_change_since_consent_forces_reconnect(self):
        # The connection's consented server_url no longer matches the catalog
        # row URL (edited since connect): the token was issued for a different
        # host, so the server must NOT bind — no oauth_connection_id, hence no
        # grant. This is defense-in-depth behind the edit-time revoke.
        base = _base_config(MCPServerConfig(name="alpha"))
        connections = [
            {
                "connection_id": "conn-1",
                "server_name": "acme",
                "server_url": "https://old-host.example.test/mcp",
                "status": "connected",
            }
        ]
        resolved = await _resolve(
            base,
            rows=[],
            user_rows=[_user_row("acme", url="https://new-host.example.test/mcp")],
            connections=connections,
        )
        by_name = {s.name: s for s in resolved.servers}
        assert by_name["acme"].oauth_connection_id is None

    async def test_matching_consented_url_still_binds(self):
        # Same host modulo trailing slash / default port ⇒ still the consented
        # endpoint, so it binds normally (the guard must not over-fire).
        base = _base_config(MCPServerConfig(name="alpha"))
        connections = [
            {
                "connection_id": "conn-1",
                "server_name": "acme",
                "server_url": "https://acme.example.test:443/mcp/",
                "status": "connected",
            }
        ]
        resolved = await _resolve(
            base,
            rows=[],
            user_rows=[_user_row("acme", url="https://acme.example.test/mcp")],
            connections=connections,
        )
        by_name = {s.name: s for s in resolved.servers}
        assert by_name["acme"].oauth_connection_id == "conn-1"

    async def test_the_policy_follows_the_address_not_the_row_name(self):
        """The identity bug this whole surface turns on, at the hiding half.

        A row is named by its owner and can be renamed or repointed at will, so
        deriving the vendor from ``server_name`` gave a row called anything else
        at a broker's host no policy, and a row holding a broker's name pointed
        elsewhere somebody else's. The relay derives from the consented
        ``server_url``; this side has to agree, or a tool is hidden from the
        prompt and still callable, or offered and then refused.
        """
        base = _base_config(MCPServerConfig(name="alpha"))
        resolved = await _resolve(
            base,
            rows=[],
            user_rows=[_user_row("my_broker", url="https://mcp.moomoo.com/mcp")],
            connections=[
                {
                    "connection_id": "conn-1",
                    "server_name": "my_broker",
                    "server_url": "https://mcp.moomoo.com/mcp",
                    "status": "connected",
                    "granted_capabilities": ["market_data"],
                }
            ],
        )
        denied = {e.name: e.denied_tools for e in resolved.entries}["my_broker"]
        assert "trading_order_place" in denied
        assert "quote_stock_quote" not in denied

    async def test_a_brokerage_name_pointed_elsewhere_gets_no_policy(self):
        """The mirror shape: the name is reserved, the address is not ours.

        Refusing moomoo's tool names on somebody else's server would be
        meaningless rather than dangerous -- the danger is the reverse, a
        denial computed for the wrong vendor that happens to omit the tools
        this one actually publishes.
        """
        base = _base_config(MCPServerConfig(name="alpha"))
        resolved = await _resolve(
            base,
            rows=[],
            user_rows=[_user_row("moomoo", url="https://not-moomoo.example.test/mcp")],
            connections=[
                {
                    "connection_id": "conn-1",
                    "server_name": "moomoo",
                    "server_url": "https://not-moomoo.example.test/mcp",
                    "status": "connected",
                    "granted_capabilities": None,
                }
            ],
        )
        denied = {e.name: e.denied_tools for e in resolved.entries}["moomoo"]
        assert denied is None

    async def test_a_brokerage_with_no_recorded_consent_denies_its_curation(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        resolved = await _resolve(
            base,
            rows=[],
            user_rows=[_user_row("moomoo", url="https://mcp.moomoo.com/mcp")],
            connections=[
                {
                    "connection_id": "conn-1",
                    "server_name": "moomoo",
                    "server_url": "https://mcp.moomoo.com/mcp",
                    "status": "connected",
                    "granted_capabilities": None,
                }
            ],
        )
        denied = {e.name: e.denied_tools for e in resolved.entries}["moomoo"]
        assert "trading_order_place" in denied
        assert "quote_stock_quote" in denied

    async def test_user_tier_is_read_through_list_enabled_user_servers(self):
        # The enabled filter lives in the DB layer: every row that read
        # returns for THIS user is inherited as enabled — the resolver never
        # re-checks an ``enabled`` column of its own.
        base = _base_config(MCPServerConfig(name="alpha"))
        reader = AsyncMock(return_value=[_user_row("acme")])
        with (
            patch(
                "src.server.database.mcp_servers.get_workspace_servers_and_version",
                new=AsyncMock(return_value=([], 0)),
            ),
            patch(
                "src.server.database.mcp_servers.list_enabled_user_servers",
                new=reader,
            ),
            patch(
                "src.server.database.mcp_oauth.list_connections",
                new=AsyncMock(return_value=[]),
            ),
            patch(
                "src.server.database.account_disables.list_account_disables",
                new=AsyncMock(return_value=AccountDisables(frozenset(), frozenset())),
            ),
            patch(
                "src.server.database.mcp_tool_schemas.get_user_tool_schemas",
                new=AsyncMock(return_value=[]),
            ),
            patch(
                "src.server.database.mcp_descriptions.current_descriptions",
                new=AsyncMock(return_value={}),
            ),
        ):
            resolved = await resolve_mcp_config(base, "user-1", "ws-1")

        reader.assert_awaited_once_with("user-1")
        assert _names(resolved, Origin.USER, State.ACTIVE) == ["acme"]
        assert resolved.servers[1].enabled is True

    async def test_inert_enabled_user_marker_row_is_skipped(self):
        # An (source='user', enabled=true) row is meaningless: it is not a
        # tombstone. It must neither run nor block the inherited server.
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("acme", source="user", enabled=True)]

        resolved = await _resolve(base, rows, user_rows=[_user_row("acme")])

        assert [s.name for s in resolved.servers] == ["alpha", "acme"]
        assert resolved.servers[1].source == "user"
        assert _names(resolved, Origin.USER, State.ACTIVE) == ["acme"]


@pytest.mark.asyncio
class TestUserBuiltinDisables:
    async def test_user_disable_applies_on_the_short_circuit_path(self):
        # No workspace rows, no user servers — the zero-state fast path must
        # still consult the disable set, or a user whose only state is a
        # disable would never see it applied.
        base = _base_config(
            MCPServerConfig(name="alpha"), MCPServerConfig(name="beta")
        )

        resolved = await _resolve(base, rows=[], user_disabled={"beta"})

        assert [s.name for s in resolved.servers] == ["alpha"]
        disabled = _entries(resolved, Origin.BUILTIN, State.DISABLED)
        assert [e.name for e in disabled] == ["beta"]
        assert disabled[0].disabled_scope == "user"

    async def test_workspace_marker_cannot_reenable_user_disable(self):
        # Both tiers are pure subtractions: an enabled builtin marker row is
        # inert and must not undo the account-wide disable.
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("alpha", source="builtin", enabled=True)]

        resolved = await _resolve(base, rows, user_disabled={"alpha"})

        assert resolved.servers == []
        disabled = _entries(resolved, Origin.BUILTIN, State.DISABLED)
        assert [e.name for e in disabled] == ["alpha"]
        assert disabled[0].disabled_scope == "user"

    async def test_user_scope_wins_when_both_disables_exist(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("alpha", source="builtin")]

        resolved = await _resolve(base, rows, user_disabled={"alpha"})

        disabled = _entries(resolved, Origin.BUILTIN, State.DISABLED)
        assert [e.name for e in disabled] == ["alpha"]
        assert disabled[0].disabled_scope == "user"

    async def test_workspace_disable_scope_is_workspace(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("alpha", source="builtin")]

        resolved = await _resolve(base, rows)

        disabled = _entries(resolved, Origin.BUILTIN, State.DISABLED)
        assert disabled[0].disabled_scope == "workspace"


# ---------------------------------------------------------------------------
# Direct bindings on a header-authenticated row
# ---------------------------------------------------------------------------

TOOL = "list_funds"


def _bound_row(name="fund_desk", **overrides):
    """A catalog row whose stored map asks for one tool on the direct path."""
    return _user_row(name, tool_binding={TOOL: "direct"}, **overrides)


def _schema_row(row, verdict, *, connection_id=None):
    """The user-tier discovery snapshot this row's fingerprint would match."""
    cfg = user_row_to_server_config(row, oauth_connection_id=connection_id)
    return {
        "server_name": row["name"],
        "config_hash": mcp_discovery_fingerprint(cfg),
        "status": "ok",
        "tools": [{"name": TOOL}],
        "last_probe": {} if verdict is None else {"verdict": verdict},
    }


@pytest.mark.asyncio
class TestHeaderRowDirectBinding:
    async def _entry(self, row, *, schemas=None, connections=None):
        resolved = await _resolve(
            _base_config(MCPServerConfig(name="alpha")),
            rows=[],
            user_rows=[row],
            schemas=schemas,
            connections=connections,
        )
        return next(e for e in resolved.entries if e.name == row["name"]), resolved

    async def test_an_unprobed_row_keeps_its_direct_tool_in_the_sandbox(self):
        # Nothing has reached the address, so the header grant the direct path
        # needs cannot be issued: binding the tool direct now would take it out
        # of the sandbox with nothing to replace it.
        row = _bound_row()

        entry, resolved = await self._entry(row)

        assert entry.binding_plan.direct == frozenset()
        assert entry.binding_plan.sandbox_excluded == frozenset()
        assert resolved.binding_plans_by_name == {}
        assert entry.awaiting_probe is True

    async def test_a_missing_snapshot_reads_the_same_as_an_empty_verdict(self):
        row = _bound_row()

        entry, _ = await self._entry(row, schemas=[_schema_row(row, None)])

        assert entry.binding_plan.direct == frozenset()
        assert entry.awaiting_probe is True

    @pytest.mark.parametrize("verdict", ["ok", "ok_authed"])
    async def test_a_clean_verdict_releases_the_stored_override(self, verdict):
        row = _bound_row()

        entry, resolved = await self._entry(
            row, schemas=[_schema_row(row, verdict)]
        )

        assert entry.binding_plan.direct == frozenset({TOOL})
        assert entry.binding_plan.sandbox_excluded == frozenset({TOOL})
        assert entry.awaiting_probe is False

    @pytest.mark.parametrize(
        "verdict",
        ["needs_credential", "credential_rejected", "oauth", "missing_secrets",
         "unreachable"],
    )
    async def test_a_server_that_answered_otherwise_keeps_the_tool_wrapped(
        self, verdict
    ):
        # A verdict is the server's answer, not an absence: the tool stays in
        # the sandbox and nothing is owed another probe on this account.
        row = _bound_row()

        entry, _ = await self._entry(row, schemas=[_schema_row(row, verdict)])

        assert entry.binding_plan.direct == frozenset()
        assert entry.awaiting_probe is False

    async def test_an_oauth_connected_row_binds_direct_without_a_verdict(self):
        # The connection is the credential there, and its own lifecycle says
        # whether the relay can spend it.
        row = _bound_row("broker")
        connections = [
            {
                "connection_id": "conn-1",
                "server_name": "broker",
                "server_url": row["url"],
                "status": "connected",
            }
        ]

        entry, resolved = await self._entry(row, connections=connections)

        assert entry.binding_plan.direct == frozenset({TOOL})
        assert entry.awaiting_probe is False

    async def test_a_stdio_row_is_unchanged_and_asks_for_no_probe(self):
        # It has no address the relay could dial, so it is clamped for a
        # reason a probe could never lift.
        row = _bound_row(transport="stdio", command="npx", url=None)

        entry, resolved = await self._entry(row)

        assert entry.binding_plan.direct == frozenset()
        assert entry.awaiting_probe is False

