"""Tests for resolve_mcp_config() merge precedence + the row→config converter.

Covers built-in disable, user add, deterministic ordering, builtin-collision
skip, the zero-rows short-circuit (returns the SAME built-in objects), and the
converter round-trip (vault_blueprints stripped, source forced to "workspace").

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
    workspace_row_to_server_config,
)
from src.server.services.plugins.bundled import ComponentOwners


def _entries(resolved, origin, state):
    """One partition of the resolved entry table, in resolver order."""
    return [e for e in resolved.entries if e.origin is origin and e.state is state]


def _names(resolved, origin, state):
    return [e.name for e in _entries(resolved, origin, state)]


def _base_config(*servers: MCPServerConfig):
    """Wrap server configs in an object exposing ``.mcp.servers``."""
    return SimpleNamespace(mcp=MCPConfig(servers=list(servers)))


def _ws_row(name, source="workspace", enabled=True, config=None):
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
):
    """Run resolve_mcp_config with all five DB reads mocked.

    ``bundle_owns`` maps a bundle name to the built-in names it ships, which
    is the only thing the resolver asks the bundle reader for.
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
            "src.server.services.plugins.bundled.component_owners",
            return_value=owners,
        ),
    ):
        return await resolve_mcp_config(base, "user-1", "ws-1")


# ---------------------------------------------------------------------------
# Converter
# ---------------------------------------------------------------------------


class TestConverter:
    def test_forces_source_workspace_and_strips_blueprints(self):
        row = _ws_row(
            "acme",
            config={
                "transport": "http",
                "url": "https://example.test/mcp",
                "source": "builtin",  # hostile / stale — must be ignored
                "vault_blueprints": [{"name": "X"}],  # built-in-only — stripped
            },
        )
        cfg = workspace_row_to_server_config(row)
        assert cfg.source == "workspace"
        assert cfg.name == "acme"
        assert cfg.vault_blueprints == []

    def test_round_trip_preserves_fields(self):
        row = _ws_row(
            "acme",
            config={
                "transport": "stdio",
                "command": "npx",
                "args": ["-y", "acme-mcp"],
                "env": {"KEY": "${vault:ACME_KEY}"},
                "description": "A server",
                "instruction": "Use for X",
                "tool_exposure_mode": "detailed",
                "discovery_uses_secrets": True,
            },
        )
        cfg = workspace_row_to_server_config(row)
        assert cfg.command == "npx"
        assert cfg.args == ["-y", "acme-mcp"]
        assert cfg.env == {"KEY": "${vault:ACME_KEY}"}
        assert cfg.tool_exposure_mode == "detailed"
        assert cfg.discovery_uses_secrets is True

    def test_round_trip_defaults_discovery_uses_secrets_off(self):
        # A row whose config omits the flag (legacy rows) defaults to secret-less.
        row = _ws_row("acme", config={"transport": "stdio", "command": "npx"})
        assert workspace_row_to_server_config(row).discovery_uses_secrets is False

    def test_row_name_overrides_config_name(self):
        row = _ws_row("authoritative", config={"name": "stale", "transport": "stdio"})
        assert workspace_row_to_server_config(row).name == "authoritative"

    def test_strips_resolve_time_binding_fields(self):
        # oauth_connection_id is a resolution OUTPUT: a stored blob must never
        # be able to bind itself to a connection. egress_grant_id is no longer
        # a config field at all — a stale key must still parse, not 500.
        row = _ws_row(
            "acme",
            config={
                "transport": "http",
                "url": "https://example.test/mcp",
                "oauth_connection_id": "conn-someone-else",
                "egress_grant_id": "grant-someone-else",
            },
        )
        cfg = workspace_row_to_server_config(row)
        assert cfg.oauth_connection_id is None
        assert not hasattr(cfg, "egress_grant_id")


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
        assert _names(resolved, Origin.WORKSPACE, State.ACTIVE) == []
        assert resolved.version == 3

    async def test_disabled_builtin_is_removed(self):
        base = _base_config(
            MCPServerConfig(name="alpha"), MCPServerConfig(name="beta")
        )
        rows = [_ws_row("beta", source="builtin", enabled=False, config=None)]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert _names(resolved, Origin.BUILTIN, State.ACTIVE) == ["alpha"]
        # Exposed so the API can keep a re-enable toggle visible in the UI.
        assert _names(resolved, Origin.BUILTIN, State.DISABLED) == ["beta"]

    async def test_disabled_builtin_names_empty_when_no_rows(self):
        base = _base_config(MCPServerConfig(name="alpha"))

        resolved = await _resolve(base, rows=[])

        assert _names(resolved, Origin.BUILTIN, State.DISABLED) == []

    async def test_user_server_appended_after_builtins(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("zeta", config={"transport": "stdio", "command": "npx"})]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha", "zeta"]
        assert resolved.servers[1].source == "workspace"
        assert _names(resolved, Origin.WORKSPACE, State.ACTIVE) == ["zeta"]

    async def test_user_servers_sorted_alphabetically(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [
            _ws_row("yankee", config={"transport": "stdio"}),
            _ws_row("xray", config={"transport": "stdio"}),
            _ws_row("zulu", config={"transport": "stdio"}),
        ]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha", "xray", "yankee", "zulu"]

    async def test_builtin_order_preserved(self):
        base = _base_config(
            MCPServerConfig(name="gamma"),
            MCPServerConfig(name="alpha"),
            MCPServerConfig(name="beta"),
        )
        resolved = await _resolve(base, rows=[])
        assert [s.name for s in resolved.servers] == ["gamma", "alpha", "beta"]

    async def test_disabled_user_row_is_skipped(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("zeta", enabled=False, config={"transport": "stdio"})]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert _names(resolved, Origin.WORKSPACE, State.ACTIVE) == []

    async def test_disabled_user_row_carried_for_reenable(self):
        # Disabled workspace servers stay out of the effective set but are
        # carried so the API can keep a re-enable toggle in the UI.
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [
            _ws_row("zeta", enabled=False, config={"transport": "stdio", "command": "npx"}),
            _ws_row("yankee", config={"transport": "stdio", "command": "npx"}),
        ]

        resolved = await _resolve(base, rows)

        # 'zeta' runs nowhere, but is available to re-enable.
        assert [s.name for s in resolved.servers] == ["alpha", "yankee"]
        assert _names(resolved, Origin.WORKSPACE, State.ACTIVE) == ["yankee"]
        disabled = _entries(resolved, Origin.WORKSPACE, State.DISABLED)
        assert [e.name for e in disabled] == ["zeta"]
        assert disabled[0].config.source == "workspace"

    async def test_disabled_workspace_servers_sorted_and_empty_when_none(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        # No disabled rows ⇒ empty list (default_factory, not a shared default).
        empty = await _resolve(base, rows=[])
        assert _names(empty, Origin.WORKSPACE, State.DISABLED) == []
        rows = [
            _ws_row("zulu", enabled=False, config={"transport": "stdio"}),
            _ws_row("yankee", enabled=False, config={"transport": "stdio"}),
        ]
        resolved = await _resolve(base, rows)
        assert _names(resolved, Origin.WORKSPACE, State.DISABLED) == ["yankee", "zulu"]
        assert [s.name for s in resolved.servers] == ["alpha"]  # only the built-in runs

    async def test_workspace_server_colliding_with_builtin_is_skipped(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        # A workspace row whose name collides with a built-in: runtime backstop
        # for the API's 409. It must be skipped, not shadow the built-in.
        rows = [_ws_row("alpha", config={"transport": "stdio", "command": "npx"})]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert resolved.servers[0].source == "builtin"
        assert _names(resolved, Origin.WORKSPACE, State.ACTIVE) == []

    async def test_workspace_row_cannot_shadow_a_shipped_brokerage(self):
        """The backstop the write-time reservation can never reach.

        Refusing the name at every door only binds writes from now on. A row
        created before that -- or by a build that predates it -- still resolves,
        and a workspace row shadows the inherited catalog row of the same name
        whether it is enabled or not. So the agent's trade-shaped ``robinhood``
        tools would come from wherever the local row points, with its description
        reaching the prompt, while the Plugins page still reports the real
        connector connected.
        """
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("robinhood", config={"transport": "stdio", "command": "npx"})]

        resolved = await _resolve(base, rows, user_rows=[_user_row("robinhood")])

        assert _names(resolved, Origin.WORKSPACE, State.ACTIVE) == []
        # ...and the real one is untouched: at the user tier this name IS the
        # brokerage, so the same rule applied there would unplug the connector
        # the reservation exists to protect.
        assert _names(resolved, Origin.USER, State.ACTIVE) == ["robinhood"]

    async def test_disabled_builtins_excluded_from_builtin_names(self):
        base = _base_config(
            MCPServerConfig(name="alpha"), MCPServerConfig(name="beta")
        )
        rows = [
            _ws_row("beta", source="builtin", enabled=False),
            _ws_row("gamma", config={"transport": "stdio"}),
        ]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha", "gamma"]
        assert _names(resolved, Origin.BUILTIN, State.ACTIVE) == ["alpha"]
        assert _names(resolved, Origin.WORKSPACE, State.ACTIVE) == ["gamma"]

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
    async def test_inherited_server_between_builtins_and_local(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("local", config={"transport": "stdio", "command": "npx"})]
        user_rows = [_user_row("acme")]

        resolved = await _resolve(base, rows, user_rows=user_rows)

        assert [s.name for s in resolved.servers] == ["alpha", "acme", "local"]
        assert _names(resolved, Origin.USER, State.ACTIVE) == ["acme"]
        assert _names(resolved, Origin.WORKSPACE, State.ACTIVE) == ["local"]
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
        rows = [_ws_row("acme", source="user", enabled=False, config=None)]

        resolved = await _resolve(base, rows, user_rows=[_user_row("acme")])

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert _names(resolved, Origin.USER, State.ACTIVE) == []
        # Carried (full config) so the UI keeps a re-enable toggle.
        assert _names(resolved, Origin.USER, State.TOMBSTONED) == ["acme"]

    async def test_workspace_local_shadows_inherited(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("acme", config={"transport": "stdio", "command": "npx"})]

        resolved = await _resolve(base, rows, user_rows=[_user_row("acme")])

        names = [s.name for s in resolved.servers]
        assert names == ["alpha", "acme"]
        # The one effective 'acme' is the LOCAL fork, not the inherited config.
        assert resolved.servers[1].source == "workspace"
        assert _names(resolved, Origin.USER, State.SHADOWED) == ["acme"]
        assert _names(resolved, Origin.USER, State.ACTIVE) == []

    async def test_disabled_local_fork_still_shadows_inherited(self):
        # A disabled local fork must not fall back to running the inherited
        # config the user explicitly forked away from.
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [
            _ws_row(
                "acme", enabled=False,
                config={"transport": "stdio", "command": "npx"},
            )
        ]

        resolved = await _resolve(base, rows, user_rows=[_user_row("acme")])

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert _names(resolved, Origin.USER, State.SHADOWED) == ["acme"]
        assert _names(resolved, Origin.WORKSPACE, State.DISABLED) == ["acme"]

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
        ):
            resolved = await resolve_mcp_config(base, "user-1", "ws-1")

        reader.assert_awaited_once_with("user-1")
        assert _names(resolved, Origin.USER, State.ACTIVE) == ["acme"]
        assert resolved.servers[1].enabled is True

    async def test_inert_enabled_user_marker_row_is_skipped(self):
        # An (source='user', enabled=true) row is meaningless — not a tombstone,
        # not a local config. It must neither run nor block the inherited server.
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("acme", source="user", enabled=True, config=None)]

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
        rows = [_ws_row("alpha", source="builtin", enabled=True, config=None)]

        resolved = await _resolve(base, rows, user_disabled={"alpha"})

        assert resolved.servers == []
        disabled = _entries(resolved, Origin.BUILTIN, State.DISABLED)
        assert [e.name for e in disabled] == ["alpha"]
        assert disabled[0].disabled_scope == "user"

    async def test_user_scope_wins_when_both_disables_exist(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("alpha", source="builtin", enabled=False, config=None)]

        resolved = await _resolve(base, rows, user_disabled={"alpha"})

        disabled = _entries(resolved, Origin.BUILTIN, State.DISABLED)
        assert [e.name for e in disabled] == ["alpha"]
        assert disabled[0].disabled_scope == "user"

    async def test_workspace_disable_scope_is_workspace(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("alpha", source="builtin", enabled=False, config=None)]

        resolved = await _resolve(base, rows)

        disabled = _entries(resolved, Origin.BUILTIN, State.DISABLED)
        assert disabled[0].disabled_scope == "workspace"
