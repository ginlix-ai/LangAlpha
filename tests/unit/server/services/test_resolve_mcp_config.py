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
from src.server.services.mcp_config import (
    ResolvedMCP,
    resolve_mcp_config,
    workspace_row_to_server_config,
)


def _base_config(*servers: MCPServerConfig):
    """Wrap server configs in an object exposing ``.mcp.servers``."""
    return SimpleNamespace(mcp=MCPConfig(servers=list(servers)))


def _ws_row(name, source="workspace", enabled=True, config=None):
    """Build a workspace_mcp_servers row dict as the DB layer returns it."""
    return {"name": name, "source": source, "enabled": enabled, "config": config}


def _patch_db_target(rows, version):
    """Patch the resolver's single snapshot-consistent DB read."""
    return patch(
        "src.server.database.mcp_servers.get_workspace_servers_and_version",
        new=AsyncMock(return_value=(rows, version)),
    )


async def _resolve(base, rows, version=0):
    with _patch_db_target(rows, version):
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
        assert resolved.builtin_names == frozenset({"alpha", "beta"})
        assert resolved.user_names == frozenset()
        assert resolved.version == 3

    async def test_disabled_builtin_is_removed(self):
        base = _base_config(
            MCPServerConfig(name="alpha"), MCPServerConfig(name="beta")
        )
        rows = [_ws_row("beta", source="builtin", enabled=False, config=None)]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert resolved.builtin_names == frozenset({"alpha"})
        # Exposed so the API can keep a re-enable toggle visible in the UI.
        assert resolved.disabled_builtin_names == frozenset({"beta"})

    async def test_disabled_builtin_names_empty_when_no_rows(self):
        base = _base_config(MCPServerConfig(name="alpha"))

        resolved = await _resolve(base, rows=[])

        assert resolved.disabled_builtin_names == frozenset()

    async def test_user_server_appended_after_builtins(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        rows = [_ws_row("zeta", config={"transport": "stdio", "command": "npx"})]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha", "zeta"]
        assert resolved.servers[1].source == "workspace"
        assert resolved.user_names == frozenset({"zeta"})

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
        assert resolved.user_names == frozenset()

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
        assert resolved.user_names == frozenset({"yankee"})
        disabled = [s.name for s in resolved.disabled_workspace_servers]
        assert disabled == ["zeta"]
        assert resolved.disabled_workspace_servers[0].source == "workspace"

    async def test_disabled_workspace_servers_sorted_and_empty_when_none(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        # No disabled rows ⇒ empty list (default_factory, not a shared default).
        assert (await _resolve(base, rows=[])).disabled_workspace_servers == []
        rows = [
            _ws_row("zulu", enabled=False, config={"transport": "stdio"}),
            _ws_row("yankee", enabled=False, config={"transport": "stdio"}),
        ]
        resolved = await _resolve(base, rows)
        assert [s.name for s in resolved.disabled_workspace_servers] == ["yankee", "zulu"]
        assert [s.name for s in resolved.servers] == ["alpha"]  # only the built-in runs

    async def test_workspace_server_colliding_with_builtin_is_skipped(self):
        base = _base_config(MCPServerConfig(name="alpha"))
        # A workspace row whose name collides with a built-in: runtime backstop
        # for the API's 409. It must be skipped, not shadow the built-in.
        rows = [_ws_row("alpha", config={"transport": "stdio", "command": "npx"})]

        resolved = await _resolve(base, rows)

        assert [s.name for s in resolved.servers] == ["alpha"]
        assert resolved.servers[0].source == "builtin"
        assert resolved.user_names == frozenset()

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
        assert resolved.builtin_names == frozenset({"alpha"})
        assert resolved.user_names == frozenset({"gamma"})

    async def test_globally_disabled_builtin_not_in_effective_set(self):
        # A built-in disabled in agent_config.yaml itself is never effective.
        base = _base_config(
            MCPServerConfig(name="alpha"),
            MCPServerConfig(name="beta", enabled=False),
        )
        resolved = await _resolve(base, rows=[])
        assert [s.name for s in resolved.servers] == ["alpha"]
