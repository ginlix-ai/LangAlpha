"""Session.stop() must restore the pristine MCP server list.

The WorkspaceManager mutates ``session.config.mcp.servers`` to the resolved
composite (built-ins + per-workspace servers). stop() clears the registries +
summary + version but, before this fix, left the mutated server list in place —
so a restart re-entered PTCSandbox with the stale resolution until Phase 2
re-resolved. Session snapshots the pristine list at __init__ and restores it on
stop().
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.session import Session


def _server(name):
    return MCPServerConfig(name=name, transport="stdio", command="x", args=[])


def _make_core_config(server_names=()):
    """A config stub whose only load-bearing field is ``mcp.servers`` (a real
    list of MCPServerConfig) — Session.__init__/stop() only touch that field."""
    config = MagicMock()
    config.mcp = MagicMock()
    config.mcp.servers = [_server(n) for n in server_names]
    return config


def test_init_snapshots_pristine_server_list():
    """__init__ snapshots the server list as a separate copy, not an alias."""
    config = _make_core_config(["builtin-a"])
    session = Session("conv-1", config)

    assert [s.name for s in session._pristine_mcp_servers] == ["builtin-a"]
    # Snapshot is a distinct list — mutating config.mcp.servers won't change it.
    config.mcp.servers.append(_server("extra"))
    assert [s.name for s in session._pristine_mcp_servers] == ["builtin-a"]


@pytest.mark.asyncio
async def test_stop_restores_pristine_server_list():
    """After the manager mutates config.mcp.servers to the resolved composite,
    stop() restores the original built-ins-only list."""
    config = _make_core_config(["builtin-a", "builtin-b"])
    session = Session("conv-2", config)

    # Simulate WorkspaceManager._install_session_composite mutating the list to
    # built-ins + a per-workspace user server.
    user_server = _server("workspace-server")
    session.config.mcp.servers = list(config.mcp.servers) + [user_server]
    assert [s.name for s in session.config.mcp.servers] == [
        "builtin-a",
        "builtin-b",
        "workspace-server",
    ]

    # stop() should not need a live sandbox/registry to restore the list.
    session.sandbox = None
    session._builtin_mcp_registry = None
    session._owns_mcp_registry = False

    await session.stop()

    assert [s.name for s in session.config.mcp.servers] == ["builtin-a", "builtin-b"]
    # And it's a fresh copy, so mutating the restored list doesn't poison the
    # pristine snapshot for a subsequent stop().
    session.config.mcp.servers.append(user_server)
    assert [s.name for s in session._pristine_mcp_servers] == [
        "builtin-a",
        "builtin-b",
    ]


@pytest.mark.asyncio
async def test_stop_restores_even_with_sandbox_present():
    """The restore happens regardless of sandbox/registry teardown."""
    config = _make_core_config(["builtin-a"])
    session = Session("conv-3", config)

    session.config.mcp.servers = list(config.mcp.servers) + [_server("ws-srv")]

    sandbox = MagicMock()
    sandbox.stop_sandbox = AsyncMock(return_value=None)
    sandbox.close = AsyncMock(return_value=None)
    session.sandbox = sandbox
    session._builtin_mcp_registry = None
    session._owns_mcp_registry = False

    await session.stop()

    sandbox.stop_sandbox.assert_awaited_once()
    assert [s.name for s in session.config.mcp.servers] == ["builtin-a"]
