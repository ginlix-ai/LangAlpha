"""Verify Session borrow/own semantics for the process-global MCPRegistry.

The borrow path is the load-bearing point of the global-frozen-registry
refactor — every Session that finds an installed global must reuse it
instead of spawning its own MCP cohort, and stop/cleanup must NEVER call
disconnect_all on a borrowed instance.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.mcp_registry import (
    MCPRegistry,
    set_global_registry,
)
from ptc_agent.core.session import Session


def _make_frozen_registry() -> MCPRegistry:
    registry = MCPRegistry(MagicMock(mcp=MagicMock(servers=[])))
    registry._frozen = True
    return registry


def _patch_session_init_collaborators():
    """Patch sandbox + per-instance MCPRegistry so we don't hit Daytona."""
    sandbox = MagicMock()
    sandbox.setup_sandbox_workspace = AsyncMock(return_value=("snapshot", None))
    sandbox.setup_tools_and_mcp = AsyncMock(return_value=None)
    sandbox.cleanup = AsyncMock(return_value=None)
    sandbox.stop_sandbox = AsyncMock(return_value=None)
    sandbox.close = AsyncMock(return_value=None)
    return sandbox


@pytest.mark.asyncio
async def test_initialize_borrows_global_when_installed():
    """When a frozen global exists, Session reuses it and marks itself
    as a non-owner — disconnect_all must never reach the global."""
    global_registry = _make_frozen_registry()
    set_global_registry(global_registry)
    sandbox = _patch_session_init_collaborators()

    session = Session("conv-1", MagicMock())

    with patch("ptc_agent.core.session.PTCSandbox", return_value=sandbox):
        await session.initialize()

    assert session.mcp_registry is global_registry
    assert session._owns_mcp_registry is False


@pytest.mark.asyncio
async def test_initialize_creates_own_registry_when_no_global():
    """No global installed → Session creates a per-instance live registry
    and takes ownership so cleanup tears it down."""
    sandbox = _patch_session_init_collaborators()
    fake_registry = MagicMock(spec=MCPRegistry)
    fake_registry.connect_all = AsyncMock(return_value=None)
    fake_registry.disconnect_all = AsyncMock(return_value=None)

    session = Session("conv-2", MagicMock())

    with patch("ptc_agent.core.session.PTCSandbox", return_value=sandbox), \
         patch("ptc_agent.core.session.MCPRegistry", return_value=fake_registry):
        await session.initialize()

    assert session.mcp_registry is fake_registry
    assert session._owns_mcp_registry is True
    fake_registry.connect_all.assert_awaited_once()


@pytest.mark.asyncio
async def test_cleanup_skips_disconnect_for_borrowed_global():
    """A borrowed global must survive any one Session's cleanup."""
    global_registry = _make_frozen_registry()
    global_registry.disconnect_all = AsyncMock(return_value=None)
    set_global_registry(global_registry)
    sandbox = _patch_session_init_collaborators()

    session = Session("conv-3", MagicMock())

    with patch("ptc_agent.core.session.PTCSandbox", return_value=sandbox):
        await session.initialize()
        await session.cleanup()

    global_registry.disconnect_all.assert_not_called()


@pytest.mark.asyncio
async def test_cleanup_disconnects_owned_registry():
    """Owned per-instance registry must be torn down on cleanup."""
    sandbox = _patch_session_init_collaborators()
    fake_registry = MagicMock(spec=MCPRegistry)
    fake_registry.connect_all = AsyncMock(return_value=None)
    fake_registry.disconnect_all = AsyncMock(return_value=None)

    session = Session("conv-4", MagicMock())

    with patch("ptc_agent.core.session.PTCSandbox", return_value=sandbox), \
         patch("ptc_agent.core.session.MCPRegistry", return_value=fake_registry):
        await session.initialize()
        await session.cleanup()

    fake_registry.disconnect_all.assert_awaited_once()
