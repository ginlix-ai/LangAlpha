"""Shared builders for MCP resolution fixtures.

``resolved_mcp`` assembles a REAL ``ResolvedMCP`` from per-partition lists, so
tests state what they mean ("this server is inherited and tombstoned") instead
of hand-maintaining the derived name sets — and so a change to the resolved
shape breaks the builder once rather than every literal.
"""

from __future__ import annotations

from collections.abc import Iterable

from ptc_agent.config.core import MCPServerConfig
from src.server.database.mcp_oauth import ConnectionStatus
from src.server.services.mcp_config import (
    Origin,
    ResolvedMCP,
    ResolvedServer,
    State,
)


def resolved_mcp(
    *,
    version: int = 3,
    builtins: Iterable[MCPServerConfig] = (),
    inherited: Iterable[MCPServerConfig] = (),
    local: Iterable[MCPServerConfig] = (),
    disabled_builtins: Iterable[MCPServerConfig] = (),
    tombstoned: Iterable[MCPServerConfig] = (),
    disabled_local: Iterable[MCPServerConfig] = (),
    shadowed: Iterable[MCPServerConfig] = (),
    oauth_status: dict[str, str] | None = None,
) -> ResolvedMCP:
    """Build a ResolvedMCP in resolver order (running set first)."""
    # ResolvedServer is a plain dataclass — nothing coerces for us, and callers
    # spell statuses as wire strings.
    status = {k: ConnectionStatus(v) for k, v in (oauth_status or {}).items()}

    def _user(config: MCPServerConfig, state: State) -> ResolvedServer:
        return ResolvedServer(
            config=config,
            origin=Origin.USER,
            state=state,
            oauth_status=status.get(config.name),
        )

    def _plain(
        configs: Iterable[MCPServerConfig], origin: Origin, state: State
    ) -> list[ResolvedServer]:
        return [
            ResolvedServer(config=c, origin=origin, state=state) for c in configs
        ]

    entries = [
        *_plain(builtins, Origin.BUILTIN, State.ACTIVE),
        *[_user(c, State.ACTIVE) for c in inherited],
        *_plain(local, Origin.WORKSPACE, State.ACTIVE),
        *_plain(disabled_builtins, Origin.BUILTIN, State.DISABLED),
        *[_user(c, State.TOMBSTONED) for c in tombstoned],
        *_plain(disabled_local, Origin.WORKSPACE, State.DISABLED),
        *[_user(c, State.SHADOWED) for c in shadowed],
    ]
    return ResolvedMCP(entries=tuple(entries), version=version)
