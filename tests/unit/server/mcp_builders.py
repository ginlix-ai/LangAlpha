"""Shared builders for MCP resolution fixtures, server checks and packages.

``resolved_mcp`` assembles a REAL ``ResolvedMCP`` from per-partition lists, so
tests state what they mean ("this server is inherited and tombstoned") instead
of hand-maintaining the derived name sets — and so a change to the resolved
shape breaks the builder once rather than every literal. The check builders do
the same for what ``check_server`` and ``check_servers`` answer.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

from ptc_agent.config.core import MCPServerConfig
from src.server.database.mcp_oauth import ConnectionStatus
from src.server.services.mcp_config import (
    Origin,
    ResolvedMCP,
    ResolvedServer,
    State,
)
from src.server.services.mcp_validation import (
    CheckFailure,
    CheckResult,
    Passed,
    SignInNeeded,
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
    oauth_status: dict[str, str] | None = None,
) -> ResolvedMCP:
    """Build a ResolvedMCP in resolver order (running set first).

    ``local`` and ``disabled_local`` predate the retirement of workspace-local
    servers and now mean what such a server became: a user server switched on
    (``inherited``) or off (``tombstoned``) in this workspace.
    """
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
        *[_user(c, State.ACTIVE) for c in (*inherited, *local)],
        *_plain(disabled_builtins, Origin.BUILTIN, State.DISABLED),
        *[_user(c, State.TOMBSTONED) for c in (*tombstoned, *disabled_local)],
    ]
    return ResolvedMCP(entries=tuple(entries), version=version)


async def passing(user_id: str, server: Any, **_kwargs: Any) -> Passed:
    """``check_server`` for a server that works, as a mock's side effect."""
    return Passed(server.name)


async def all_passing(
    user_id: str, items: Iterable[tuple[Any, Mapping[str, str]]], **_kwargs: Any
) -> list[CheckResult]:
    """``check_servers`` for servers that all work, as a mock's side effect."""
    return [Passed(server.name) for server, _extras in items]


def check_results(
    *,
    passed: Iterable[str] = (),
    failed: Iterable[str] = (),
    sign_in: Iterable[str] = (),
) -> list[CheckResult]:
    """What ``check_servers`` answers, by server name: each passes, fails as a
    rejected key, or asks for a sign-in at ``https://<name>.example.com/mcp``.
    For a caller that reads the answers by name rather than by position."""
    return [
        *(Passed(n) for n in passed),
        *(
            CheckFailure(server=n, verdict="credential_rejected", reason="bad key")
            for n in failed
        ),
        *(SignInNeeded(server=n, url=f"https://{n}.example.com/mcp") for n in sign_in),
    ]


def checked_servers(call: Any) -> dict[str, dict[str, str]]:
    """The servers one ``check_servers`` call checked, by name, each with the
    values its check saw ahead of the vault."""
    return {server.name: dict(extras) for server, extras in call.args[1]}


def zip_package(members: Mapping[str, bytes | str]) -> bytes:
    """A zip archive holding ``members`` (path to content) as written."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


@contextmanager
def checks_pass() -> Iterator[SimpleNamespace]:
    """Every server checked on its way in answers as one that works.

    Each door that writes a user's server checks it first, and the tests that
    use this are about what the door does once it has passed. Yields the
    ``check_server`` seam the add and edit doors share (``catalog``), the
    ``check_servers`` one the import doors pass (``imports``), ``bearer``, the
    connected server's token lookup an edit makes, and ``sign_in``, the
    question every switch asks before it turns a server on (nothing missing).
    """
    sign_in = AsyncMock(return_value=None)
    with (
        patch(
            "src.server.services.mcp_catalog.check_server",
            new=AsyncMock(side_effect=passing),
        ) as catalog,
        patch(
            "src.server.services.mcp_validation.check_server",
            new=AsyncMock(side_effect=passing),
        ) as imports,
        patch(
            "src.server.services.mcp_catalog.consented_bearer",
            new=AsyncMock(return_value=None),
        ) as bearer,
        patch("src.server.services.mcp_catalog.sign_in_missing", new=sign_in),
    ):
        yield SimpleNamespace(
            catalog=catalog, imports=imports, bearer=bearer, sign_in=sign_in
        )
