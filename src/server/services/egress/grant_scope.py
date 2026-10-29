"""Which servers in a resolved workspace earn an egress grant, and of which kind.

One place answers it, because the answer is read by two paths that would
otherwise drift: the sandbox session's bind, and the config mutations that
retire a grant before replying. A server that earns a
grant on one path and not on another is either a tool the model cannot call or
an authorization overhang, depending on which way the two disagree.

The kind is decided by what authenticates the server, never by what it is
named. An OAuth connection means the relay spends its token; otherwise a
remote row authenticates with its own headers, and the grant names the row so
the relay can resolve them per request.
"""

from __future__ import annotations

from typing import Any

from src.server.database.egress_grants import (
    GRANT_KIND_HEADER_MCP,
    GRANT_KIND_OAUTH_MCP,
    GrantRef,
)
from src.server.database.mcp_tool_schemas import get_user_tool_schemas
from src.server.models.mcp_server import probe_ok
from src.server.services.mcp_config import Origin, State
from src.server.services.mcp_discovery import ToolSnapshotIndex


async def user_snapshots(user_id: str) -> ToolSnapshotIndex:
    """The user tier's discovery snapshots, hash-gated to each row's current config."""
    return ToolSnapshotIndex(user_rows=await get_user_tool_schemas(user_id))


def _header_candidate(server: Any) -> bool:
    """Whether a row with no connection could authenticate itself to the relay.

    ``http`` only, not every remote transport: the relay dials streamable HTTP,
    so a legacy ``sse`` row has nothing a grant could authorize.
    """
    return bool(server.transport == "http" and server.url)


def _probe_ok(server: Any, snapshots: ToolSnapshotIndex) -> bool:
    """Whether this row's own headers were last seen working.

    A grant for anything else would authorize a call that can only fail.
    Crossing this predicate bumps the workspace config version, which is what
    makes a warm sandbox session resolve again, so a key that stops working
    costs the row its grant without waiting for the next edit.
    """
    return probe_ok(snapshots.snapshot(server))


def _kick_probe(user_id: str, name: str) -> None:
    """Ask for the first verdict on a row whose direct bindings are waiting.

    Imported here because discovery reaches back into the resolver, so the
    module-level edge would close a cycle. Throttled: every workspace resolve
    passes through here, and the row needs one dial, not one per resolve.
    """
    from src.server.services.mcp_oauth.discovery import schedule_catalog_discovery

    schedule_catalog_discovery(user_id, name, reason="resolve", throttle=True)


async def grant_refs(
    resolved: Any,
    *,
    user_id: str,
    snapshots: ToolSnapshotIndex | None = None,
) -> list[GrantRef]:
    """Every grant this workspace should hold, one per server that earns one.

    A connection outranks the row's own headers: while it is servable the relay
    has a token to spend, and a row must never carry two credentials for one
    address. Everything else that is remote, directly bound and probing clean
    earns a header grant naming the row.

    Every OAuth connection earns one, bound directly or not, because the
    sandbox's generated wrappers dial the relay for all of them.

    ``snapshots`` is the probe index; pass one already built, or leave it out
    and it is read only if some row could actually earn a header grant.
    """
    plans = resolved.binding_plans_by_name
    refs: list[GrantRef] = []
    for entry in resolved.entries:
        # The user tier only: a grant resolves its credential from the catalog
        # row or the connection behind it, and a built-in has neither.
        if entry.state is not State.ACTIVE or entry.origin is not Origin.USER:
            continue
        server = entry.config
        if server.oauth_connection_id:
            kind = GRANT_KIND_OAUTH_MCP
        else:
            if not _header_candidate(server):
                continue
            if entry.awaiting_probe:
                # The row's stored direct bindings are clamped to the sandbox
                # until a probe answers, and no turn path kicks one: without
                # this the tool would sit in neither place until the user next
                # opened the Plugins page. A row that has an answer is left
                # alone, whatever the answer was.
                _kick_probe(user_id, entry.name)
                continue
            if entry.name not in plans:
                continue
            if snapshots is None:
                snapshots = await user_snapshots(user_id)
            if not _probe_ok(server, snapshots):
                continue
            kind = GRANT_KIND_HEADER_MCP
        refs.append(
            GrantRef(
                kind=kind,
                server_name=entry.name,
                connection_id=server.oauth_connection_id,
            )
        )
    return refs
