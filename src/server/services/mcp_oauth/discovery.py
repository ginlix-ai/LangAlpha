"""Host-side tool discovery for OAuth-connected user servers.

OAuth servers are never probed from a sandbox (no token exists there); a
short-lived SDK session runs here instead — on connect and on manual refresh.
The cache row lives in ``user_mcp_tool_schemas``; a schema-digest change fans
out a version bump so sessions re-resolve, while an unchanged re-discovery
stays silent.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging

from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

from src.server.database.mcp_oauth import SERVABLE, get_connection
from src.server.database.mcp_servers import (
    bump_user_workspaces_mcp_version,
    get_catalog_server,
)
from src.server.database.mcp_tool_schemas import (
    SchemaWrite,
    get_user_tool_schemas,
    upsert_user_tool_schemas,
)
from src.server.database.pool import get_db_connection
from src.server.services.mcp_identity import bounded_identity
from src.server.services.mcp_oauth.lifecycle import (
    TokenUnavailable,
    ensure_fresh_access_token,
)

logger = logging.getLogger(__name__)

DISCOVERY_TIMEOUT_S = 30


def _schema_digest(tools: list[dict]) -> str:
    canonical = json.dumps(tools, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _discarded(server_name: str, write: SchemaWrite) -> TokenUnavailable:
    """The connection left the servable set while we were on the network.

    Answered exactly as the entry gate would have: the status IS the reason, so
    a disconnect that lands mid-discovery reads the same to the caller (409)
    whether it beat the gate or the write.
    """
    reason = write.connection_status or "unknown_connection"
    logger.info(
        "[mcp_oauth] dropped a discovery write for %s: connection is %s",
        server_name, reason,
    )
    return TokenUnavailable(reason)


def _superseded(server_name: str) -> TokenUnavailable:
    """The catalog config moved while we were on the network.

    The edit that moved it schedules its own rediscovery, which owns
    convergence — landing this result would key the snapshot to a dead
    fingerprint and delete the current one.
    """
    logger.info(
        "[mcp_oauth] dropped a stale discovery write for %s: config changed",
        server_name,
    )
    return TokenUnavailable("superseded", "server config changed during discovery")


async def refresh_user_tool_schemas(user_id: str, server_name: str) -> dict:
    """Discover an OAuth server's tools host-side and cache the snapshot.

    Returns the cache row. Never raises for discovery failures — they land as
    an ``error`` row (the no-downgrade upsert keeps the last good snapshot).
    Raises :class:`TokenUnavailable` when the connection is unusable, including
    when it stops being usable while this discovery is in flight.
    """
    from src.server.services.mcp_config import (
        same_consented_url,
        user_row_to_server_config,
    )
    from src.server.services.mcp_discovery import (
        ToolSnapshotIndex,
        mcp_discovery_fingerprint,
        sanitize_discovered_tools,
    )
    from ptc_agent.core.mcp_schema import client_identity
    from src.server.services.mcp_oauth.http import pinned_discovery_client
    from src.server.utils.egress_guard import pin_public_url

    row = await get_catalog_server(user_id, server_name)
    if row is None:
        raise TokenUnavailable("unknown_server")
    connection = await get_connection(user_id, server_name)
    if connection is None:
        raise TokenUnavailable("unknown_connection")
    if connection.status not in SERVABLE:
        # Raised rather than recorded: a connection the user has to repair is
        # the caller's answer (409), not a discovery error row that would then
        # read as "this server's tools are broken".
        raise TokenUnavailable(str(connection.status))
    # This is where the token meets the URL: an edit may have moved the catalog
    # row off the endpoint the user consented to (and the revoke that follows
    # such an edit is not atomic with it), so re-bind here rather than trust
    # that every write path got there first.
    if connection.server_url and not same_consented_url(
        connection.server_url, row.get("url")
    ):
        raise TokenUnavailable("needs_reauth", "server URL changed since consent")

    server = user_row_to_server_config(
        row, oauth_connection_id=connection.connection_id
    )
    fingerprint = mcp_discovery_fingerprint(server)

    async def _write(**upsert_kwargs) -> dict:
        # One transaction covers the fingerprint re-check and the write: a
        # concurrent edit either commits first (this result is discarded as
        # stale) or blocks on the FOR SHARE until this snapshot lands and the
        # edit's own rediscovery supersedes it. Without the fence, a slow
        # discovery finishing after a newer one deletes the current config's
        # snapshot and resurrects a dead fingerprint's.
        async with get_db_connection() as conn:
            async with conn.transaction():
                current = await get_catalog_server(
                    user_id, server_name, conn=conn, for_share=True
                )
                if current is None or mcp_discovery_fingerprint(
                    user_row_to_server_config(
                        current, oauth_connection_id=connection.connection_id
                    )
                ) != fingerprint:
                    raise _superseded(server_name)
                write = await upsert_user_tool_schemas(
                    user_id, server_name, fingerprint,
                    connection_id=connection.connection_id, conn=conn,
                    **upsert_kwargs,
                )
        if write.row is None:
            # Refused under the write's own lock — the disconnect already
            # purged what we would be re-adding.
            raise _discarded(server_name, write)
        return write.row

    async def _fail(error: str) -> dict:
        return await _write(status="error", error=error)

    try:
        token = await ensure_fresh_access_token(connection.connection_id)
    except TokenUnavailable as e:
        return await _fail(f"token unavailable: {e.reason}")

    url = row["url"]
    try:
        # One pin for the whole session. The SDK dials hostnames itself, so a
        # validation that merely PRECEDES the connect leaves the rebinding
        # TOCTOU open — the pin has to travel with the requests, which is what
        # the transport inside pinned_discovery_client does (it also refuses
        # redirects and bounds the response, neither of which the SDK does).
        target = await pin_public_url(url)
    except Exception as e:
        return await _fail(f"blocked url: {e}")

    headers = {"Authorization": token.header()}
    try:
        async with asyncio.timeout(DISCOVERY_TIMEOUT_S):
            async with pinned_discovery_client(target, headers=headers) as http_client:
                # The streams context manager IS the SDK's Transport protocol.
                transport = streamable_http_client(url, http_client=http_client)
                async with Client(transport) as client:
                    result = await client.list_tools(cache_mode="refresh")
                    # The handshake already asked who this is; read it here
                    # rather than reconnect later for a field the connection
                    # is holding. Never raises, so it cannot demote a good
                    # discovery to an error row.
                    identity = client_identity(client)
    except Exception as e:
        logger.warning(
            "[mcp_oauth] discovery failed for %s: %s", server_name, e
        )
        return await _fail(f"discovery failed: {e}")

    raw_tools = [
        {
            "name": t.name,
            "description": t.description or "",
            "input_schema": t.input_schema or {},
        }
        for t in result.tools
    ]
    kept, skipped = sanitize_discovered_tools(raw_tools)
    for name, reason in skipped:
        logger.info(
            "[mcp_oauth] skipped tool %r on %s: %s", name, server_name, reason
        )

    digest = _schema_digest(kept)
    # Same acceptance rule as every other consumer: only a snapshot taken under
    # this server's CURRENT fingerprint is comparable.
    prior = ToolSnapshotIndex(
        user_rows=await get_user_tool_schemas(user_id)
    ).snapshot(server)
    cached_row = await _write(
        tools=kept,
        status="ok",
        schema_digest=digest,
        observed_meta={
            "skipped": [list(s) for s in skipped],
            "server_info": bounded_identity(identity),
        },
    )
    if prior is None or prior.get("schema_digest") != digest:
        # Tool surface changed → sessions must regenerate wrappers.
        await bump_user_workspaces_mcp_version(user_id)
    logger.info(
        "[mcp_oauth] discovered %d tools on %s (digest %s)",
        len(kept), server_name, digest[:12],
    )
    return cached_row


# Strong refs: asyncio holds only weak references to tasks, so a bare
# fire-and-forget handle can be garbage-collected mid-flight.
_rediscovery_tasks: set[asyncio.Task] = set()


def schedule_post_edit_rediscovery(
    user_id: str, name: str, *, prior: dict | None, updated: dict
) -> None:
    """Re-run host-side discovery after an edit moves a connected server's
    discovery fingerprint without moving consent.

    The user-tier snapshot serves only under the CURRENT fingerprint, host-side
    OAuth servers are excluded from sandbox discovery, and no other path
    re-discovers — without this, a consent-preserving edit (a header, a trailing
    slash, ``discovery_uses_secrets``) empties the connector's tools in every
    workspace until a manual refresh. Fire-and-forget: discovery is a vendor
    network round-trip and must not hold the write's response.
    """
    if prior is None:
        return
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    if mcp_discovery_fingerprint(
        user_row_to_server_config(prior)
    ) == mcp_discovery_fingerprint(user_row_to_server_config(updated)):
        return

    async def _refresh() -> None:
        from src.server.services.mcp_oauth.connect import _resync_live_sandboxes

        try:
            await refresh_user_tool_schemas(user_id, name)
        except TokenUnavailable:
            # No usable connection behind this row: sandbox-side discovery
            # owns it (or a reconnect will), nothing to refresh host-side.
            return
        except Exception:
            logger.warning(
                "[mcp_oauth] post-edit rediscovery failed for %s",
                name, exc_info=True,
            )
            return
        await _resync_live_sandboxes(user_id)

    task = asyncio.create_task(_refresh(), name=f"mcp-rediscover-{name}")
    _rediscovery_tasks.add(task)
    task.add_done_callback(_rediscovery_tasks.discard)
