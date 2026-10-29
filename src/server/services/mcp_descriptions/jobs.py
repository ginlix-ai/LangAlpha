"""Discovery's hook into the description queue.

Called wherever a server's tool list is cached. It enqueues only when the
scope's current description was written from other tools, so an unchanged
rediscovery (the common case) costs two reads and writes nothing. The sync that
called it never waits on a model: the worker picks the job up on its own.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from ptc_agent.config.core import MCPServerConfig
from urllib.parse import urlsplit

from src.config.settings import get_mcp_descriptions_config
from src.server.database import mcp_descriptions as db
from src.server.database.pool import get_db_connection
from src.server.services.brokerages import brokerage_by_name
from src.server.services.mcp_descriptions.keys import (
    Scope,
    described_tools,
    dial_identity,
    server_digest,
)
from src.server.services.mcp_descriptions.schema import PROFILE_PROMPT_VERSION

logger = logging.getLogger(__name__)


async def owners_of(scope: Scope, user_id: str | None = None, *, conn=None) -> set[str]:
    """Every user whose catalog row reads this scope's description: the users
    whose grant at the brokerage resolves to it, or the one user whose own
    server it is."""
    if scope.brokerage is None:
        return {user_id} if user_id else set()
    host = urlsplit(brokerage_by_name(scope.brokerage).url).hostname or ""
    return {
        str(row["user_id"])
        for row in await db.rows_at_host(host, conn=conn)
        if Scope.of(row["user_id"], row["name"], row["url"], row["connection"]) == scope
    }


def own_description_scopes(user_id: str, servers: Iterable[MCPServerConfig]) -> list[str]:
    """The description scopes to drop with a purge of these servers' snapshots,
    whose tools may have changed with no edit to their config, as when a
    secret their discovery uses changes. Their rediscovery queues the new
    descriptions. A shared brokerage description is never dropped for one
    user's secret."""
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    scopes = [
        Scope.of(user_id, server.name, server.url, None, mcp_discovery_fingerprint(server))
        for server in servers
    ]
    return [s.key for s in scopes if s.brokerage is None]


async def own_description_scope(user_id: str, server_name: str, *, conn=None) -> list[str]:
    """The private description scope of one catalog row, for a connect or a
    disconnect to drop with its own write. The scope follows the row's config,
    which an OAuth connection never moves, so another account signed in on the
    same row would otherwise read the last one's description. None for a row
    that is gone or does not parse, which no agent reads a description of."""
    from src.server.database.mcp_servers import get_catalog_server
    from src.server.services.mcp_config import user_row_to_server_config

    row = await get_catalog_server(user_id, server_name, conn=conn)
    if row is None:
        return []
    try:
        server = user_row_to_server_config(row)
    except Exception:
        return []
    return own_description_scopes(user_id, [server])


async def enqueue_if_new(
    *,
    user_id: str,
    server_name: str,
    tools: list[dict[str, Any]],
    from_vendor: bool,
    settled: Mapping[str, Any] | None = None,
    conn=None,
) -> bool:
    """Queue this tool list if the scope's current description was written from
    other tools, under the row and grant as they stand now. Never raises: a
    failure here must not fail the discovery write it rides on.

    ``from_vendor`` says the server dialled the vendor itself for the list.
    Only such a list may write a shared brokerage description, which reaches
    other users' agents; a list from a sandbox or the user's own computer is
    whatever that machine reports. A changed grant is a reconnect, and a
    reconnect rediscovers from the vendor, so the new grant is queued here too.
    An empty list is queued like any other, so the description of the tools
    the server had is dropped; the worker writes none for it.

    ``settled`` is the snapshot a list read from storage came from, which
    ``enqueue_job`` checks still stands.
    """
    config = get_mcp_descriptions_config()
    if not config.enabled:
        return False
    try:
        # A savepoint inside a caller's transaction, so a failure here rolls
        # back this alone and the discovery write still commits.
        async with get_db_connection(conn) as db_conn, db_conn.transaction():
            row = await db.row_for(user_id, server_name, conn=db_conn)
            if row is None:
                return False
            scope = Scope.of(
                user_id, server_name, row["url"], row["connection"], dial_identity(server_name, row)
            )
            if scope.brokerage is not None and not from_vendor:
                return False
            # The user's own words outrank a generated description, so one
            # written under them would never be read.
            # A row the user described still queues: a changed surface drops the
            # old description, which would show again if they cleared theirs,
            # and the worker writes none while theirs stands.
            if scope.brokerage is None and not config.user_servers:
                return False
            queued, dropped = await db.enqueue_job(
                scope_key=scope.key,
                server_digest=server_digest(described_tools(scope, tools)),
                prompt_version=PROFILE_PROMPT_VERSION,
                user_id=None if scope.brokerage else user_id,
                server_name=server_name,
                tools=tools,
                settled=settled,
                conn=db_conn,
            )
            # Sessions hold the tool view their version was resolved at, so the
            # dropped description stays in front of them until they are bumped.
            if dropped:
                await db.bump_workspaces(
                    await owners_of(scope, user_id, conn=db_conn), conn=db_conn
                )
    except Exception:
        logger.warning(
            "[mcp_descriptions] could not queue %s for %s", server_name, user_id, exc_info=True
        )
        return False
    if queued:
        logger.info("[mcp_descriptions] queued %s (%d tools)", scope.key, len(tools))
    return queued
