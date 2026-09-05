"""
Database layer for sandbox egress grants — the relay's contract.

A grant binds (user, workspace, credential) to one exact destination captured
at creation. The relay authorizes every request with one query here; grant or
connection status flips deny the next request with no sandbox convergence.
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row

from src.server.database.mcp_oauth import SERVABLE_PARAM
from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

GRANT_KIND_OAUTH_MCP = "oauth_mcp"


@dataclass(frozen=True)
class GrantSync:
    """The workspace's grant set after one convergence.

    ``grants`` maps connection_id → grant_id for every connection that got one;
    ``retired`` counts the overhang that was revoked, which is what tells a
    caller with no local state that a sandbox still has a credential file to
    tear down.
    """

    grants: dict[str, str]
    retired: int


async def _tool_policies(
    cur: Any, *, user_id: str, connection_ids: Sequence[str]
) -> tuple[list[str], list[str | None], list[str | None], list[bool]]:
    """Expand each connection's stored consent into the denial its grant carries.

    Read inside the caller's transaction and keyed only by connection_id, so
    the policy has the same provenance as ``destination_url``: the DB row, not
    a caller argument. Expansion happens here rather than being stored because
    the curation map is source, so a curation change ships with a deploy and
    applies at the next sync, with no row to migrate and no way for the two to
    drift.

    The denial names the curated tools whose group the user declined, so
    curating a tool the user did not consent to is what *adds* a refusal. A
    vendor's newly published tool is in no group and so in no denial, which is
    the deliberate choice this policy makes.

    The permitted set is derived alongside it and written to the old
    ``tool_allowlist`` column, which nothing in this version reads. That column
    is what the previous version enforces, and both versions serve at once
    through a blue/green cutover -- so writing only the denial left the draining
    colour reading a policy frozen at the last deploy, still honouring a group
    the user had since declined. The two disagree on an uncurated tool (the
    allowlist refuses it, the denylist permits it), and the stricter reading is
    the right one to hand a version on its way out.

    Returned as four parallel arrays for the INSERT's ``unnest`` join. A
    connection we curate no groups for contributes a NULL denial and
    ``policy_required`` false, which the relay reads as no policy at all.
    """
    from src.server.services.brokerage_capabilities import (
        denied_tools,
        tools_for,
        vendor_for_url,
    )

    await cur.execute(
        """
        SELECT connection_id, server_url, granted_capabilities
        FROM user_mcp_oauth_connections
        WHERE connection_id = ANY(%s::uuid[]) AND user_id = %s
        """,
        (list(connection_ids), user_id),
    )
    ids: list[str] = []
    denylists: list[str | None] = []
    allowlists: list[str | None] = []
    required: list[bool] = []
    for row in await cur.fetchall():
        # The vendor comes from the consented address, never the row's name.
        # The name is the user's to pick and to edit, so keying on it let a row
        # called anything else at a broker's host carry no policy, and a row
        # holding a broker's name but pointed elsewhere carry the wrong one.
        vendor = vendor_for_url(row["server_url"])
        granted = row["granted_capabilities"] or ()
        tools = denied_tools(vendor, granted)
        permitted = tools_for(vendor, granted)
        ids.append(str(row["connection_id"]))
        denylists.append(None if tools is None else json.dumps(sorted(tools)))
        allowlists.append(
            None if permitted is None else json.dumps(sorted(permitted))
        )
        required.append(tools is not None)
    return ids, denylists, allowlists, required


async def sync_oauth_grants(
    *,
    user_id: str,
    workspace_id: str,
    connection_ids: Sequence[str],
    config_version: int,
) -> GrantSync | None:
    """Make ``connection_ids`` exactly this workspace's active OAuth grants.

    One transaction: upsert a grant per connection, then revoke every other
    active grant of the workspace. Retirement is not optional cleanup — an
    active grant the resolved set no longer contains is an authorization
    overhang, since the sandbox may still hold that grant_id and a live relay
    JWT — so it must not be able to commit separately from the upserts.

    The relay dials ``destination_url``, and it is taken from the connection's
    consented ``server_url`` inside the INSERT — never from a caller argument.
    That is the whole security posture: a mutable catalog-row URL can never
    steer a grant at a host the token wasn't issued for. Connections are
    likewise *selected* under the owner predicate rather than trusted, so an id
    that is absent or another user's simply produces no grant (and is then
    retired like any other): a caller that guessed an id learns nothing. That
    same SELECT carries the servable-status predicate, since the upsert's
    ``status = 'active'`` would otherwise reactivate a grant on a connection
    that has since been revoked or needs re-auth.

    Returns None, having touched no grant row, when ``config_version`` no
    longer matches ``workspaces.mcp_config_version`` — the caller resolved
    against a superseded config and a newer sync owns the set. This is a
    whole-set replacement, so two workers cannot be merged by row locks: the
    stale one would reactivate what the fresh one just revoked.
    """
    # Deferred like platform_secret_sweep's: writer_guard reaches back into
    # src.server.database.pool, so importing it at module scope from the
    # database layer would close a database → services → database loop.
    from src.server.services.writer_guard import advisory_key

    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            # The owner's lock first, then this workspace's. The outer one is
            # what a narrowing consent holds while it rewrites the policy on
            # grants that already exist: taken here, before the connection rows
            # are read, a sync creating this connection's first grant in a brand
            # new workspace cannot read the old consent and commit it after the
            # narrowing. Always in this order, so two holders never wait on each
            # other.
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("EGU", user_id),),
            )
            # Serialize this workspace's replacements across workers, THEN
            # re-read the version under that lock. The lock alone would only
            # order a stale writer last; the CAS alone could pass and then be
            # overtaken. Together they make "CAS passed" mean no newer set can
            # commit ahead of this one. Transaction-scoped, so a worker that
            # dies mid-replacement releases it.
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("EG", workspace_id),),
            )
            await cur.execute(
                "SELECT mcp_config_version FROM workspaces WHERE workspace_id = %s",
                (workspace_id,),
            )
            ws_row = await cur.fetchone()
            # A workspace that no longer exists matches no version, so it also
            # replaces nothing.
            live_version = ws_row["mcp_config_version"] if ws_row else None
            if live_version != config_version:
                logger.info(
                    f"[egress_grants_db] stale grant replacement for workspace "
                    f"{workspace_id} (resolved v{config_version}, live "
                    f"v{live_version}) — left to the newer sync"
                )
                return None

            granted: dict[str, str] = {}
            if connection_ids:
                ids, denylists, allowlists, required = await _tool_policies(
                    cur, user_id=user_id, connection_ids=connection_ids
                )
                await cur.execute(
                    """
                    INSERT INTO sandbox_egress_grants
                        (user_id, workspace_id, kind, connection_id,
                         destination_url, tool_denylist, tool_allowlist,
                         policy_required, status, created_at, updated_at)
                    SELECT %s, %s::uuid, %s, c.connection_id, c.server_url,
                           p.denylist, p.allowlist, COALESCE(p.required, false),
                           'active', NOW(), NOW()
                    FROM user_mcp_oauth_connections c
                    LEFT JOIN (
                        SELECT * FROM unnest(
                            %s::uuid[], %s::text[]::jsonb[],
                            %s::text[]::jsonb[], %s::boolean[]
                        ) AS t(connection_id, denylist, allowlist, required)
                    ) p ON p.connection_id = c.connection_id
                    WHERE c.connection_id = ANY(%s::uuid[]) AND c.user_id = %s
                      AND c.status = ANY(%s)
                    ON CONFLICT (workspace_id, kind, connection_id) DO UPDATE SET
                        destination_url = EXCLUDED.destination_url,
                        tool_denylist = EXCLUDED.tool_denylist,
                        tool_allowlist = EXCLUDED.tool_allowlist,
                        policy_required = EXCLUDED.policy_required,
                        status = 'active',
                        updated_at = NOW()
                    RETURNING connection_id, grant_id
                    """,
                    (
                        user_id, workspace_id, GRANT_KIND_OAUTH_MCP,
                        ids, denylists, allowlists, required,
                        list(connection_ids), user_id, SERVABLE_PARAM,
                    ),
                )
                granted = {
                    str(row["connection_id"]): str(row["grant_id"])
                    for row in await cur.fetchall()
                }

            await cur.execute(
                """
                UPDATE sandbox_egress_grants
                SET status = 'revoked', updated_at = NOW()
                WHERE workspace_id = %s AND kind = %s AND status = 'active'
                  AND grant_id != ALL(%s::uuid[])
                """,
                (
                    workspace_id, GRANT_KIND_OAUTH_MCP,
                    list(granted.values()),
                ),
            )
            if cur.rowcount:
                logger.info(
                    f"[egress_grants_db] retired {cur.rowcount} stale grant(s) "
                    f"for workspace {workspace_id}"
                )
            return GrantSync(grants=granted, retired=cur.rowcount)


async def fetch_grant_for_relay(grant_id: str) -> dict[str, Any] | None:
    """The relay's per-request authorization read.

    Authorization only — no credential. The vendor token comes from the OAuth
    lifecycle (which owns refresh and the generation CAS), so this stays a
    cheap non-decrypting read on the hot path. None for an unknown grant_id
    (the route answers a uniform 404 for absent and wrong-scope alike).
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT g.user_id, g.workspace_id, g.connection_id,
                       g.destination_url, g.allowed_methods, g.tool_denylist,
                       g.status AS grant_status,
                       c.status AS connection_status
                FROM sandbox_egress_grants g
                JOIN user_mcp_oauth_connections c ON c.connection_id = g.connection_id
                WHERE g.grant_id = %s
                """,
                (grant_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "user_id": row["user_id"],
                "workspace_id": str(row["workspace_id"]),
                "connection_id": str(row["connection_id"]),
                "destination_url": row["destination_url"],
                "allowed_methods": row["allowed_methods"],
                "tool_denylist": row["tool_denylist"],
                "grant_status": row["grant_status"],
                "connection_status": row["connection_status"],
            }


async def apply_consent_to_active_grants(connection_id: str, *, conn=None) -> int:
    """Rewrite the denial on every active grant of a connection. Returns count.

    Narrowing consent has to bite when the user confirms it, not when something
    later happens to resolve. A reconnect writes the new keys onto the
    connection and then leans on a version bump plus a best-effort re-apply to
    reach the grants -- and the relay reads the grant, not the connection. So a
    user who reconnected specifically to switch trading *off* kept an agent that
    could place orders: for as long as the proactive apply took, and
    indefinitely if it failed, since its failure is a warning log.

    Cheap enough to be unconditional (one UPDATE keyed on an indexed column) and
    idempotent, so it costs a widening reconnect nothing to run it too. The
    later sync stays: this converges the grants that exist right now, and that
    is a different question from which grants a workspace should have.

    Takes the owner's grant-sync lock first, for the whole read-and-write.
    Without it the two overlap in the one order that loses: a sync reads the
    old consent, this narrows the grant, and the sync's upsert then writes the
    old policy back over it -- and there is no version bump left to make the
    sync notice, since it CASed successfully before any of this began. Under
    the lock the sync either commits entirely before this reads, or reads the
    consent this connect already wrote. The consent is therefore re-read below,
    under the lock, rather than carried in from the query that found the owner.

    One lock over the user, not one per workspace. Fencing the workspaces this
    query can see leaves out the one that matters most: a workspace being
    created right now holds no grant, appears in no enumeration, and its first
    sync is exactly the writer that has read the old consent and not yet
    committed. There is no later pass to correct it, since the version bump has
    already gone out. Every grant sync takes this lock before its own
    per-workspace one, so the order is fixed and no cycle can form.

    ``conn`` joins the caller's transaction. The connect callback passes the one
    it wrote the connection row on, so the consent and the policy enforcing it
    land together.
    """
    from src.server.services.brokerage_capabilities import (
        denied_tools,
        tools_for,
        vendor_for_url,
    )
    from src.server.services.writer_guard import advisory_key

    async with get_db_connection(conn) as db, db.transaction():
        async with db.cursor(row_factory=dict_row) as cur:
            # Bounded, because this runs in the OAuth callback and waits on a
            # lock the owner's own workspaces are taking. Postgres raises rather
            # than waiting out a wedged holder, the raise unwinds the connect's
            # whole transaction, and the user sees a failed connect instead of a
            # hung one -- with nothing written, so a retry starts clean. Generous
            # on purpose: a grant sync holds this for a handful of short
            # statements, so reaching five seconds means something is stuck, not
            # that the system is busy.
            await cur.execute("SET LOCAL lock_timeout = '5s'")
            await cur.execute(
                """
                SELECT user_id FROM user_mcp_oauth_connections
                WHERE connection_id = %s
                """,
                (connection_id,),
            )
            owner = await cur.fetchone()
            if owner is None:
                return 0
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("EGU", str(owner["user_id"])),),
            )
            await cur.execute(
                """
                SELECT server_url, granted_capabilities
                FROM user_mcp_oauth_connections
                WHERE connection_id = %s
                """,
                (connection_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return 0
            vendor = vendor_for_url(row["server_url"])
            granted = row["granted_capabilities"] or ()
            tools = denied_tools(vendor, granted)
            # Both columns, for the reason ``_tool_policies`` gives: the other
            # blue/green colour enforces the allowlist, and a consent change
            # that touched only the denial never reached it.
            permitted = tools_for(vendor, granted)
            await cur.execute(
                """
                UPDATE sandbox_egress_grants
                SET tool_denylist = %s::jsonb, tool_allowlist = %s::jsonb,
                    policy_required = %s, updated_at = NOW()
                WHERE connection_id = %s AND status = 'active'
                """,
                (
                    None if tools is None else json.dumps(sorted(tools)),
                    None if permitted is None else json.dumps(sorted(permitted)),
                    tools is not None,
                    connection_id,
                ),
            )
            if cur.rowcount:
                logger.info(
                    f"[egress_grants_db] applied consent to {cur.rowcount} active "
                    f"grant(s) for connection {connection_id}"
                )
            return cur.rowcount


async def revoke_grants_for_connection(connection_id: str, *, conn=None) -> int:
    """Flip every grant of a connection to revoked. Returns count."""
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                """
                UPDATE sandbox_egress_grants
                SET status = 'revoked', updated_at = NOW()
                WHERE connection_id = %s AND status != 'revoked'
                """,
                (connection_id,),
            )
            if cur.rowcount:
                logger.info(
                    f"[egress_grants_db] revoked {cur.rowcount} grant(s) "
                    f"for connection {connection_id}"
                )
            return cur.rowcount
