"""
Database layer for sandbox egress grants — the relay's contract.

A grant binds (user, workspace, credential) to one exact destination captured
at creation. The relay authorizes every request with one query here; grant or
connection status flips deny the next request with no sandbox convergence.

Two kinds share the table, and they differ only in what resolves the
credential: ``oauth_mcp`` names the connection whose token the relay spends,
``header_mcp`` names the catalog row whose own headers it sends. Neither
stores a credential value; the grant stores the *reference*, so a rotated
secret and a revoked connection both take effect on the next request rather
than on the next sync.
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row

from src.server.database.mcp_oauth import SERVABLE_PARAM, ConnectionStatus
from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

GRANT_KIND_OAUTH_MCP = "oauth_mcp"
GRANT_KIND_HEADER_MCP = "header_mcp"


@dataclass(frozen=True)
class GrantRef:
    """One grant a workspace should hold: its kind, and what resolves it.

    The reference, never the credential: a connection id for ``oauth_mcp``, the
    catalog row's name for ``header_mcp``. Which rows earn which kind is
    decided in ``services/egress/grant_scope.py``; this layer only writes what
    it is handed, under its own owner and status predicates.
    """

    kind: str
    server_name: str
    connection_id: str | None = None

    @property
    def subject(self) -> str:
        """The column this kind is keyed by."""
        return self.connection_id if self.kind == GRANT_KIND_OAUTH_MCP else self.server_name

    @property
    def key(self) -> tuple[str, str]:
        """The key this ref's grant comes back under.

        The kind rides along because the two subjects are drawn from different
        namespaces: a connection id and a catalog row's name share one dict
        otherwise, and a row named after a uuid would collect the wrong grant.
        """
        return (self.kind, self.subject)


@dataclass(frozen=True)
class GrantSync:
    """The workspace's grant set after one convergence.

    ``grants`` maps :attr:`GrantRef.key` → grant_id for every ref that got one;
    ``retired`` counts the overhang that was revoked, which is what tells a
    caller with no local state that a sandbox still has a credential file to
    tear down.
    """

    grants: dict[tuple[str, str], str]
    retired: int


@dataclass(frozen=True)
class _Policies:
    """One policy per grant, as the parallel arrays the INSERT's ``unnest`` joins.

    ``keys`` holds whatever the kind's join column is (a connection id, a
    server name), so the same shape serves both upserts.
    """

    keys: list[str]
    denylists: list[str | None]
    allowlists: list[str | None]
    required: list[bool]
    direct_only: list[str | None]


def _policy(
    vendor: str | None, granted: Sequence[str], row: Any
) -> tuple[str | None, str | None, bool, str | None]:
    """One grant's policy columns, derived once for every path that writes them."""
    from src.server.services.brokerage_capabilities import denied_tools, tools_for
    from src.server.services.tool_binding import inputs_from_row, resolve_plan

    tools = denied_tools(vendor, granted)
    permitted = tools_for(vendor, granted)
    bound = resolve_plan(vendor, granted, inputs_from_row(row)).sandbox_excluded
    return (
        None if tools is None else json.dumps(sorted(tools)),
        None if permitted is None else json.dumps(sorted(permitted)),
        tools is not None,
        json.dumps(sorted(bound)) if bound else None,
    )


async def _header_policies(
    cur: Any, *, user_id: str, server_names: Sequence[str]
) -> _Policies:
    """The same expansion for a header-authenticated row, off the row itself.

    A header row has no consent record to read: the vendor comes from its own
    address and nothing is granted, which denies a curated vendor's whole
    curation and leaves everything else unpoliced. What the row does decide is
    the direct set, so a tool the user put on the JSON path is still refused to
    a sandbox caller at the relay.
    """
    from src.server.services.brokerage_capabilities import vendor_for_url

    await cur.execute(
        """
        SELECT s.name, s.url, s.transport, s.tool_binding, s.binding_preset,
               s.order_approval
        FROM user_mcp_servers s
        WHERE s.user_id = %s AND s.name = ANY(%s::text[])
        """,
        (user_id, list(server_names)),
    )
    policies = _Policies([], [], [], [], [])
    for row in await cur.fetchall():
        # Granting ``()`` is the deliberate half of that: a row that never went
        # through a consent screen is denied a curated vendor's whole curation,
        # while its uncurated tools flow.
        denylist, allowlist, required, direct_only = _policy(
            vendor_for_url(row["url"]), (), row
        )
        policies.keys.append(row["name"])
        policies.denylists.append(denylist)
        policies.allowlists.append(allowlist)
        policies.required.append(required)
        policies.direct_only.append(direct_only)
    return policies


async def _tool_policies(
    cur: Any, *, user_id: str, connection_ids: Sequence[str]
) -> _Policies:
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

    The fifth array is the direct-only set: granted tools bound to the model
    as JSON tools, which the relay refuses to a sandbox caller so the one
    path per tool holds at the choke point and not only in the composite.

    Returned as parallel arrays for the INSERT's ``unnest`` join. A
    connection we curate no groups for contributes a NULL denial and
    ``policy_required`` false, which the relay reads as no policy at all.
    """
    from src.server.services.brokerage_capabilities import vendor_for_url

    await cur.execute(
        """
        SELECT c.connection_id, c.server_url, c.granted_capabilities,
               s.transport, s.tool_binding, s.binding_preset, s.order_approval
        FROM user_mcp_oauth_connections c
        LEFT JOIN user_mcp_servers s
               ON s.user_id = c.user_id AND s.name = c.server_name
        WHERE c.connection_id = ANY(%s::uuid[]) AND c.user_id = %s
        """,
        (list(connection_ids), user_id),
    )
    policies = _Policies([], [], [], [], [])
    for row in await cur.fetchall():
        # The vendor comes from the consented address, never the row's name.
        # The name is the user's to pick and to edit, so keying on it let a row
        # called anything else at a broker's host carry no policy, and a row
        # holding a broker's name but pointed elsewhere carry the wrong one.
        denylist, allowlist, required, direct_only = _policy(
            vendor_for_url(row["server_url"]), row["granted_capabilities"] or (), row
        )
        policies.keys.append(str(row["connection_id"]))
        policies.denylists.append(denylist)
        policies.allowlists.append(allowlist)
        policies.required.append(required)
        policies.direct_only.append(direct_only)
    return policies


async def lock_user_egress_state(conn, user_id: str) -> None:
    """Serialize every writer of one user's egress policy for the transaction.

    Consent, the binding map and the grant set are each derived from the
    others, and every writer reads the current state before rewriting it, so
    two workers editing the same user must queue rather than interleave. The
    lock is re-entrant within a transaction: a caller that already holds it can
    call into another holder without waiting on itself.
    """
    # Deferred, as sync_egress_grants' import is: writer_guard reaches back
    # into src.server.database.pool, and a module-scope import would close a
    # database -> services -> database loop.
    from src.server.services.writer_guard import advisory_key

    await conn.execute(
        "SELECT pg_advisory_xact_lock(%s)", (advisory_key("EGU", user_id),)
    )


async def _upsert_oauth_grants(
    cur: Any, *, user_id: str, workspace_id: str, connection_ids: Sequence[str]
) -> dict[str, str]:
    """Upsert one grant per connection; returns connection_id → grant_id.

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
    """
    policies = await _tool_policies(
        cur, user_id=user_id, connection_ids=connection_ids
    )
    await cur.execute(
        """
        INSERT INTO sandbox_egress_grants
            (user_id, workspace_id, kind, connection_id,
             destination_url, tool_denylist, tool_allowlist,
             policy_required, tool_direct_only,
             status, created_at, updated_at)
        SELECT %s, %s::uuid, %s, c.connection_id, c.server_url,
               p.denylist, p.allowlist, COALESCE(p.required, false),
               p.direct_only,
               'active', NOW(), NOW()
        FROM user_mcp_oauth_connections c
        LEFT JOIN (
            SELECT * FROM unnest(
                %s::uuid[], %s::text[]::jsonb[],
                %s::text[]::jsonb[], %s::boolean[],
                %s::text[]::jsonb[]
            ) AS t(connection_id, denylist, allowlist, required,
                   direct_only)
        ) p ON p.connection_id = c.connection_id
        WHERE c.connection_id = ANY(%s::uuid[]) AND c.user_id = %s
          AND c.status = ANY(%s)
        ON CONFLICT (workspace_id, kind, connection_id, server_name)
        DO UPDATE SET
            destination_url = EXCLUDED.destination_url,
            tool_denylist = EXCLUDED.tool_denylist,
            tool_allowlist = EXCLUDED.tool_allowlist,
            policy_required = EXCLUDED.policy_required,
            tool_direct_only = EXCLUDED.tool_direct_only,
            status = 'active',
            updated_at = NOW()
        RETURNING connection_id, grant_id
        """,
        (
            user_id, workspace_id, GRANT_KIND_OAUTH_MCP,
            policies.keys, policies.denylists, policies.allowlists,
            policies.required, policies.direct_only,
            list(connection_ids), user_id, SERVABLE_PARAM,
        ),
    )
    return {
        str(row["connection_id"]): str(row["grant_id"])
        for row in await cur.fetchall()
    }


async def _upsert_header_grants(
    cur: Any, *, user_id: str, workspace_id: str, server_names: Sequence[str]
) -> dict[str, str]:
    """Upsert one grant per header-authenticated row; returns name → grant_id.

    Same posture as the OAuth half, with the catalog row standing in for the
    connection: ``destination_url`` is pinned from ``s.url`` inside the INSERT
    rather than passed in, and the row is selected under the owner predicate,
    so a name that is absent or another user's yields no grant. Enabled and
    ``http`` (the one transport the relay dials) are predicates here too, for
    the reason the connection's status is one: the upsert reactivates, so a row
    switched off or turned into a stdio command must not have a grant revived
    on the next sync.

    The last predicate is what keeps the two kinds from overlapping: a row with
    a servable connection is that connection's grant, and a row must never earn
    both.
    """
    policies = await _header_policies(
        cur, user_id=user_id, server_names=server_names
    )
    await cur.execute(
        """
        INSERT INTO sandbox_egress_grants
            (user_id, workspace_id, kind, server_name,
             destination_url, tool_denylist, tool_allowlist,
             policy_required, tool_direct_only,
             status, created_at, updated_at)
        SELECT %s, %s::uuid, %s, s.name, s.url,
               p.denylist, p.allowlist, COALESCE(p.required, false),
               p.direct_only,
               'active', NOW(), NOW()
        FROM user_mcp_servers s
        LEFT JOIN (
            SELECT * FROM unnest(
                %s::text[], %s::text[]::jsonb[],
                %s::text[]::jsonb[], %s::boolean[],
                %s::text[]::jsonb[]
            ) AS t(server_name, denylist, allowlist, required,
                   direct_only)
        ) p ON p.server_name = s.name
        WHERE s.user_id = %s AND s.name = ANY(%s::text[])
          AND s.enabled = TRUE
          AND s.transport = 'http' AND COALESCE(s.url, '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM user_mcp_oauth_connections c
              WHERE c.user_id = s.user_id AND c.server_name = s.name
                AND c.status <> %s
          )
        ON CONFLICT (workspace_id, kind, connection_id, server_name)
        DO UPDATE SET
            destination_url = EXCLUDED.destination_url,
            tool_denylist = EXCLUDED.tool_denylist,
            tool_allowlist = EXCLUDED.tool_allowlist,
            policy_required = EXCLUDED.policy_required,
            tool_direct_only = EXCLUDED.tool_direct_only,
            status = 'active',
            updated_at = NOW()
        RETURNING server_name, grant_id
        """,
        (
            user_id, workspace_id, GRANT_KIND_HEADER_MCP,
            policies.keys, policies.denylists, policies.allowlists,
            policies.required, policies.direct_only,
            user_id, list(server_names),
            ConnectionStatus.REVOKED.value,
        ),
    )
    return {
        str(row["server_name"]): str(row["grant_id"])
        for row in await cur.fetchall()
    }


async def sync_egress_grants(
    *,
    user_id: str,
    workspace_id: str,
    refs: Sequence[GrantRef],
    config_version: int,
) -> GrantSync | None:
    """Make ``refs`` exactly this workspace's active grants, whatever their kind.

    One transaction: upsert a grant per ref, then revoke every other active
    grant of the workspace. Retirement is not optional cleanup: an active
    grant the resolved set no longer contains is an authorization overhang,
    since the sandbox may still hold that grant_id and a live relay JWT, so it
    must not be able to commit separately from the upserts. It sweeps every
    kind, because the set being replaced is the workspace's, not one kind's.

    Each kind's upsert owns its own selection predicates (see them), but they
    share the rule that the destination and the reference are read from the
    row, never taken from the caller.

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
            await lock_user_egress_state(cur, user_id)
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

            # Keyed by (kind, subject): each upsert answers in its own
            # namespace (a connection id, a catalog row's name), and one flat
            # keyspace would let a row named after a uuid answer for a
            # connection.
            granted: dict[tuple[str, str], str] = {}
            connection_ids = [
                r.connection_id
                for r in refs
                if r.kind == GRANT_KIND_OAUTH_MCP and r.connection_id
            ]
            if connection_ids:
                granted |= {
                    (GRANT_KIND_OAUTH_MCP, subject): grant_id
                    for subject, grant_id in (
                        await _upsert_oauth_grants(
                            cur,
                            user_id=user_id,
                            workspace_id=workspace_id,
                            connection_ids=connection_ids,
                        )
                    ).items()
                }
            server_names = [
                r.server_name for r in refs if r.kind == GRANT_KIND_HEADER_MCP
            ]
            if server_names:
                granted |= {
                    (GRANT_KIND_HEADER_MCP, subject): grant_id
                    for subject, grant_id in (
                        await _upsert_header_grants(
                            cur,
                            user_id=user_id,
                            workspace_id=workspace_id,
                            server_names=server_names,
                        )
                    ).items()
                }

            await cur.execute(
                """
                UPDATE sandbox_egress_grants
                SET status = 'revoked', updated_at = NOW()
                WHERE workspace_id = %s AND status = 'active'
                  AND grant_id != ALL(%s::uuid[])
                """,
                (workspace_id, list(granted.values())),
            )
            if cur.rowcount:
                logger.info(
                    f"[egress_grants_db] retired {cur.rowcount} stale grant(s) "
                    f"for workspace {workspace_id}"
                )
            return GrantSync(grants=granted, retired=cur.rowcount)


async def fetch_grant_for_relay(grant_id: str) -> dict[str, Any] | None:
    """The relay's per-request authorization read.

    Authorization only, no credential. Every kind resolves its own credential
    afterwards, from the reference this read carries, so the hot path decrypts
    nothing here. None for an unknown grant_id (the route answers a uniform 404
    for absent and wrong-scope alike), and likewise for an ``oauth_mcp`` grant
    whose connection row is gone: the credential's identity has vanished, which
    is the same answer as never having had one.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT g.user_id, g.workspace_id, g.kind, g.connection_id,
                       g.server_name,
                       g.destination_url, g.allowed_methods, g.tool_denylist,
                       g.tool_direct_only,
                       g.status AS grant_status,
                       c.status AS connection_status
                FROM sandbox_egress_grants g
                LEFT JOIN user_mcp_oauth_connections c
                       ON c.connection_id = g.connection_id
                WHERE g.grant_id = %s
                """,
                (grant_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            connection_id = row["connection_id"]
            if row["kind"] == GRANT_KIND_OAUTH_MCP and row["connection_status"] is None:
                return None
            return {
                "user_id": row["user_id"],
                "workspace_id": str(row["workspace_id"]),
                "kind": row["kind"],
                "connection_id": str(connection_id) if connection_id else None,
                "server_name": row["server_name"],
                "destination_url": row["destination_url"],
                "allowed_methods": row["allowed_methods"],
                "tool_denylist": row["tool_denylist"],
                "tool_direct_only": row["tool_direct_only"],
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
    from src.server.services.brokerage_capabilities import vendor_for_url

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
            await lock_user_egress_state(cur, str(owner["user_id"]))
            await cur.execute(
                """
                SELECT c.server_url, c.granted_capabilities,
                       s.transport, s.tool_binding, s.binding_preset,
                       s.order_approval
                FROM user_mcp_oauth_connections c
                LEFT JOIN user_mcp_servers s
                       ON s.user_id = c.user_id AND s.name = c.server_name
                WHERE c.connection_id = %s
                """,
                (connection_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return 0
            # Both columns, for the reason ``_tool_policies`` gives: the other
            # blue/green colour enforces the allowlist, and a consent change
            # that touched only the denial never reached it.
            denylist, allowlist, required, direct_only = _policy(
                vendor_for_url(row["server_url"]),
                row["granted_capabilities"] or (),
                row,
            )
            await cur.execute(
                """
                UPDATE sandbox_egress_grants
                SET tool_denylist = %s::jsonb, tool_allowlist = %s::jsonb,
                    policy_required = %s, tool_direct_only = %s::jsonb,
                    updated_at = NOW()
                WHERE connection_id = %s AND status = 'active'
                """,
                (denylist, allowlist, required, direct_only, connection_id),
            )
            if cur.rowcount:
                logger.info(
                    f"[egress_grants_db] applied consent to {cur.rowcount} active "
                    f"grant(s) for connection {connection_id}"
                )
            return cur.rowcount


async def apply_binding_to_active_header_grants(
    user_id: str, server_name: str, *, conn=None
) -> int:
    """Rewrite the policy on every active header grant of one row. Returns count.

    The header half of :func:`apply_consent_to_active_grants`, which is keyed
    by connection and so never reaches a grant that names a row instead. The
    reason is the same one: the relay reads the grant, so a tool moved onto the
    direct path stays callable from a sandbox already holding one, and a tool
    moved back off it stays refused, until whenever the next sync happens to
    run.

    Idempotent, and a row with no header grant pays one UPDATE that matches
    nothing; the lookup runs on (user_id, kind, server_name), which no index
    leads with yet, so it is a scan of a table that only ever grows by the
    workspace count. ``conn`` joins the caller's transaction,
    which is what lands the binding map and the policy enforcing it together.
    """
    from src.server.services.brokerage_capabilities import vendor_for_url

    async with get_db_connection(conn) as db, db.transaction():
        async with db.cursor(row_factory=dict_row) as cur:
            await lock_user_egress_state(cur, user_id)
            await cur.execute(
                """
                SELECT s.url, s.transport, s.tool_binding, s.binding_preset,
                       s.order_approval
                FROM user_mcp_servers s
                WHERE s.user_id = %s AND s.name = %s
                """,
                (user_id, server_name),
            )
            row = await cur.fetchone()
            if row is None:
                return 0
            # Granting ``()`` for the reason ``_header_policies`` gives: a row
            # that never went through a consent screen is denied a curated
            # vendor's whole curation.
            denylist, allowlist, required, direct_only = _policy(
                vendor_for_url(row["url"]), (), row
            )
            await cur.execute(
                """
                UPDATE sandbox_egress_grants
                SET tool_denylist = %s::jsonb, tool_allowlist = %s::jsonb,
                    policy_required = %s, tool_direct_only = %s::jsonb,
                    updated_at = NOW()
                WHERE user_id = %s AND kind = %s AND server_name = %s
                  AND status = 'active'
                """,
                (
                    denylist, allowlist, required, direct_only,
                    user_id, GRANT_KIND_HEADER_MCP, server_name,
                ),
            )
            if cur.rowcount:
                logger.info(
                    f"[egress_grants_db] applied binding to {cur.rowcount} active "
                    f"header grant(s) for server {server_name!r}"
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
