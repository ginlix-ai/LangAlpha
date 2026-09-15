"""Discovery schema cache for MCP tool snapshots — workspace and user tiers.

Both tables have the same shape and are keyed by ``(owner, server_name,
config_hash)``: a per-server config fingerprint, so adding or toggling an
unrelated server never orphans a snapshot. The SQL therefore lives here once,
parameterized by a ``_SchemaTier`` descriptor — the tiers differ only in the
owner column and in the user tier's extra ``schema_digest``, which lets OAuth
fan-out fire only when tool content actually changed.

Snapshot reads are deliberately decoupled from ``workspaces.mcp_config_version``
(that lives in ``mcp_servers``): the caller compares a row's ``config_hash``
against the server's CURRENT fingerprint to decide hit vs. stale.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.mcp_oauth import SERVABLE_PARAM, ConnectionStatus
from src.server.database.mcp_servers import _bump_version, bump_user_versions
from src.server.database.pool import get_db_connection


# SQL identifiers can never be bound parameters, so a tier's table/column names
# are f-string interpolated into the statements below. These allowlists are what
# makes that safe: a tier is only constructible from names that exist here.
_ALLOWED_TABLES = frozenset({"workspace_mcp_tool_schemas", "user_mcp_tool_schemas"})
_ALLOWED_COLUMNS = frozenset({"workspace_id", "user_id"})


@dataclass(frozen=True)
class _SchemaTier:
    """The identifiers that distinguish one snapshot tier from the other."""

    table: str
    owner_col: str
    has_digest: bool

    def __post_init__(self) -> None:
        if self.table not in _ALLOWED_TABLES:
            raise ValueError(f"Unknown schema table: {self.table!r}")
        if self.owner_col not in _ALLOWED_COLUMNS:
            raise ValueError(f"Unknown schema column: {self.owner_col!r}")

    @property
    def columns(self) -> tuple[str, ...]:
        """Full column list, in INSERT order (``discovered_at`` last)."""
        digest = ("schema_digest",) if self.has_digest else ()
        return (
            self.owner_col, "server_name", "config_hash", "tools", "status",
            "error", *digest, "observed_meta", "last_probe", "discovered_at",
        )


WORKSPACE_TIER = _SchemaTier("workspace_mcp_tool_schemas", "workspace_id", False)
USER_TIER = _SchemaTier("user_mcp_tool_schemas", "user_id", True)

# A non-ok write must never downgrade an existing same-hash ``ok`` row: the
# config is unchanged, so the cached tools are still valid. Only ``error`` and
# ``last_probe`` are taken from the failing write.
#
# Those two are what keeps the rule from lying about the server. Everything the
# no-downgrade arm holds on to describes the tools this row still serves;
# ``last_probe`` is the probe's own verdict, and a server that discovered fine
# and now answers 401 has to say so while its cached tools stay servable.
_DOWNGRADE = "t.status = 'ok' AND EXCLUDED.status <> 'ok'"


def _keep_on_downgrade(col: str, fresh: str = "") -> str:
    return f"{col} = CASE WHEN {_DOWNGRADE} THEN t.{col} ELSE {fresh or f'EXCLUDED.{col}'} END"


@dataclass(frozen=True, slots=True)
class SchemaWrite:
    """A snapshot write's outcome: the cached row, or the status that vetoed it.

    ``row is None`` means the user-tier connection guard refused the write —
    ``connection_status`` is what the row said, or None if it was gone.

    ``replaced_digest`` is the digest this write overwrote, read under the same
    lock (None when nothing was there, and on the workspace tier, which has no
    digest). It is what the caller's version bump has to compare against: a
    digest read before the write is stale by the time a second worker's write
    lands on top of it. ``replaced_verdict`` is that same row's ``last_probe``
    verdict, read under the same lock for the same reason: a digest alone
    cannot see a row that kept its tools and stopped answering.
    """

    row: dict[str, Any] | None
    connection_status: str | None = None
    replaced_digest: str | None = None
    replaced_verdict: str | None = None


# ---------------------------------------------------------------------------
# Tier-parameterized implementations
# ---------------------------------------------------------------------------


async def _latest(tier: _SchemaTier, owner_id: str) -> list[dict[str, Any]]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT DISTINCT ON (server_name) {", ".join(tier.columns)}
                FROM {tier.table}
                WHERE {tier.owner_col} = %s
                ORDER BY server_name, discovered_at DESC
                """,
                (owner_id,),
            )
            return [_row_to_dict(tier, r) for r in await cur.fetchall()]


async def _upsert(
    tier: _SchemaTier,
    owner_id: str,
    server_name: str,
    config_hash: str,
    *,
    tools: list[dict[str, Any]] | None,
    status: str,
    error: str,
    schema_digest: str,
    observed_meta: dict[str, Any] | None,
    last_probe: dict[str, Any] | None = None,
    connection_id: str | None = None,
    conn=None,
) -> SchemaWrite:
    columns = tier.columns
    values = [owner_id, server_name, config_hash, Json(tools or []), status, error]
    if tier.has_digest:
        values.append(schema_digest)
    values.append(Json(observed_meta or {}))
    values.append(Json(last_probe or {}))
    updates = [
        _keep_on_downgrade("tools"),
        _keep_on_downgrade("status"),
        "error = EXCLUDED.error",
        *([_keep_on_downgrade("schema_digest")] if tier.has_digest else []),
        _keep_on_downgrade("observed_meta"),
        "last_probe = EXCLUDED.last_probe",
        _keep_on_downgrade("discovered_at", "NOW()"),
    ]
    async with get_db_connection(conn) as db:
        async with db.transaction():
            async with db.cursor(row_factory=dict_row) as cur:
                # A user-tier snapshot is only servable while the credential it
                # was discovered with is still the one execution sends, and that
                # can move on either side during the ~40s a probe spends on the
                # network. An OAuth writer holds a connection_id and re-reads
                # that connection: a disconnect purges both snapshot tiers, so a
                # write landing after it resurrects an "ok" row for a grant the
                # user surrendered, and the caller's version bump fans it out. A
                # header writer holds none and re-reads the row's connection by
                # name (one per name since migration 025): a connect that lands
                # in the same window makes the row relay-bound, so this write
                # would publish the API-key account's tools to a session now
                # sending the OAuth token, and the fingerprint is blind to the
                # difference. FOR SHARE, not a bare SELECT (which under READ
                # COMMITTED reads the pre-commit status and writes anyway), is
                # what makes both commit orders correct: lock first and the
                # other side's status write waits, then its own purge takes
                # these rows with it; commit first and this read blocks, then
                # sees what landed and skips. Covers the catalog
                # delete/recreate variant too: the revoked connection row
                # outlives the catalog entry.
                if connection_id is not None:
                    await cur.execute(
                        "SELECT status FROM user_mcp_oauth_connections "
                        "WHERE connection_id = %s FOR SHARE",
                        (connection_id,),
                    )
                    guard = await cur.fetchone()
                    conn_status = guard["status"] if guard else None
                    if conn_status not in SERVABLE_PARAM:
                        return SchemaWrite(None, conn_status)
                elif tier is USER_TIER:
                    await cur.execute(
                        "SELECT status FROM user_mcp_oauth_connections "
                        "WHERE user_id = %s AND server_name = %s FOR SHARE",
                        (owner_id, server_name),
                    )
                    guard = await cur.fetchone()
                    conn_status = guard["status"] if guard else None
                    # Claimed the way discovery claims it: anything short of
                    # revoked owns the row, whatever repair it needs.
                    if conn_status is not None and (
                        conn_status != ConnectionStatus.REVOKED
                    ):
                        return SchemaWrite(None, conn_status)
                # Only the current config's snapshot is kept, so iterating on a
                # server's config doesn't accumulate dead rows.
                await cur.execute(
                    f"""
                    DELETE FROM {tier.table}
                    WHERE {tier.owner_col} = %s AND server_name = %s
                      AND config_hash <> %s
                    """,
                    (owner_id, server_name, config_hash),
                )
                replaced_digest: str | None = None
                replaced_verdict: str | None = None
                if tier.has_digest:
                    # Taken here, under the lock, because the caller bumps every
                    # workspace when the tool surface moved and only this read
                    # says what moved. Two workers may probe one row at once
                    # (an explicit kick is not throttled, and a request path
                    # discovers outside the scheduler): both read the digest
                    # before either write lands, so the second one compares
                    # against a value its own write already replaced and
                    # declines the bump. FOR UPDATE serializes the pair on the
                    # row the INSERT below is about to overwrite, which settles
                    # every write but the first: a config probed for the first
                    # time has no row here to lock, and the pair is excluded
                    # there by the lock the caller holds on the catalog row.
                    # The verdict rides along because the tool surface is not
                    # the only thing a session re-resolves on: the no-downgrade
                    # rule holds the digest still while ``last_probe`` moves.
                    await cur.execute(
                        f"""
                        SELECT schema_digest, last_probe FROM {tier.table}
                        WHERE {tier.owner_col} = %s AND server_name = %s
                          AND config_hash = %s
                        FOR UPDATE
                        """,
                        (owner_id, server_name, config_hash),
                    )
                    replaced = await cur.fetchone()
                    if replaced is not None:
                        replaced_digest = replaced["schema_digest"] or ""
                        replaced_verdict = (
                            replaced["last_probe"] or {}
                        ).get("verdict")
                await cur.execute(
                    f"""
                    INSERT INTO {tier.table} AS t ({", ".join(columns)})
                    VALUES ({", ".join(["%s"] * len(values))}, NOW())
                    ON CONFLICT ({tier.owner_col}, server_name, config_hash)
                        DO UPDATE SET {", ".join(updates)}
                    RETURNING {", ".join(columns)}
                    """,
                    values,
                )
                return SchemaWrite(
                    _row_to_dict(tier, await cur.fetchone()),
                    replaced_digest=replaced_digest,
                    replaced_verdict=replaced_verdict,
                )


async def _delete(
    tier: _SchemaTier, owner_id: str, server_name: str, *, conn=None
) -> int:
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                f"DELETE FROM {tier.table} "
                f"WHERE {tier.owner_col} = %s AND server_name = %s",
                (owner_id, server_name),
            )
            return cur.rowcount


def _row_to_dict(tier: _SchemaTier, row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        tier.owner_col: str(row[tier.owner_col]),
        "server_name": row["server_name"],
        "config_hash": row["config_hash"],
        "tools": row["tools"] or [],
        "status": row["status"],
        "error": row["error"] or "",
        "observed_meta": row["observed_meta"] or {},
        "last_probe": row["last_probe"] or {},
        "discovered_at": row["discovered_at"].isoformat(),
    }
    if tier.has_digest:
        out["schema_digest"] = row["schema_digest"] or ""
    return out


# ---------------------------------------------------------------------------
# Workspace tier — public API
# ---------------------------------------------------------------------------


async def get_tool_schemas(workspace_id: str) -> list[dict[str, Any]]:
    return await _latest(WORKSPACE_TIER, workspace_id)


async def upsert_tool_schemas(
    workspace_id: str,
    server_name: str,
    config_hash: str,
    *,
    tools: list[dict[str, Any]] | None = None,
    status: str = "pending",
    error: str = "",
    observed_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    write = await _upsert(
        WORKSPACE_TIER, workspace_id, server_name, config_hash,
        tools=tools, status=status, error=error, schema_digest="",
        observed_meta=observed_meta, last_probe=None,
    )
    # In-sandbox discovery has no OAuth connection to check, so this tier passes
    # no connection_id and its write is never skipped.
    return write.row


async def delete_tool_schemas(workspace_id: str, server_name: str) -> int:
    """Drop a server's snapshots at EVERY hash — for invalidation that the
    config fingerprint can't see, e.g. a vault secret discovery depends on
    changing value."""
    return await _delete(WORKSPACE_TIER, workspace_id, server_name)


async def delete_tool_schemas_and_bump(
    workspace_id: str, server_names: list[str]
) -> int:
    """Purge workspace snapshots for the named servers AND bump the config
    version, atomically — a mid-purge failure must never leave schemas
    partially deleted with the version un-bumped (live sessions would then skip
    re-resolution against the half-purged cache)."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM workspace_mcp_tool_schemas "
                    "WHERE workspace_id = %s AND server_name = ANY(%s)",
                    (workspace_id, server_names),
                )
                deleted = cur.rowcount
                await _bump_version(cur, workspace_id)
                return deleted


# ---------------------------------------------------------------------------
# User tier — public API (host-side discovery for OAuth servers)
# ---------------------------------------------------------------------------


async def get_user_tool_schemas(user_id: str) -> list[dict[str, Any]]:
    return await _latest(USER_TIER, user_id)


async def upsert_user_tool_schemas(
    user_id: str,
    server_name: str,
    config_hash: str,
    *,
    last_probe: dict[str, Any],
    tools: list[dict[str, Any]] | None = None,
    status: str = "pending",
    error: str = "",
    schema_digest: str = "",
    observed_meta: dict[str, Any] | None = None,
    connection_id: str | None = None,
    conn=None,
) -> SchemaWrite:
    """Cache a host-side discovery snapshot, keyed by the server's fingerprint.

    ``last_probe`` is the one field a failing write always takes: it is the
    probe's verdict rather than a description of the cached tools, so it is
    exempt from the no-downgrade rule the rest of the row follows. Required,
    not defaulted, because an omitted one stores ``{}`` and blanks the verdict,
    which costs the row every header grant that reads it.

    The returned ``replaced_digest`` and ``replaced_verdict`` are what the row
    held before this write, which is the only sound input to the caller's
    version bump.

    Pass ``connection_id`` whenever the caller holds one: the write is then
    conditional on that connection still being servable when the transaction
    commits, which is the only thing standing between a slow discovery and a
    disconnect it overtakes. A caller holding none is guarded the other way
    round, on nothing having connected this server meanwhile: a header snapshot
    is not servable once the relay is what execution dials.

    Pass ``conn`` to join a caller's transaction, which is how the discovery
    path fences this write on the catalog fingerprint still being current.
    """
    return await _upsert(
        USER_TIER, user_id, server_name, config_hash,
        tools=tools, status=status, error=error, schema_digest=schema_digest,
        observed_meta=observed_meta, last_probe=last_probe,
        connection_id=connection_id, conn=conn,
    )


async def delete_user_tool_schemas(
    user_id: str, server_name: str, *, conn=None
) -> int:
    return await _delete(USER_TIER, user_id, server_name, conn=conn)


async def delete_user_and_workspace_tool_schemas_and_bump(
    user_id: str, server_names: list[str], *, conn=None
) -> int:
    """Purge an inherited server's snapshots in BOTH tiers, then fan the version
    bump out to every workspace of the user, in one transaction.

    Not the user tier alone: a user-level server caches host-side (OAuth)
    discovery in the user tier but in-sandbox discovery under EACH workspace,
    and a vault value change churns neither fingerprint — so a surviving
    same-hash workspace row would still be served and discovery never rerun.

    ``conn`` lets a caller that is already mid-transaction (disconnect) fold the
    purge into it, so the credential revoke and the snapshot purge cannot commit
    apart.
    """
    async with get_db_connection(conn) as db:
        async with db.transaction():
            async with db.cursor() as cur:
                await cur.execute(
                    "DELETE FROM user_mcp_tool_schemas "
                    "WHERE user_id = %s AND server_name = ANY(%s)",
                    (user_id, server_names),
                )
                deleted = cur.rowcount
                # ALL the user's workspaces, not just the running ones — an idle
                # sandbox would otherwise wake onto a stale snapshot. This also
                # purges a workspace-local fork that shadows the inherited name;
                # one needless rediscovery is cheaper than under-purging.
                await cur.execute(
                    """
                    DELETE FROM workspace_mcp_tool_schemas
                    WHERE server_name = ANY(%s) AND workspace_id IN
                        (SELECT workspace_id FROM workspaces WHERE user_id = %s)
                    """,
                    (server_names, user_id),
                )
                deleted += cur.rowcount
                await bump_user_versions(cur, user_id)
                return deleted
