"""SQL for model-written MCP server descriptions and their job queue.

A scope has one current description per prompt version, with the digest of the
tools it was written from. Every read takes the prompt version it wants, so a
row written under another one is invisible rather than wrong.

Every write to a scope's queue rows runs under that scope's advisory lock, and
a worker's writes are fenced by its claim (``job_id`` and ``attempts``), so a
worker whose lease ran out can neither finish nor reset a job another worker
has since claimed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import advisory_key

# A job no worker finished within this long is claimable again: its worker died
# or lost its database session mid-run. A worker stops its own run well before.
JOB_LEASE_S = 600
MAX_JOB_ATTEMPTS = 5
# A surface the model failed on is not queued again for this long, however
# often it is rediscovered; after that, a rediscovery tries it afresh.
GIVE_UP_FOR_S = 24 * 3600


def _scope_lock(scope_key: str) -> int:
    return advisory_key("MCP_DESCRIBE", scope_key)


_BUMP = "UPDATE workspaces SET mcp_config_version = mcp_config_version + 1 WHERE user_id = ANY(%s)"


def _fenced(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"{prefix}job_id = %s AND {prefix}attempts = %s AND {prefix}status = 'running'"


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------


async def enqueue_job(
    *,
    scope_key: str,
    server_digest: str,
    prompt_version: str,
    user_id: str | None,
    server_name: str,
    tools: list[dict[str, Any]],
    settled: Mapping[str, Any] | None = None,
    conn=None,
) -> tuple[bool, bool]:
    """Make the scope's queue follow the tool surface discovery saw last.
    Returns whether a job was queued, and whether the current description was
    dropped, which the caller owes its readers a bump for.

    Each prompt version keeps its own queue, so a build on another version
    never supersedes this one's work. A waiting job for any other surface is
    dropped, since the latest discovery supersedes it. A job already waiting or running for this surface is left
    as it is, so rediscovering the same tools never resets a backoff, and so
    is one given up on within ``GIVE_UP_FOR_S``. When the current description
    already covers this surface, nothing is queued unless a job for another
    surface is running, which would otherwise land last. When it covers
    another surface, it is dropped: agents read the shipped text, or none,
    rather than a description of tools the server no longer has.

    ``settled`` is the ``settled_snapshots`` row the tools were read from, for
    a caller that read them before taking the lock. Nothing is queued unless
    that snapshot still stands: a purge drops the scope's jobs under this lock
    before it lets the snapshot go, so a list read ahead of it is never queued
    after it.
    """
    async with get_db_connection(conn) as db, db.transaction(), db.cursor() as cur:
        await cur.execute("SELECT pg_advisory_xact_lock(%s)", (_scope_lock(scope_key),))
        if settled is not None:
            table, owner = (
                ("user_mcp_tool_schemas", "user_id")
                if settled["workspace_id"] is None
                else ("workspace_mcp_tool_schemas", "workspace_id")
            )
            await cur.execute(
                f"SELECT 1 FROM {table} WHERE {owner} = %s AND server_name = %s "
                "AND config_hash = %s AND discovered_at = %s AND status = 'ok'",
                (
                    settled["workspace_id"] or settled["user_id"],
                    settled["server_name"],
                    settled["config_hash"],
                    settled["discovered_at"],
                ),
            )
            if await cur.fetchone() is None:
                return False, False
        await cur.execute(
            """
            SELECT
                (SELECT server_digest FROM mcp_server_profiles
                  WHERE scope_key = %s AND prompt_version = %s),
                EXISTS (SELECT 1 FROM mcp_description_jobs
                         WHERE scope_key = %s AND prompt_version = %s
                           AND status <> 'failed')
            """,
            (scope_key, prompt_version, scope_key, prompt_version),
        )
        current, busy = await cur.fetchone()
        if current == server_digest and not busy:
            return False, False
        dropped = current is not None and current != server_digest
        if dropped:
            await cur.execute(
                "DELETE FROM mcp_server_profiles WHERE scope_key = %s AND prompt_version = %s",
                (scope_key, prompt_version),
            )
        await cur.execute(
            """
            DELETE FROM mcp_description_jobs
            WHERE scope_key = %s AND prompt_version = %s AND status = 'pending'
              AND server_digest <> %s
            """,
            (scope_key, prompt_version, server_digest),
        )
        await cur.execute(
            """
            SELECT status FROM mcp_description_jobs
            WHERE scope_key = %s AND (server_digest, prompt_version) = (%s, %s)
              AND (status <> 'failed' OR updated_at > NOW() - make_interval(secs => %s))
            """,
            (scope_key, server_digest, prompt_version, GIVE_UP_FOR_S),
        )
        if await cur.fetchone():
            return False, dropped
        if current == server_digest:
            await cur.execute(
                "SELECT 1 FROM mcp_description_jobs "
                "WHERE scope_key = %s AND prompt_version = %s AND status = 'running'",
                (scope_key, prompt_version),
            )
            if await cur.fetchone() is None:
                return False, False
        await cur.execute(
            "DELETE FROM mcp_description_jobs "
            "WHERE scope_key = %s AND prompt_version = %s AND status = 'failed'",
            (scope_key, prompt_version),
        )
        await cur.execute(
            """
            INSERT INTO mcp_description_jobs
                (scope_key, server_digest, prompt_version, user_id, server_name, tools)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (scope_key, server_digest, prompt_version, user_id, server_name, Json(tools)),
        )
    return True, dropped


async def drop_descriptions(scope_keys: Sequence[str], *, conn=None) -> None:
    """Drop these scopes' current descriptions and their jobs, each under its
    scope's lock. A running job goes too, so its claim's fence fails and it
    lands nothing written from the tools before."""
    if not scope_keys:
        return
    async with get_db_connection(conn) as db, db.transaction(), db.cursor() as cur:
        for key in sorted(set(scope_keys)):
            await cur.execute("SELECT pg_advisory_xact_lock(%s)", (_scope_lock(key),))
        await cur.execute(
            "DELETE FROM mcp_server_profiles WHERE scope_key = ANY(%s)", (list(scope_keys),)
        )
        await cur.execute(
            "DELETE FROM mcp_description_jobs WHERE scope_key = ANY(%s)", (list(scope_keys),)
        )


async def claim_job(prompt_version: str, *, lease_s: int = JOB_LEASE_S) -> dict[str, Any] | None:
    """Take one due job queued for this build's prompt version, or None.

    Each candidate is claimed under its scope's advisory lock, inside which no
    other live claim on the scope may exist, so two workers never run one scope
    at once even when a stale claim and a newer pending row coexist. A stale
    claim is superseded by a newer surface waiting behind it, so that one runs
    and the stale one is dropped. A claim whose run ended its worker on every
    attempt never reached ``fail_job``, so it is given up on here.
    """
    live_sibling = """
        EXISTS (
            SELECT 1 FROM mcp_description_jobs o
            WHERE o.scope_key = j.scope_key AND o.job_id <> j.job_id
              AND o.status = 'running' AND o.lease_until >= NOW()
        )
    """
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE mcp_description_jobs
                SET status = 'failed', lease_until = NULL, updated_at = NOW(),
                    error = 'the lease ran out on every attempt'
                WHERE prompt_version = %s AND status = 'running'
                  AND lease_until < NOW() AND attempts >= %s
                """,
                (prompt_version, MAX_JOB_ATTEMPTS),
            )
            await cur.execute(
                f"""
                SELECT job_id, scope_key FROM mcp_description_jobs j
                WHERE ((status = 'pending' AND run_after <= NOW())
                    OR (status = 'running' AND lease_until < NOW()))
                  AND prompt_version = %s AND NOT {live_sibling}
                  AND NOT (status = 'running' AND EXISTS (
                      SELECT 1 FROM mcp_description_jobs p
                      WHERE p.scope_key = j.scope_key AND p.prompt_version = j.prompt_version
                        AND p.status = 'pending'
                  ))
                ORDER BY run_after
                LIMIT 10
                FOR UPDATE SKIP LOCKED
                """,
                (prompt_version,),
            )
            for candidate in await cur.fetchall():
                await cur.execute(
                    "SELECT pg_try_advisory_xact_lock(%s) AS got",
                    (_scope_lock(candidate["scope_key"]),),
                )
                if not (await cur.fetchone())["got"]:
                    continue
                await cur.execute(
                    f"""
                    UPDATE mcp_description_jobs j
                    SET status = 'running', attempts = attempts + 1,
                        lease_until = NOW() + make_interval(secs => %s),
                        updated_at = NOW()
                    WHERE j.job_id = %s AND NOT {live_sibling}
                    RETURNING *
                    """,
                    (lease_s, candidate["job_id"]),
                )
                if row := await cur.fetchone():
                    await cur.execute(
                        """
                        DELETE FROM mcp_description_jobs
                        WHERE scope_key = %s AND prompt_version = %s AND job_id <> %s
                          AND status = 'running' AND lease_until < NOW()
                        """,
                        (row["scope_key"], prompt_version, row["job_id"]),
                    )
                    return row
    return None


async def finish_job(job_id: int, attempts: int) -> None:
    """Drop a claimed job that had nothing to write."""
    async with get_db_connection() as conn:
        await conn.execute(
            f"DELETE FROM mcp_description_jobs WHERE {_fenced()}", (job_id, attempts)
        )


async def fail_job(
    job_id: int, attempts: int, scope_key: str, error: str, *, retry_after_s: int
) -> None:
    """Back a claimed job off, or give up on it after ``MAX_JOB_ATTEMPTS``.

    A newer pending row for the same scope and prompt version supersedes this
    one outright: it carries the newer tool list, and the unique pending index
    has room for one.
    """
    async with get_db_connection() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(%s)", (_scope_lock(scope_key),))
        await conn.execute(
            f"""
            DELETE FROM mcp_description_jobs j
            WHERE {_fenced("j")} AND EXISTS (
                SELECT 1 FROM mcp_description_jobs o
                WHERE o.scope_key = j.scope_key AND o.prompt_version = j.prompt_version
                  AND o.status = 'pending'
            )
            """,
            (job_id, attempts),
        )
        await conn.execute(
            f"""
            UPDATE mcp_description_jobs
            SET status = CASE WHEN attempts >= %s THEN 'failed' ELSE 'pending' END,
                run_after = NOW() + make_interval(secs => %s),
                lease_until = NULL, error = %s, updated_at = NOW()
            WHERE {_fenced()}
            """,
            (MAX_JOB_ATTEMPTS, retry_after_s, error[:2000], job_id, attempts),
        )


async def complete_job(
    job_id: int,
    attempts: int,
    *,
    scope_key: str,
    prompt_version: str,
    server_digest: str,
    model: str | None,
    description: str,
    owners: Iterable[str],
) -> bool:
    """Land a description: end the claim, make it the scope's current one, and
    bump every owner's workspaces, all in one transaction under the scope's
    lock. Nothing lands when the claim was lost, or when discovery has since
    queued another surface, which this description would only cover up.
    Returns whether it landed."""
    async with get_db_connection() as conn, conn.transaction(), conn.cursor() as cur:
        await cur.execute("SELECT pg_advisory_xact_lock(%s)", (_scope_lock(scope_key),))
        await cur.execute(
            f"DELETE FROM mcp_description_jobs WHERE {_fenced()} RETURNING 1",
            (job_id, attempts),
        )
        if await cur.fetchone() is None:
            return False
        await cur.execute(
            "SELECT 1 FROM mcp_description_jobs "
            "WHERE scope_key = %s AND prompt_version = %s AND status = 'pending'",
            (scope_key, prompt_version),
        )
        if await cur.fetchone() is not None:
            return False
        await cur.execute(
            """
            INSERT INTO mcp_server_profiles
                (scope_key, prompt_version, server_digest, model, description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (scope_key, prompt_version) DO UPDATE
            SET server_digest = EXCLUDED.server_digest, model = EXCLUDED.model,
                description = EXCLUDED.description, written_at = NOW()
            """,
            (scope_key, prompt_version, server_digest, model, description),
        )
        await cur.execute(_BUMP, (sorted(owners),))
    return True


async def bump_workspaces(user_ids: Iterable[str], *, conn=None) -> None:
    async with get_db_connection(conn) as db:
        await db.execute(_BUMP, (sorted(user_ids),))


# ---------------------------------------------------------------------------
# Descriptions and the rows that read them
# ---------------------------------------------------------------------------


async def current_digest(scope_key: str, *, prompt_version: str, conn=None) -> str | None:
    """The digest of the tools the scope's current description was written from."""
    async with get_db_connection(conn) as db, db.cursor() as cur:
        await cur.execute(
            "SELECT server_digest FROM mcp_server_profiles "
            "WHERE scope_key = %s AND prompt_version = %s",
            (scope_key, prompt_version),
        )
        row = await cur.fetchone()
        return row[0] if row else None


async def current_descriptions(
    scope_keys: Sequence[str], *, prompt_version: str
) -> dict[str, str]:
    if not scope_keys:
        return {}
    async with get_db_connection() as db, db.cursor() as cur:
        await cur.execute(
            "SELECT scope_key, description FROM mcp_server_profiles "
            "WHERE scope_key = ANY(%s) AND prompt_version = %s",
            (list(scope_keys), prompt_version),
        )
        return dict(await cur.fetchall())


# Only rows an agent is served: switched on, and not withheld by a plugin the
# user switched off, as ``list_enabled_user_servers`` reads them.
_ROW_WITH_CONNECTION = """
    SELECT s.user_id, s.name, s.transport, s.url, s.command, s.args, s.env, s.headers,
           s.discovery_uses_secrets, s.description, c.server_url, c.granted_capabilities
    FROM user_mcp_servers s
    LEFT JOIN user_plugins p ON p.user_plugin_id = s.plugin_id
    LEFT JOIN user_mcp_oauth_connections c
      ON c.user_id = s.user_id AND c.server_name = s.name AND c.status <> 'revoked'
    WHERE s.enabled = TRUE AND (s.plugin_id IS NULL OR p.enabled = TRUE)
"""


def _row(r: dict[str, Any]) -> dict[str, Any]:
    """A catalog row with its live connection, the consented URL and grant, or
    None: a revoked one is history and binds nothing, as ``resolve_mcp_config``
    reads it."""
    connection = None
    if r["server_url"] is not None:
        connection = {"server_url": r["server_url"], "granted_capabilities": r["granted_capabilities"]}
    return {
        "user_id": r["user_id"],
        "name": r["name"],
        "transport": r["transport"],
        "url": r["url"],
        "command": r["command"],
        "args": r["args"] or [],
        "env": r["env"] or {},
        "headers": r["headers"] or {},
        "discovery_uses_secrets": r["discovery_uses_secrets"],
        "description": r["description"],
        "connection": connection,
    }


async def row_for(user_id: str, server_name: str, *, conn=None) -> dict[str, Any] | None:
    async with get_db_connection(conn) as db, db.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _ROW_WITH_CONNECTION + " AND s.user_id = %s AND s.name = %s",
            (user_id, server_name),
        )
        found = await cur.fetchone()
        return _row(found) if found else None


@asynccontextmanager
async def seed_pass() -> AsyncIterator[Any]:
    """One worker's seeding pass at a time, every statement of it on the one
    connection this yields, so the pass never waits on a second checkout from
    the pool it holds. Yields None while another worker's pass runs. A session
    lock, released here or when the connection drops with a dead worker, and
    no transaction, so each server's queueing commits on its own."""
    async with get_db_connection() as conn:
        key = advisory_key("MCP_DESCRIBE_SEED")
        if not (await (await conn.execute("SELECT pg_try_advisory_lock(%s)", (key,))).fetchone())[0]:
            yield None
            return
        try:
            yield conn
        finally:
            await conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


async def settled_snapshots(
    user_id: str | None = None, server_name: str | None = None, *, conn=None
) -> list[dict[str, Any]]:
    """The latest settled snapshot of every discovered server, or of one,
    without its tools: the host-side one, which the server fetched from the
    vendor, and each workspace's sandbox one under the workspace's owner."""
    async with get_db_connection(conn) as db, db.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT user_id, workspace_id, server_name, config_hash, discovered_at, from_vendor
            FROM (
                SELECT DISTINCT ON (user_id, server_name)
                       user_id::text, NULL::uuid AS workspace_id, server_name, config_hash,
                       status, discovered_at, TRUE AS from_vendor
                FROM user_mcp_tool_schemas
                WHERE (%(u)s::text IS NULL OR user_id = %(u)s)
                  AND (%(n)s::text IS NULL OR server_name = %(n)s)
                ORDER BY user_id, server_name, discovered_at DESC
            ) host
            WHERE status = 'ok'
            UNION ALL
            SELECT user_id, workspace_id, server_name, config_hash, discovered_at, from_vendor
            FROM (
                SELECT DISTINCT ON (s.workspace_id, s.server_name)
                       w.user_id::text, s.workspace_id, s.server_name, s.config_hash,
                       s.status, s.discovered_at, FALSE AS from_vendor
                FROM workspace_mcp_tool_schemas s
                JOIN workspaces w ON w.workspace_id = s.workspace_id
                WHERE (%(u)s::text IS NULL OR w.user_id = %(u)s)
                  AND (%(n)s::text IS NULL OR s.server_name = %(n)s)
                ORDER BY s.workspace_id, s.server_name, s.discovered_at DESC
            ) sandbox
            WHERE status = 'ok'
            """,
            {"u": user_id, "n": server_name},
        )
        return await cur.fetchall()


async def snapshot_tools(snapshot: dict[str, Any], *, conn=None) -> list[dict[str, Any]]:
    """The tools of one snapshot ``settled_snapshots`` returned."""
    if snapshot["workspace_id"] is None:
        sql = (
            "SELECT tools FROM user_mcp_tool_schemas WHERE user_id = %s "
            "AND server_name = %s AND config_hash = %s ORDER BY discovered_at DESC LIMIT 1"
        )
        owner = snapshot["user_id"]
    else:
        sql = (
            "SELECT tools FROM workspace_mcp_tool_schemas WHERE workspace_id = %s "
            "AND server_name = %s AND config_hash = %s ORDER BY discovered_at DESC LIMIT 1"
        )
        owner = snapshot["workspace_id"]
    async with get_db_connection(conn) as db, db.cursor() as cur:
        await cur.execute(sql, (owner, snapshot["server_name"], snapshot["config_hash"]))
        row = await cur.fetchone()
        return list(row[0] or []) if row else []


async def rows_at_host(host: str, *, conn=None) -> list[dict[str, Any]]:
    """Every catalog row whose own or consented address is on this host."""
    async with get_db_connection(conn) as db, db.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _ROW_WITH_CONNECTION + " AND (s.url ILIKE %s OR c.server_url ILIKE %s)",
            (f"%://{host}%", f"%://{host}%"),
        )
        return [_row(r) for r in await cur.fetchall()]
