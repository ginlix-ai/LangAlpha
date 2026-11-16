"""Sources the relay read off paid replies, held until a turn takes them.

The relay sees each reply whole, where the sandbox's own trace keeps 64 KiB
of it and can be skipped or faked by the code it runs in. It cannot redact a
body, which needs the turn's secrets, so it holds the sources here under the
call id, and the turn's provenance middleware collects, redacts and stores
them like any other source's.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from psycopg.rows import dict_row

from ptc_agent.agent.provenance.trace import RelayBatch, RelayedSource, relayed_call
from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import SafeJson, strip_pg_nul_str

# Held sources nothing collects (a call whose command never reports back) are
# dropped after this long.
_RETENTION = "interval '7 days'"


async def stage(
    *,
    user_id: str,
    call_id: str | None,
    job_ref: str | None,
    server: str,
    tool: str,
    sources: Sequence[RelayedSource],
) -> int:
    """Hold the sources one reply from ``server``'s ``tool`` carried; returns how many
    were new. A crawl's page already held for its job is skipped, since each
    status poll lists every page so far: the held identifiers are read first,
    so a poll sends only its new pages' bodies rather than every page's for
    the unique index to throw away. The new pages share what the job's total
    so far (the batch's costs) leaves over the held pages' costs, so a job's
    sources add up to its total whichever polls held them. Two polls of one
    job stage in turn, under a lock on it, or both would read none held."""
    if not sources:
        return 0
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor() as cur:
            if job_ref is not None:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"relayed_sources:{user_id}:{job_ref}",),
                )
                await cur.execute(
                    """
                    SELECT source->>'identifier', (source->>'cost_credits')::float8
                    FROM relayed_sources
                    WHERE user_id = %s AND job_ref = %s AND job_ref IS NOT NULL
                    """,
                    (user_id, job_ref),
                )
                rows = await cur.fetchall()
                held = {row[0] for row in rows}
                fresh: list[RelayedSource] = []
                for source in sources:
                    if source.identifier not in held:
                        held.add(source.identifier)
                        fresh.append(source)
                sources = _share_rest(fresh, sources, sum(row[1] or 0.0 for row in rows))
            staged = 0
            if sources:
                # Still ON CONFLICT: a poll racing this one may hold a page
                # between the read and the insert.
                await cur.executemany(
                    """
                    INSERT INTO relayed_sources (user_id, call_id, job_ref, source, body)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        (
                            user_id,
                            call_id,
                            job_ref,
                            # A vendor's text can carry NUL, which neither
                            # column takes; one would lose the whole reply's.
                            SafeJson(_stored(source, server, tool)),
                            strip_pg_nul_str(source.body),
                        )
                        for source in sources
                    ],
                )
                staged = cur.rowcount
            return staged


async def purge_expired(*, batch_size: int = 5000, max_batches: int = 10) -> int:
    """Drop held sources past retention, a batch per statement so no delete
    holds a long lock; returns how many. Every worker runs it: SKIP LOCKED
    has two at once split the rows, and passes over a row a collect holds."""
    purged = 0
    async with get_db_connection() as conn:
        for _ in range(max_batches):
            cur = await conn.execute(
                f"""
                DELETE FROM relayed_sources WHERE id IN (
                    SELECT id FROM relayed_sources
                    WHERE created_at < NOW() - {_RETENTION}
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                """,
                (batch_size,),
            )
            purged += cur.rowcount
            if cur.rowcount < batch_size:
                break
    return purged


async def collect(user_id: str, call_id: str) -> RelayBatch:
    """Take the sources held for ``user_id`` under ``call_id``, once, in the
    order the relay read them and each with its row id, with every tool the
    relay held sources for under it, as ``<server>:<tool>``. By user too: the
    sandbox picks its call ids.

    The body is dropped from the row as it is taken, but the calls outlast
    the taking: a command's end can be read twice (its notice, a ThreadStatus
    look), and the second read must still drop the sandbox trace entries
    those sources stood in for.
    """
    # One transaction: a failed read of the calls must not leave the sources
    # taken, their bodies dropped, with nothing returned to hold them.
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH taken AS (
                    SELECT id, source, body FROM relayed_sources
                    WHERE call_id = %s AND user_id = %s AND collected_at IS NULL
                    ORDER BY id
                    FOR UPDATE
                )
                UPDATE relayed_sources r
                SET collected_at = NOW(), body = NULL
                FROM taken
                WHERE r.id = taken.id
                RETURNING taken.id, taken.source, taken.body
                """,
                (call_id, user_id),
            )
            rows = sorted(await cur.fetchall(), key=lambda r: r["id"])
            # Collected rows only: one staged after the taking above would
            # name a tool whose source this batch does not carry.
            await cur.execute(
                "SELECT DISTINCT source->>'server' AS server, source->>'tool' AS tool"
                " FROM relayed_sources WHERE call_id = %s AND user_id = %s"
                " AND collected_at IS NOT NULL"
                " AND source ? 'server' AND source ? 'tool'",
                (call_id, user_id),
            )
            calls = frozenset(
                relayed_call(row["server"], row["tool"]) for row in await cur.fetchall()
            )
    return RelayBatch(
        sources=tuple(
            _relayed(row["source"], row["body"], row["id"]) for row in rows
        ),
        calls=calls,
    )


async def settling(user_id: str, call_ids: Sequence[str], within_s: float) -> set[str]:
    """Which of ``user_id``'s ``call_ids`` have a paid call held for less than
    ``within_s``, whose sources may still be held here as it settles. An
    older hold outlived any reply that could settle it."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT DISTINCT call_id FROM metered_vendor_calls
                WHERE call_id = ANY(%s) AND user_id = %s AND status = 'held'
                  AND created_at > NOW() - make_interval(secs => %s)
                """,
                (list(call_ids), user_id, within_s),
            )
            return {row[0] for row in await cur.fetchall()}


def _share_rest(
    fresh: list[RelayedSource], batch: Sequence[RelayedSource], held_cost: float
) -> list[RelayedSource]:
    """``fresh`` each costing an even share of what ``batch``'s costs leave
    over ``held_cost``; as they are when ``batch`` is unpriced."""
    if not fresh or any(s.cost_credits is None for s in batch):
        return fresh
    rest = max(0.0, sum(s.cost_credits or 0.0 for s in batch) - held_cost)
    return [replace(s, cost_credits=rest / len(fresh)) for s in fresh]


def _stored(source: RelayedSource, server: str, tool: str) -> dict[str, Any]:
    """The ``source`` column: everything but the body, which has its own, and
    with the server and tool, which :func:`collect` reads back for the call."""
    return {
        "source_type": source.source_type,
        "identifier": source.identifier,
        "title": source.title,
        "provider": source.provider,
        "site": source.site,
        "args": source.args,
        "server": server,
        "tool": tool,
        "cost_credits": source.cost_credits,
    }


def _relayed(source: dict[str, Any], body: str | None, held_id: int) -> RelayedSource:
    return RelayedSource(
        source_type=source["source_type"],
        identifier=source["identifier"],
        title=source.get("title"),
        provider=source.get("provider"),
        site=source.get("site"),
        args=source.get("args"),
        cost_credits=source.get("cost_credits"),
        body=body,
        held_id=held_id,
    )
