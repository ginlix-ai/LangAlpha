"""What the Drive's database suites share: a person's space, a save of a
short text by a person or the agent, a mark, a back-dated undo window, the
versions a file keeps and the space's counts checked against its rows."""

from __future__ import annotations

import asyncio

import pytest_asyncio

from src.server.database.drive import memos, reads, writes
from src.server.database.drive.records import Actor, Limits
from src.server.database.drive.writes import ContentRef

MIB = 1024 * 1024
LIMITS = Limits(
    max_space_bytes=None,
    max_entries=100_000,
    max_trash_bytes=None,
    max_file_bytes=1 << 40,
    max_request_bytes=50 * MIB,
)


@pytest_asyncio.fixture(loop_scope="session")
async def space(seed_user):
    return await reads.personal_space(seed_user["user_id"])


def person_of(space, sitting: str | None = None) -> Actor:
    return Actor(kind="person", user_id=space.user_id, via="app", sitting=sitting)


def ref_of(text: str) -> tuple[ContentRef, bytes]:
    data = text.encode()
    return ContentRef(f"sha-{text}", len(data), False, "application/octet-stream"), data


async def save_text(space, path: str, text: str, *, sitting: str | None = None, limits=LIMITS):
    ref, data = ref_of(text)
    async with writes.locked(space.space_id, limits) as tx:
        return await tx.save(path, ref, person_of(space, sitting), data=data)


async def mark(space, path: str, **change) -> bool:
    """Change the file's mark as a person (``memo: true`` unless given);
    whether anything changed."""
    async with writes.locked(space.space_id, LIMITS) as tx:
        entry = await tx.live(path)
        change = memos.checked({"memo": True} | change)
        return await memos.set_mark(tx, entry, person_of(space), change, note=None) is not None


async def make_due(db_conn, path: str, space) -> None:
    """Back-date a file's undo window, as if its days had passed."""
    async with db_conn.cursor() as cur:
        await cur.execute(
            "UPDATE drive_entries SET undo_until = now() - interval '1 second'"
            " WHERE space_id = %s AND path = %s",
            (space.space_id, path),
        )


async def kept_seqs(space, path: str) -> list[int]:
    entry = await reads.live_entry(space.space_id, path=path)
    return [v.seq for v in (await reads.versions(entry.entry_id, LIMITS.history))[0]]


async def counts_of(db_conn, space_id: str) -> tuple[dict, dict]:
    """The space's kept counts, and the same counted from its rows."""
    async with db_conn.cursor() as cur:
        await cur.execute(
            "SELECT held_bytes, released_bytes, gone_bytes, entry_count"
            " FROM drive_spaces WHERE space_id = %s",
            (space_id,),
        )
        kept = dict(await cur.fetchone())
        await cur.execute(
            """
            SELECT
              (SELECT COALESCE(sum(byte_len), 0) FROM drive_contents WHERE space_id = %(s)s) AS held_bytes,
              (SELECT COALESCE(sum(byte_len), 0) FROM drive_contents
                WHERE space_id = %(s)s AND released_at IS NOT NULL) AS released_bytes,
              (SELECT COALESCE(sum(c.byte_len), 0) FROM drive_contents c
                WHERE c.space_id = %(s)s AND c.released_at IS NULL
                  AND EXISTS (SELECT 1 FROM drive_versions v
                               WHERE v.space_id = c.space_id AND v.sha256 = c.sha256)
                  AND NOT EXISTS (SELECT 1 FROM drive_versions v
                                   JOIN drive_entries e ON e.entry_id = v.entry_id
                                  WHERE v.space_id = c.space_id AND v.sha256 = c.sha256
                                    AND e.deleted_at IS NULL)) AS gone_bytes,
              (SELECT count(*) FROM drive_entries WHERE space_id = %(s)s) AS entry_count
            """,
            {"s": space_id},
        )
        counted = dict(await cur.fetchone())
        await cur.execute(
            """
            SELECT count(*) AS wrong FROM drive_contents c
             WHERE c.space_id = %(s)s AND (
               c.live_refs <> (SELECT count(*) FROM drive_versions v JOIN drive_entries e
                                 ON e.entry_id = v.entry_id AND e.deleted_at IS NULL
                                WHERE v.space_id = c.space_id AND v.sha256 = c.sha256)
               OR c.gone_refs <> (SELECT count(*) FROM drive_versions v JOIN drive_entries e
                                    ON e.entry_id = v.entry_id AND e.deleted_at IS NOT NULL
                                   WHERE v.space_id = c.space_id AND v.sha256 = c.sha256))
            """,
            {"s": space_id},
        )
        counted["wrong_refs"] = (await cur.fetchone())["wrong"]
    kept["wrong_refs"] = 0
    return kept, counted


def agent_of(
    space, via: str = "cli", sitting: str | None = None, thread: str | None = None
) -> Actor:
    """The agent, in ``thread``'s chat; with none, a command no call filed
    on computer ``pc``."""
    return Actor(
        kind="agent", user_id=space.user_id, via=via, agent="main", sitting=sitting,
        thread_id=thread, computer_id="pc",
    )


async def agent_save(
    space, path: str, text: str, *, via="cli", sitting=None, note=None, thread=None
):
    ref, data = ref_of(text)
    async with writes.locked(space.space_id, LIMITS) as tx:
        actor = agent_of(space, via, sitting, thread)
        return await tx.save(path, ref, actor, data=data, note=note)


async def waiting_on_a_lock(pool, holder, within_s: float = 10.0) -> None:
    """Until another session waits on a lock ``holder``'s open transaction
    holds: a session waiting on anything else proves nothing of the order
    under test."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within_s
    while loop.time() < deadline:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT count(*) AS n FROM pg_stat_activity"
                " WHERE %s = ANY(pg_blocking_pids(pid))",
                (holder.info.backend_pid,),
            )
            if (await cur.fetchone())["n"]:
                return
        await asyncio.sleep(0.02)
    raise AssertionError("no session came to wait on the lock")
