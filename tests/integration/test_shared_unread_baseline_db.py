"""A shared thread's history is not unread for someone who joined after it,
real PostgreSQL.

One turn ends an hour ago. A member listed before it has it to read; one
listed after it does not; someone reaching the workspace through everyone has
it to read only when both the opening to everyone and their account came
before it, and whichever way reached someone first is the one that counts. A later turn is unread for all of them, and a read row, once there,
is what counts.
"""

from __future__ import annotations

import uuid

import pytest

from tests.integration.test_shared_thread_reads_db import A, _seed

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

EARLY = "ub-early"  # listed before the turn
LATE = "ub-late"  # listed after it
OLD = "ub-old"  # through everyone, account older than the turn
NEW = "ub-new"  # through everyone, account made after the turn


@pytest.fixture(autouse=True)
def _tuple_rows(patched_get_db_connection, monkeypatch):
    from tests.integration.test_steer_outbox_db import _app_rows

    _app_rows(monkeypatch)


async def _ended_turn(conn, thread: str, ago: str) -> None:
    async with conn.cursor() as cur:
        run = uuid.uuid4()
        await cur.execute(
            "INSERT INTO conversation_responses"
            " (conversation_response_id, conversation_thread_id, turn_index, status, user_id, created_at)"
            " VALUES (%s, %s, (SELECT count(*) FROM conversation_responses WHERE conversation_thread_id = %s),"
            f"  'in_progress', %s, NOW() - INTERVAL '{ago}')",
            (run, thread, thread, A),
        )
        await cur.execute(
            "UPDATE conversation_responses SET status = 'completed', usage_settled_at = created_at"
            " WHERE conversation_response_id = %s",
            (run,),
        )


async def _setup(conn) -> tuple[str, str]:
    ws, thread = await _seed(conn)
    async with conn.cursor() as cur:
        for uid, made in ((EARLY, "3 hours"), (LATE, "3 hours"), (OLD, "3 hours"), (NEW, "1 minute")):
            await cur.execute(
                f"INSERT INTO users (user_id, created_at) VALUES (%s, NOW() - INTERVAL '{made}')", (uid,)
            )
        for uid, added in ((EARLY, "2 hours"), (LATE, "1 minute")):
            await cur.execute(
                "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by, added_at)"
                f" VALUES (%s, %s, 'member', %s, NOW() - INTERVAL '{added}')",
                (ws, uid, A),
            )
    await _ended_turn(conn, thread, "1 hour")
    return ws, thread


async def _everyone_since(conn, ws: str, ago: str) -> None:
    await conn.execute(
        f"UPDATE shared_workspaces SET everyone = TRUE, everyone_since = NOW() - INTERVAL '{ago}'"
        " WHERE workspace_id = %s",
        (ws,),
    )


async def _unread(member: str, ws: str, thread: str) -> bool:
    from src.server.database.conversation.viewer_threads import (
        shared_lifecycle_rows,
        threads_with_shared,
        workspace_threads_for_member,
    )

    rows, _ = await shared_lifecycle_rows(member, [ws])
    snapshot = any(
        str(r["conversation_thread_id"]) == thread and r["branch"] == "unseen" for r in rows
    )
    recents, _ = await threads_with_shared(member, [ws])
    listed, _ = await workspace_threads_for_member(ws, member)
    def behind(rows) -> bool:
        row = next(r for r in rows if str(r["conversation_thread_id"]) == thread)
        return row["latest_run_seq"] > row["last_seen_run_seq"]

    views = {snapshot, behind(recents), behind(listed)}
    assert len(views) == 1, f"the snapshot, recents and the workspace list disagree for {member}"
    return views.pop()


async def test_listed_members_have_only_what_ended_after_they_joined_to_read(
    patched_get_db_connection, cleanup_tables
):
    async with patched_get_db_connection() as conn:
        ws, thread = await _setup(conn)

    assert await _unread(EARLY, ws, thread) is True
    assert await _unread(LATE, ws, thread) is False

    async with patched_get_db_connection() as conn:
        await _ended_turn(conn, thread, "10 seconds")
    assert await _unread(LATE, ws, thread) is True


async def test_everyone_counts_from_the_later_of_the_opening_and_the_account(
    patched_get_db_connection, cleanup_tables
):
    async with patched_get_db_connection() as conn:
        ws, thread = await _setup(conn)
        await _everyone_since(conn, ws, "30 minutes")
    assert await _unread(OLD, ws, thread) is False

    async with patched_get_db_connection() as conn:
        await _everyone_since(conn, ws, "2 hours")
    assert await _unread(OLD, ws, thread) is True
    assert await _unread(NEW, ws, thread) is False
    # Listed later, but everyone reached them first: the turn is theirs.
    assert await _unread(LATE, ws, thread) is True


async def test_a_read_row_wins_over_the_baseline(patched_get_db_connection, cleanup_tables):
    from src.server.database.conversation.shared_thread_reads import stamp_member_seen

    async with patched_get_db_connection() as conn:
        ws, thread = await _setup(conn)
        run = await (
            await conn.execute(
                "SELECT conversation_response_id FROM conversation_responses WHERE conversation_thread_id = %s",
                (thread,),
            )
        ).fetchone()
    await stamp_member_seen(thread, EARLY, str(run["conversation_response_id"]))
    assert await _unread(EARLY, ws, thread) is False


async def test_a_first_pin_keeps_the_baseline(patched_get_db_connection, cleanup_tables):
    from src.server.database.conversation.shared_thread_reads import set_member_pin
    from src.server.database.conversation.viewer_threads import workspace_threads_for_member

    async with patched_get_db_connection() as conn:
        ws, thread = await _setup(conn)
    # LATE has no read row and joined after the turn; pinning writes one.
    await set_member_pin(thread, LATE, True)
    assert await _unread(LATE, ws, thread) is False
    listed, _ = await workspace_threads_for_member(ws, LATE)
    assert next(r for r in listed if str(r["conversation_thread_id"]) == thread)["is_pinned"]
    # A row that is there is what counts: a later turn is unread.
    async with patched_get_db_connection() as conn:
        await _ended_turn(conn, thread, "10 seconds")
    assert await _unread(LATE, ws, thread) is True
