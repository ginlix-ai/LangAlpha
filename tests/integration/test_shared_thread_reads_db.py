"""Each member's read state on a shared workspace's thread, real PostgreSQL.

Two members, one shared thread. A member reading or pinning it moves only
their own row: the other member still sees the turn unread and the thread
unpinned, and the thread's own row, which a personal thread reads, never
moves. A turn paused for its sender's answer reads as running to the other
member, who cannot answer it.
"""

from __future__ import annotations

import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
def _tuple_rows(patched_get_db_connection, monkeypatch):
    """The app pool's tuple rows, which the counting read indexes."""
    from tests.integration.test_steer_outbox_db import _app_rows

    _app_rows(monkeypatch)

# The seeded threads are shared with the workspace, as every thread there was
# before a starter could keep one private.
A = "sr-member-a"
B = "sr-member-b"
HOLDER = "shared-00000000-0000-4000-8000-0000000000bb"


async def _seed(conn) -> tuple[str, str]:
    ws, thread = uuid.uuid4(), uuid.uuid4()
    async with conn.cursor() as cur:
        for uid in (A, B):
            await cur.execute("INSERT INTO users (user_id) VALUES (%s)", (uid,))
        await cur.execute(
            "INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (HOLDER,)
        )
        await cur.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, 'Desk', 'running')",
            (ws, HOLDER),
        )
        await cur.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, created_by)"
            " VALUES (%s, %s, 'org', %s)",
            (ws, HOLDER, A),
        )
        await cur.execute(
            "INSERT INTO conversation_threads"
            " (conversation_thread_id, workspace_id, current_status, thread_index, created_by,"
            "  workspace_shared_at, workspace_shared_by)"
            " VALUES (%s, %s, 'completed', 0, %s, NOW(), %s)",
            (thread, ws, A, A),
        )
    return str(ws), str(thread)


async def _turn(conn, thread: str, sender: str, status: str, interrupt_reason=None) -> tuple[str, int]:
    """A turn ``sender`` ran on the thread, ended as ``status``. Rows are born
    in progress, as the lifecycle guard requires."""
    run = uuid.uuid4()
    async with conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO conversation_responses"
            " (conversation_response_id, conversation_thread_id, turn_index, status, user_id, created_at)"
            " VALUES (%s, %s, (SELECT count(*) FROM conversation_responses WHERE conversation_thread_id = %s),"
            "  'in_progress', %s, NOW())"
            " RETURNING run_seq",
            (run, thread, thread, sender),
        )
        row = await cur.fetchone()
        if status != "in_progress":
            await cur.execute(
                "UPDATE conversation_responses SET status = %s, interrupt_reason = %s"
                " WHERE conversation_response_id = %s",
                (status, interrupt_reason, run),
            )
    seq = row["run_seq"] if isinstance(row, dict) else row[0]
    return str(run), int(seq)


async def _thread_row(conn, thread: str) -> tuple:
    cur = await conn.execute(
        "SELECT last_seen_run_seq, is_pinned FROM conversation_threads WHERE conversation_thread_id = %s",
        (thread,),
    )
    row = await cur.fetchone()
    return (row["last_seen_run_seq"], row["is_pinned"]) if isinstance(row, dict) else tuple(row)


def _unseen(rows) -> set[str]:
    return {str(r["conversation_thread_id"]) for r in rows if r["branch"] == "unseen"}


async def test_reading_marks_the_turn_read_for_that_member_only(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database.conversation.threads_read import get_thread_lifecycle_rows
    from src.server.database.conversation.viewer_threads import (
        count_shared_attention,
        shared_lifecycle_rows,
    )
    from src.server.database.conversation.shared_thread_reads import member_seen, stamp_member_seen

    async with patched_get_db_connection() as conn:
        ws, thread = await _seed(conn)
        run, seq = await _turn(conn, thread, A, "completed")

    # Before anyone reads it, the turn is unread for both members.
    for member in (A, B):
        rows, _ = await shared_lifecycle_rows(member, [ws])
        assert _unseen(rows) == {thread}
        assert await count_shared_attention(member, [ws]) == 1

    assert await stamp_member_seen(thread, A, run) == {"last_seen_run_seq": seq, "latest_run_seq": seq}

    rows, _ = await shared_lifecycle_rows(A, [ws])
    assert _unseen(rows) == set()
    rows, _ = await shared_lifecycle_rows(B, [ws])
    assert _unseen(rows) == {thread}
    assert await count_shared_attention(B, [ws]) == 1
    assert (await member_seen(thread, A), await member_seen(thread, B)) == (seq, 0)
    async with patched_get_db_connection() as conn:
        assert await _thread_row(conn, thread) == (0, False)

    # Without the shared workspaces the read is the personal one: nothing of
    # the holder's threads reaches a member.
    rows, _ = await get_thread_lifecycle_rows(B)
    assert rows == []


async def test_a_cursor_moves_only_to_a_finished_run_of_its_thread_and_never_back(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database.conversation.shared_thread_reads import member_seen, stamp_member_seen

    async with patched_get_db_connection() as conn:
        _, thread = await _seed(conn)
        first, first_seq = await _turn(conn, thread, A, "completed")
        second, second_seq = await _turn(conn, thread, A, "completed")
        _, other = await _seed_other_thread(conn)
        foreign, _ = await _turn(conn, other, A, "completed")

    assert await stamp_member_seen(thread, B, foreign) is None
    await stamp_member_seen(thread, B, second)
    await stamp_member_seen(thread, B, first)
    assert await member_seen(thread, B) == second_seq

    async with patched_get_db_connection() as conn:
        running, _ = await _turn(conn, thread, A, "in_progress")
    assert await stamp_member_seen(thread, B, running) is None
    assert second_seq > first_seq


async def _seed_other_thread(conn) -> tuple[str, str]:
    ws = await (
        await conn.execute(
            "SELECT workspace_id::text FROM shared_workspaces WHERE holder_id = %s", (HOLDER,)
        )
    ).fetchone()
    ws_id = ws["workspace_id"] if isinstance(ws, dict) else ws[0]
    other = str(uuid.uuid4())
    await conn.execute(
        "INSERT INTO conversation_threads"
        " (conversation_thread_id, workspace_id, current_status, thread_index, created_by,"
        "  workspace_shared_at, workspace_shared_by)"
        " VALUES (%s, %s, 'completed', 1, %s, NOW(), %s)",
        (other, ws_id, A, A),
    )
    return ws_id, other


async def test_a_pin_is_one_members_ordering(patched_get_db_connection, cleanup_tables):
    from src.server.database.conversation.viewer_threads import workspace_threads_for_member
    from src.server.database.conversation.shared_thread_reads import set_member_pin

    async with patched_get_db_connection() as conn:
        ws, older = await _seed(conn)
        _, newer = await _seed_other_thread(conn)
        await conn.execute(
            "UPDATE conversation_threads SET updated_at = NOW() - interval '1 hour'"
            " WHERE conversation_thread_id = %s",
            (older,),
        )

    await set_member_pin(older, B, True)

    a_rows, _ = await workspace_threads_for_member(ws, A)
    b_rows, _ = await workspace_threads_for_member(ws, B)
    assert [str(r["conversation_thread_id"]) for r in a_rows] == [newer, older]
    assert [str(r["conversation_thread_id"]) for r in b_rows] == [older, newer]
    assert [r["is_pinned"] for r in b_rows] == [True, False]
    assert not any(r["is_pinned"] for r in a_rows)
    async with patched_get_db_connection() as conn:
        assert await _thread_row(conn, older) == (0, False)


async def test_a_turn_paused_for_its_sender_reads_as_running_to_another_member(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database.conversation.viewer_threads import (
        count_shared_attention,
        shared_lifecycle_rows,
    )

    async with patched_get_db_connection() as conn:
        ws, thread = await _seed(conn)
        await _turn(conn, thread, A, "interrupted", "user_question")

    rows, _ = await shared_lifecycle_rows(A, [ws])
    (mine,) = [r for r in rows if r["branch"] == "live"]
    assert (mine["latest_run_status"], mine["latest_interrupt_reason"]) == (
        "interrupted",
        "user_question",
    )
    assert await count_shared_attention(A, [ws]) == 1

    rows, _ = await shared_lifecycle_rows(B, [ws])
    (theirs,) = [r for r in rows if r["branch"] == "live"]
    assert (theirs["latest_run_status"], theirs["latest_interrupt_reason"]) == ("in_progress", None)
    assert await count_shared_attention(B, [ws]) == 0


async def test_a_quiet_check_carries_only_the_cursors_that_were_up_to_date(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database.conversation.shared_thread_reads import (
        keep_members_seen_over_quiet,
        member_seen,
        stamp_member_seen,
    )

    async with patched_get_db_connection() as conn:
        _, thread = await _seed(conn)
        _, early = await _turn(conn, thread, A, "completed")
        read, read_seq = await _turn(conn, thread, A, "completed")
        _, quiet_seq = await _turn(conn, thread, A, "completed")
    await stamp_member_seen(thread, A, read)
    async with patched_get_db_connection() as conn:
        await conn.execute(
            "INSERT INTO shared_thread_reads (conversation_thread_id, user_id, last_seen_run_seq)"
            " VALUES (%s, %s, %s)",
            (thread, B, early),
        )
        await keep_members_seen_over_quiet(conn, thread, quiet_seq)

    assert await member_seen(thread, A) == quiet_seq
    assert await member_seen(thread, B) == early
    assert read_seq < quiet_seq


async def test_both_sender_reads_name_a_turns_latest_attempt(
    patched_get_db_connection, cleanup_tables
):
    """The turn's sender, one turn at a time and for every turn at once,
    read off the same attempt: the newest of the turn's retry chain."""
    from src.server.database.conversation.senders import turn_sender, turn_senders

    async with patched_get_db_connection() as conn:
        _, thread = await _seed(conn)
        first, _ = await _turn(conn, thread, A, "completed")
        async with conn.cursor() as cur:
            # A retry of turn 0: attempt 2, inserted after it.
            await cur.execute(
                "INSERT INTO conversation_responses"
                " (conversation_response_id, conversation_thread_id, turn_index,"
                "  attempt_no, retry_of_run_id, status, user_id, created_at)"
                " VALUES (%s, %s, 0, 2, %s, 'in_progress', %s, NOW())",
                (uuid.uuid4(), thread, first, B),
            )

    assert await turn_sender(thread, 0) == B
    assert (await turn_senders(thread))[0]["user_id"] == B
