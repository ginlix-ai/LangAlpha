"""Whether a member's list of shared workspaces says one holds a thread they
have not read, real PostgreSQL.

The list answers it from the cursor the thread list and the feed snapshot
read, so the three agree: a colleague's finished turn is unread for a member
until that member reads it, a running turn is not unread, and an archived
thread counts for nothing.
"""

from __future__ import annotations

import pytest

from tests.integration.test_shared_thread_reads_db import A, B, _seed, _turn

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
def _tuple_rows(patched_get_db_connection, monkeypatch):
    from tests.integration.test_steer_outbox_db import _app_rows

    _app_rows(monkeypatch)


async def _unread(member: str) -> bool:
    from src.server.database.shared_workspaces import list_with_unread

    [(_, _, unread)] = await list_with_unread(member, "org")
    return unread


async def _snapshot_unseen(member: str, ws: str) -> bool:
    from src.server.database.conversation.viewer_threads import shared_lifecycle_rows

    rows, _ = await shared_lifecycle_rows(member, [ws])
    return any(r["branch"] == "unseen" for r in rows)


async def test_the_list_says_unread_by_each_members_own_cursor(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database.conversation.shared_thread_reads import stamp_member_seen

    async with patched_get_db_connection() as conn:
        ws, thread = await _seed(conn)
        for uid in (A, B):
            await conn.execute(
                "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by, added_at)"
                " VALUES (%s, %s, 'member', %s, NOW() - INTERVAL '2 hours')",
                (ws, uid, A),
            )

    async def agree(member: str, expected: bool) -> None:
        assert await _unread(member) is expected
        assert await _snapshot_unseen(member, ws) is expected

    # No turn yet: nothing to read.
    await agree(A, False)
    await agree(B, False)

    async with patched_get_db_connection() as conn:
        run, _ = await _turn(conn, thread, A, "completed")
    await agree(A, True)
    await agree(B, True)

    # B reads it; A has not.
    await stamp_member_seen(thread, B, run)
    await agree(B, False)
    await agree(A, True)

    # A turn running now is not unread for anyone.
    async with patched_get_db_connection() as conn:
        live, _ = await _turn(conn, thread, A, "in_progress")
    await agree(A, False)
    await agree(B, False)

    # Once it ends it is unread for B again, and an archived thread counts for nothing.
    async with patched_get_db_connection() as conn:
        await conn.execute(
            "UPDATE conversation_responses SET status = 'completed' WHERE conversation_response_id = %s", (live,)
        )
    await agree(B, True)
    async with patched_get_db_connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET archived_at = NOW() WHERE conversation_thread_id = %s", (thread,)
        )
    await agree(B, False)
    await agree(A, False)


async def test_the_reach_read_is_unchanged(patched_get_db_connection, cleanup_tables):
    """The feed's reach read takes no unread column: it runs per event."""
    from src.server.database.shared_workspaces import list_for_member

    async with patched_get_db_connection() as conn:
        ws, thread = await _seed(conn)
        await conn.execute(
            "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)"
            " VALUES (%s, %s, 'manager', %s)",
            (ws, A, A),
        )
        await _turn(conn, thread, B, "completed")
    [(row, role)] = await list_for_member(A, "org")
    assert (row.workspace_id, role) == (ws, "manager")
