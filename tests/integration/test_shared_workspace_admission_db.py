"""Two senders, one shared workspace thread, real PostgreSQL.

A send into a thread whose running turn another member sent is refused at
admission, read off the same in-progress run row every worker reads, so no
worker steers one member's message into another's turn. The member whose
turn it is steers it as before.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.handlers.chat.admission_gate import wait_or_steer
from src.server.services.runs import admission

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

A = "sw-member-a"
B = "sw-member-b"
HOLDER = "shared-00000000-0000-4000-8000-0000000000aa"


class _Manager:
    """The executor's admission read, with no run hosted on this process."""

    async def wait_for_admission(self, thread_id):
        return await admission.wait_for_admission(
            thread_id, local_task_probe=AsyncMock(return_value=None)
        )


@pytest.fixture
def no_mutation():
    runner = MagicMock()
    runner.is_mutating = AsyncMock(return_value=False)
    with patch(
        "src.server.services.thread_mutation.ThreadMutationRunner.get_instance",
        return_value=runner,
    ):
        yield


async def _seed(conn) -> tuple[str, str]:
    ws, thread, run = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with conn.cursor() as cur:
        for uid, name in ((A, "Maya Okafor"), (B, "Lee Chen")):
            await cur.execute("INSERT INTO users (user_id, name) VALUES (%s, %s)", (uid, name))
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
            " (conversation_thread_id, workspace_id, current_status, thread_index, created_by)"
            " VALUES (%s, %s, 'in_progress', 0, %s)",
            (thread, ws, A),
        )
        await cur.execute(
            "INSERT INTO conversation_responses"
            " (conversation_response_id, conversation_thread_id, turn_index, status, user_id, created_at)"
            " VALUES (%s, %s, 0, 'in_progress', %s, NOW())",
            (run, thread, A),
        )
    return str(thread), str(run)


async def _count(conn, sql: str, *args) -> int:
    async with conn.cursor() as cur:
        await cur.execute(sql, args)
        row = await cur.fetchone()
    return int(next(iter(row.values())) if isinstance(row, dict) else row[0])


async def test_another_members_send_is_refused_and_the_sender_steers(
    patched_get_db_connection, cleanup_tables, no_mutation
):
    async with patched_get_db_connection() as conn:
        thread, run = await _seed(conn)

    with pytest.raises(HTTPException) as refused:
        await wait_or_steer(_Manager(), thread, "B butts in", B)
    assert refused.value.status_code == 409
    detail = refused.value.detail
    assert detail["code"] == "another_member"
    assert detail["member"] == {"user_id": A, "name": "Maya Okafor"}
    assert "Maya Okafor's turn is running" in detail["message"]

    async with patched_get_db_connection() as conn:
        # Nothing was written for the refused send: no steer, no second run.
        assert await _count(conn, "SELECT count(*) FROM hook_outbox WHERE conversation_thread_id = %s", thread) == 0
        assert await _count(
            conn, "SELECT count(*) FROM conversation_responses WHERE conversation_thread_id = %s", thread
        ) == 1

    ready, steering = await wait_or_steer(_Manager(), thread, "A adds a note", A)
    assert ready is False and steering is not None
    async with patched_get_db_connection() as conn:
        assert await _count(conn, "SELECT count(*) FROM hook_outbox WHERE conversation_thread_id = %s", thread) == 1


@pytest.mark.parametrize(
    ("reason", "holds"),
    [("user_question", True), ("approval_required", True), ("credit_pause", False), (None, False)],
)
async def test_a_pause_holds_the_thread_only_while_it_waits_on_an_answer(
    patched_get_db_connection, cleanup_tables, reason, holds
):
    from src.server.database.conversation.senders import paused_turn_sender

    async with patched_get_db_connection() as conn:
        thread, run = await _seed(conn)
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE conversation_responses SET status = 'interrupted', interrupt_reason = %s"
                " WHERE conversation_response_id = %s",
                (reason, run),
            )

    assert await paused_turn_sender(thread) == (A if holds else None)
