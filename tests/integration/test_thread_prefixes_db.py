"""Which threads keep their dirs on the machine, against real PostgreSQL.

An archived thread keeps its scratchpad while it is still in use, which the
prefix read decides from the thread's runs and its background tasks in one
statement; and the prune's fenced read has to hold a turn's admission off
until its removal is done. Only Postgres can show either. Each test makes its
own user, workspace and threads and removes them afterwards.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg.errors import LockNotAvailable

from src.server.database.conversation import (
    fenced_workspace_thread_prefixes,
    get_workspace_thread_prefixes,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ARCHIVED_AT = datetime(2026, 1, 2, tzinfo=timezone.utc)
BEFORE = ARCHIVED_AT - timedelta(hours=1)
AFTER = ARCHIVED_AT + timedelta(hours=1)


@pytest_asyncio.fixture
async def workspace(patched_get_db_connection, test_db_pool):
    from src.server.database.conversation import create_thread
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    user_id = f"prefix-test-{uuid.uuid4().hex[:12]}"
    await create_user(user_id=user_id, email=f"{user_id}@example.com", name="Test")
    row = await create_workspace(user_id=user_id, name="Prefixes", status="running")
    workspace_id = str(row["workspace_id"])

    async def thread(
        *,
        archived: bool = False,
        run: tuple[str, datetime] | None = None,
        task_running: bool = False,
    ) -> str:
        """A thread, archived at ARCHIVED_AT if asked, whose latest run has
        ``run``'s status and start, with a background task still running if
        asked."""
        thread_id = str(uuid.uuid4())
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=workspace_id,
            current_status="completed",
        )
        async with test_db_pool.connection() as conn:
            if run is not None:
                status, created_at = run
                run_id = str(uuid.uuid4())
                # Inserted live and moved on, as the terminal-status trigger asks.
                await conn.execute(
                    "INSERT INTO conversation_responses (conversation_response_id, "
                    "conversation_thread_id, turn_index, status, created_at) "
                    "VALUES (%s, %s, 0, 'in_progress', %s)",
                    (run_id, thread_id, created_at),
                )
                if status != "in_progress":
                    await conn.execute(
                        "UPDATE conversation_responses SET status = %s "
                        "WHERE conversation_response_id = %s",
                        (status, run_id),
                    )
            if task_running:
                await conn.execute(
                    "INSERT INTO subagent_tasks (thread_id, task_id) VALUES (%s, 'k1')",
                    (thread_id,),
                )
                await conn.execute(
                    "INSERT INTO subagent_runs (thread_id, task_id) VALUES (%s, 'k1')",
                    (thread_id,),
                )
            if archived:
                await conn.execute(
                    "UPDATE conversation_threads SET archived_at = %s "
                    "WHERE conversation_thread_id = %s",
                    (ARCHIVED_AT, thread_id),
                )
        return thread_id

    try:
        yield SimpleNamespace(id=workspace_id, thread=thread)
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM conversation_threads WHERE workspace_id = %s", (workspace_id,)
            )
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (workspace_id,))
            await conn.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


async def test_an_archived_thread_keeps_its_scratchpad_only_while_in_use(workspace):
    kept = {
        "open": await workspace.thread(run=("completed", BEFORE)),
        "never run": await workspace.thread(),
        "running": await workspace.thread(archived=True, run=("in_progress", BEFORE)),
        "paused": await workspace.thread(archived=True, run=("interrupted", BEFORE)),
        "run since": await workspace.thread(archived=True, run=("completed", AFTER)),
        "task running": await workspace.thread(
            archived=True, run=("completed", BEFORE), task_running=True
        ),
    }
    idle = {
        "idle": await workspace.thread(archived=True, run=("completed", BEFORE)),
        "never run, archived": await workspace.thread(archived=True),
    }

    prefixes = await get_workspace_thread_prefixes(workspace.id)

    assert prefixes.all == {t[:8] for t in (*kept.values(), *idle.values())}
    assert prefixes.open == {t[:8] for t in kept.values()}


async def test_admission_waits_for_the_fenced_read(workspace, test_db_uri):
    """A turn admitted between the prune's judgment and its removal would
    write into a dir about to go."""
    from src.server.database.workspace import lock_run_workspace

    thread_id = await workspace.thread(archived=True, run=("completed", BEFORE))

    async def admit() -> None:
        # Its own session: the fence holds one of the test pool's few.
        async with await AsyncConnection.connect(test_db_uri) as conn, conn.transaction():
            await conn.execute("SET LOCAL lock_timeout = '200ms'")
            await lock_run_workspace(conn, thread_id)

    async with fenced_workspace_thread_prefixes(workspace.id) as kept:
        assert thread_id[:8] in kept.all and thread_id[:8] not in kept.open
        with pytest.raises(LockNotAvailable):
            await admit()
    await asyncio.wait_for(admit(), timeout=5)
