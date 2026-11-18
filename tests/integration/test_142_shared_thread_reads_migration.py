"""142 against real PostgreSQL: each member's read cursor and pin.

Seeds a database at 141 with a thread, upgrades to 142 and checks the new
table: one row per member and thread, starting unread and unpinned, gone with
its thread. The documented rollback drops the table and leaves the threads;
the upgrade then runs again.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest

from tests.integration.test_117_legacy_annotations_to_layers_migration import _alembic
from tests.integration.test_141_shared_workspaces_migration import _tables, scratch  # noqa: F401

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_142_up_down_up(scratch):  # noqa: F811
    await _alembic(scratch, "upgrade", "141")
    ws, thread, gone = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        await c.execute("INSERT INTO users (user_id, email) VALUES ('u-141', 'u@example.com')")
        await c.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, 'u-141', 'W', 'running')",
            (ws,),
        )
        for index, t in enumerate((thread, gone)):
            await c.execute(
                "INSERT INTO conversation_threads (conversation_thread_id, workspace_id, current_status, thread_index)"
                " VALUES (%s, %s, 'completed', %s)",
                (t, ws, index),
            )

    await _alembic(scratch, "upgrade", "142")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "shared_thread_reads" in await _tables(c)
        await c.execute(
            "INSERT INTO shared_thread_reads (conversation_thread_id, user_id) VALUES (%s, 'u-141'), (%s, 'u-141')",
            (thread, gone),
        )
        row = await (
            await c.execute(
                "SELECT last_seen_run_seq, is_pinned FROM shared_thread_reads WHERE conversation_thread_id = %s",
                (thread,),
            )
        ).fetchone()
        assert row == (0, False)
        with pytest.raises(psycopg.errors.UniqueViolation):
            await c.execute(
                "INSERT INTO shared_thread_reads (conversation_thread_id, user_id) VALUES (%s, 'u-141')",
                (thread,),
            )
        # A member's row goes with its thread.
        await c.execute("DELETE FROM conversation_threads WHERE conversation_thread_id = %s", (gone,))
        n = await (await c.execute("SELECT count(*) FROM shared_thread_reads")).fetchone()
        assert n == (1,)

    await _alembic(scratch, "downgrade", "141")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "shared_thread_reads" not in await _tables(c)
        n = await (await c.execute("SELECT count(*) FROM conversation_threads")).fetchone()
        assert n == (1,)

    await _alembic(scratch, "upgrade", "142")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "shared_thread_reads" in await _tables(c)
