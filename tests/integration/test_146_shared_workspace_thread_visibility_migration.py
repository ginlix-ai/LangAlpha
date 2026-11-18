"""146 against real PostgreSQL: whether a shared workspace's thread is shared.

Seeds a database at 145 with a shared workspace holding two threads and a
personal workspace holding one. After 146 the shared workspace's threads read
as shared since their creation, by their starter, and the personal thread
stays null, which no reader of it asks about. A thread whose starter's account
is gone (``created_by`` has no foreign key) reads as shared with no sharer
recorded, since the new key would refuse its id. A thread inserted with no word
on it is private, and the rollback refuses while one is, since dropping both
columns would show it to the whole workspace, unless the operator says to;
the upgrade then runs again and records it as shared.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest

from tests.integration.test_117_legacy_annotations_to_layers_migration import _alembic
from tests.integration.test_141_shared_workspaces_migration import scratch  # noqa: F401

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

STARTERS = ("u-146-maya", "u-146-lee")
#: A starter whose account is gone: in ``created_by``, with no ``users`` row.
GONE = "u-146-gone"


async def _columns(c) -> set[str]:
    rows = await (
        await c.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'conversation_threads' AND column_name LIKE 'workspace_shared%%'"
        )
    ).fetchall()
    return {r[0] for r in rows}


async def _key_validated(c) -> bool | None:
    """Whether the sharer's key is validated; None when there is none."""
    row = await (
        await c.execute(
            "SELECT convalidated FROM pg_constraint"
            " WHERE conname = 'conversation_threads_workspace_shared_by_fkey'"
        )
    ).fetchone()
    return row[0] if row else None


async def _state(c) -> dict[str, tuple]:
    rows = await (
        await c.execute(
            "SELECT title, workspace_shared_at = created_at, workspace_shared_by, workspace_shared_at IS NULL"
            " FROM conversation_threads"
        )
    ).fetchall()
    return {r[0]: tuple(r[1:]) for r in rows}


async def test_146_up_down_up(scratch, monkeypatch):  # noqa: F811
    await _alembic(scratch, "upgrade", "145")
    shared_ws, own_ws = uuid.uuid4(), uuid.uuid4()
    holder = f"shared-{shared_ws}"
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        for uid in STARTERS:
            await c.execute("INSERT INTO users (user_id) VALUES (%s)", (uid,))
        await c.execute("INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (holder,))
        for ws, owner in ((shared_ws, holder), (own_ws, STARTERS[0])):
            await c.execute(
                "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, %s, 'running')",
                (ws, owner, str(ws)[:8]),
            )
        await c.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, everyone, created_by)"
            " VALUES (%s, %s, 'org', false, %s)",
            (shared_ws, holder, STARTERS[0]),
        )
        for i, (ws, title, starter) in enumerate((
            (shared_ws, "maya-in-shared", STARTERS[0]),
            (shared_ws, "lee-in-shared", STARTERS[1]),
            (shared_ws, "gone-in-shared", GONE),
            (own_ws, "maya-own", None),
        )):
            await c.execute(
                "INSERT INTO conversation_threads"
                " (conversation_thread_id, workspace_id, current_status, thread_index, title, created_by,"
                "  created_at)"
                " VALUES (%s, %s, 'completed', %s, %s, %s, NOW() - INTERVAL '3 days')",
                (uuid.uuid4(), ws, i, title, starter),
            )

    expected = {
        "maya-in-shared": (True, STARTERS[0], False),
        "lee-in-shared": (True, STARTERS[1], False),
        "gone-in-shared": (True, None, False),
        "maya-own": (None, None, True),
    }
    await _alembic(scratch, "upgrade", "146")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _state(c) == expected
        # The key is validated, not left NOT VALID.
        assert (await _key_validated(c)) is True
        # A new row that says nothing about sharing is private.
        await c.execute(
            "INSERT INTO conversation_threads"
            " (conversation_thread_id, workspace_id, current_status, thread_index, title, created_by)"
            " VALUES (%s, %s, 'completed', 9, 'new', %s)",
            (uuid.uuid4(), shared_ws, STARTERS[1]),
        )
        assert (await _state(c))["new"] == (None, None, True)

    with pytest.raises(RuntimeError, match=r"1 thread\(s\) in 1 shared workspace\(s\)"):
        await _alembic(scratch, "downgrade", "145")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _columns(c) == {"workspace_shared_at", "workspace_shared_by"}

    monkeypatch.setenv("MIGRATION_146_DOWNGRADE_SHOWS_PRIVATE_THREADS", "1")
    await _alembic(scratch, "downgrade", "145")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _columns(c) == set()
        assert await _key_validated(c) is None
        assert (await (await c.execute("SELECT count(*) FROM conversation_threads")).fetchone())[0] == 5

    await _alembic(scratch, "upgrade", "146")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _columns(c) == {"workspace_shared_at", "workspace_shared_by"}
        assert await _state(c) == {**expected, "new": (True, STARTERS[1], False)}
        assert await _key_validated(c) is True
        # The key still nulls the sharer when their account goes.
        await c.execute("DELETE FROM users WHERE user_id = %s", (STARTERS[1],))
        assert (await _state(c))["lee-in-shared"] == (True, None, False)
