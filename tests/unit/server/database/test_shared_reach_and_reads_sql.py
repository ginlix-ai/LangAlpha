"""The statements behind two shared-workspace rules whose effect only a real
Postgres shows (``tests/integration/test_shared_thread_privacy_db.py`` and
``test_shared_unread_baseline_db.py``): START's reach read locks the rows that
grant reach, and a member's first read row starts at the cursor they read
with none."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture
def statements(mock_connection, mock_cursor, monkeypatch):
    from src.server.database import shared_workspaces
    from src.server.database.conversation import shared_thread_reads

    @asynccontextmanager
    async def _get_db_connection(conn=None):
        yield conn if conn is not None else mock_connection

    for module in (shared_workspaces, shared_thread_reads):
        monkeypatch.setattr(module, "get_db_connection", _get_db_connection)
    return mock_cursor


async def test_starts_reach_read_locks_what_grants_it(statements, mock_connection):
    from src.server.database.shared_workspaces import reach_held

    statements.fetchone.return_value = (True,)
    assert await reach_held(mock_connection, "shared-0", "lee", "org", {"t2", "t1"}) is True
    sql, params = statements.execute.await_args.args
    # The workspace's row, which an audience change updates, and the member
    # row, which a removal deletes.
    assert "FOR SHARE OF s" in sql
    assert "m.user_id = %(user)s FOR KEY SHARE" in sql
    assert "s.team_id = ANY(%(teams)s::text[])" in sql and "OR s.everyone" in sql
    assert params == {"holder": "shared-0", "user": "lee", "org": "org", "teams": ["t1", "t2"]}

    statements.fetchone.return_value = None
    assert await reach_held(mock_connection, "shared-0", "lee", "org", ()) is False
    sql, params = statements.execute.await_args.args
    assert "teams" not in params and "team_id" not in sql


@pytest.mark.parametrize("write", ["pin", "seen"])
async def test_a_first_read_row_starts_at_the_baseline(statements, write):
    from src.server.database.conversation import shared_thread_reads
    from src.server.database.conversation.viewer_threads import member_baseline_sql

    statements.fetchone.return_value = None
    if write == "pin":
        await shared_thread_reads.set_member_pin(
            "7157f2a5-26af-4dab-9b63-0219dbd5d814", "lee", True
        )
    else:
        await shared_thread_reads.stamp_member_seen(
            "7157f2a5-26af-4dab-9b63-0219dbd5d814", "lee",
            "0b5a1c34-4a8e-4c43-9a43-7b7a7d2ef0a1",
        )
    sql = statements.execute.await_args.args[0]
    assert member_baseline_sql("%(user)s::varchar") in sql
