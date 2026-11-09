"""User-facing thread lists leave room-member threads out.

A room member answers in its own thread, but that thread is the member's
context boundary, not a conversation the user started: listing it put one
untitled row per bot in the sidebar (and the room page is where it is read).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.server.database.conversation import threads_read


class _Cursor:
    def __init__(self, sql: list[str]) -> None:
        self.sql = sql

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, query, params):
        self.sql.append(str(query))

    async def fetchone(self):
        return {"total": 0}

    async def fetchall(self):
        return []


class _Conn:
    def __init__(self, sql: list[str]) -> None:
        self.sql = sql

    def cursor(self, **_kwargs):
        return _Cursor(self.sql)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def get_db_connection(self):
        return _Conn(self.sql)


@pytest.mark.asyncio
async def test_workspace_list_excludes_room_member_threads():
    fake = _Pool()
    with patch.object(threads_read.pool, "get_db_connection", fake.get_db_connection):
        rows, total = await threads_read.get_workspace_threads("ws-1")
    assert rows == [] and total == 0
    assert len(fake.sql) == 2
    for query in fake.sql:
        assert "bot_threads" in query
        assert "bt.kind = 'room'" in query


@pytest.mark.asyncio
async def test_cross_workspace_list_excludes_room_member_threads():
    fake = _Pool()
    with patch.object(threads_read.pool, "get_db_connection", fake.get_db_connection):
        rows, total = await threads_read.get_threads_for_user("u-1")
    assert rows == [] and total == 0
    assert len(fake.sql) == 2
    for query in fake.sql:
        assert "bot_threads" in query
        assert "bt.kind = 'room'" in query
