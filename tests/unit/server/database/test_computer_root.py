"""A reinstall's root move and a reconnect's repoint: the fences and what each writes.

A mock cannot tell a CAS holds, so what these lock is the SQL: each write is
fenced on what it was decided on and on the computer being down, and a root
move keeps the old root for every workspace's old paths.
"""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database import computer_root

COMPUTER_ID = "11111111-1111-4111-8111-111111111111"


@pytest.fixture
def cursor():
    cur = AsyncMock()
    cur.fetchone = AsyncMock(return_value={"computer_id": COMPUTER_ID})
    conn = MagicMock()

    @asynccontextmanager
    async def _cursor(**_):
        yield cur

    @asynccontextmanager
    async def _transaction():
        yield

    conn.cursor = _cursor
    conn.transaction = _transaction

    @asynccontextmanager
    async def _connection(passed=None):
        yield conn

    with patch.object(computer_root, "get_db_connection", new=_connection):
        yield cur


def _statements(cursor):
    return [
        (re.sub(r"\s+", " ", call.args[0]), call.args[1])
        for call in cursor.execute.await_args_list
    ]


@pytest.mark.asyncio
async def test_a_root_move_is_fenced_and_keeps_the_old_root(cursor):
    await computer_root.move_computer_root(
        COMPUTER_ID, old_root="/Users/dev/langalpha", new_root="/srv/la"
    )

    (comp_sql, comp), (ws_sql, ws) = _statements(cursor)
    assert "SET root_dir = %(new)s, start_failure = NULL" in comp_sql
    assert "AND root_dir = %(old)s" in comp_sql
    assert "status = ANY(%(repairable)s)" in comp_sql
    assert comp["repairable"] == ["stopped", "creating"]
    # The old root leads, once, capped as a move caps it.
    assert (
        "ARRAY[%(old)s::text] || array_remove(w.previous_root_dirs, %(old)s::text)"
        in ws_sql
    )
    assert ws["old"] == "/Users/dev/langalpha"
    assert ws["cap"] == 16
    # A restore is owed only where there is a backup to restore from.
    assert "CASE WHEN b.held THEN COALESCE(w.files_restore_incomplete_at, NOW())" in ws_sql
    assert "files_epoch = gen_random_uuid()" in ws_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("files_backup", "owed"), [(True, True), (None, True), (False, False)]
)
async def test_a_root_move_owes_restores_only_while_backup_is_on(
    cursor, files_backup, owed
):
    # Off, the reinstall tells the owner the files stay in the old folder, so
    # the last backup is not laid in the new one unasked.
    cursor.fetchone.return_value = {
        "computer_id": COMPUTER_ID,
        "files_backup": files_backup,
    }

    await computer_root.move_computer_root(COMPUTER_ID, old_root="/a", new_root="/b")

    (_, _), (ws_sql, ws) = _statements(cursor)
    assert "%(backed_up)s AND EXISTS (" in ws_sql
    assert ws["backed_up"] is owed
    # Off, a restore the old folder still owed goes too, or the start would
    # lay the last backup in the new folder all the same.
    assert "WHEN NOT %(backed_up)s THEN NULL" in ws_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("files_backup", "owed"), [(True, True), (None, True), (False, False)]
)
async def test_a_reconnect_move_owes_restores_only_while_backup_is_on(
    cursor, files_backup, owed
):
    cursor.fetchone.return_value = {
        "computer_id": COMPUTER_ID,
        "files_backup": files_backup,
    }

    await computer_root.repoint_computer_machine(
        COMPUTER_ID,
        provider_config={"host": "new"},
        expected_provider_config={"host": "old"},
        moved=True,
    )

    (_, _), (ws_sql, ws) = _statements(cursor)
    assert "%(backed_up)s AND EXISTS (" in ws_sql
    assert ws["backed_up"] is owed


@pytest.mark.asyncio
async def test_a_refused_root_move_touches_no_workspace(cursor):
    cursor.fetchone.return_value = None

    moved = await computer_root.move_computer_root(
        COMPUTER_ID, old_root="/a", new_root="/b"
    )

    assert moved is None
    assert len(_statements(cursor)) == 1


@pytest.mark.asyncio
async def test_a_repoint_is_fenced_on_the_target_it_replaces(cursor):
    await computer_root.repoint_computer_machine(
        COMPUTER_ID,
        provider_config={"host": "new"},
        expected_provider_config={"host": "old"},
    )

    ((sql, params),) = _statements(cursor)
    assert "AND provider_config = %(expected)s::jsonb" in sql
    assert "start_failure = NULL" in sql
    assert "status = ANY(%(repairable)s)" in sql
    assert params["expected"].obj == {"host": "old"}


@pytest.mark.asyncio
async def test_a_move_to_another_machine_owes_restores_in_the_same_transaction(cursor):
    await computer_root.repoint_computer_machine(
        COMPUTER_ID,
        provider_config={"host": "new"},
        expected_provider_config={"host": "old"},
        moved=True,
    )

    (_, _), (ws_sql, ws) = _statements(cursor)
    assert "CASE WHEN b.held THEN COALESCE(w.files_restore_incomplete_at, NOW())" in ws_sql
    assert "files_epoch = gen_random_uuid()" in ws_sql
    # The root is the same path, so no workspace remembers another.
    assert ws["old"] is None
