"""The claim handing the sweep a folder a move from the backup left, and the
check whether a workspace holds that folder's name now.

Setting a folder aside dials its owner's machine, which may be dark or slow,
so the claim gives each row to one worker per retry window, puts a row tried
lately behind untried ones, and never hands out one on a machine that cannot
answer. These pin that at the SQL level, as the other database units do; the
statement runs on Postgres. The check folds names as folder allocation does,
since the owner's disk may not tell case apart.
"""

from __future__ import annotations

import importlib.util
import re
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.workspace_move import (
    LEFT_FOLDER_RETRY_S,
    claim_folders_left_in_place,
    left_folder_taken,
)

_DB = "src.server.database.workspace_move"
_VERSIONS = Path(__file__).resolve().parents[4] / "migrations" / "versions"
MOVE_ID = "66666666-6666-4666-8666-666666666666"
SOURCE = "77777777-7777-4777-8777-777777777777"


class _Conn:
    def __init__(self, rows=()) -> None:
        self.sql = ""
        self.params: dict = {}
        self.rows = list(rows)

    @asynccontextmanager
    async def cursor(self, **_kwargs):
        yield self

    async def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = dict(params or {})

    async def fetchall(self):
        return self.rows


async def _claim(rows=()) -> tuple[list, _Conn]:
    conn = _Conn(rows)

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch(f"{_DB}.get_db_connection", _connection):
        claimed = await claim_folders_left_in_place()
    return claimed, conn


def _inner_where(sql: str) -> str:
    return sql[sql.index("WHERE m.") : sql.index(" ORDER BY")]


@pytest.mark.asyncio
async def test_a_claim_stamps_each_row_it_hands_out_and_skips_rows_another_holds():
    row = {
        "move_id": MOVE_ID,
        "source_computer_id": SOURCE,
        "source_folder": "Research",
        "source_gone": False,
    }

    claimed, conn = await _claim([row])

    assert claimed == [row]
    # Stamped in the statement that picks it, so a worker claiming at the
    # same moment skips the row rather than trying it too.
    assert conn.sql.startswith(
        "UPDATE workspace_moves claimed SET left_folder_tried_at = NOW() FROM ("
    )
    assert "FOR UPDATE OF m SKIP LOCKED ) due WHERE claimed.move_id = due.move_id" in conn.sql
    # One at a time, so the window starts when the row's try does.
    assert conn.params["limit"] == 1


@pytest.mark.asyncio
async def test_a_row_tried_lately_waits_out_the_retry_then_goes_behind_untried_ones():
    """A folder that never clears (no room, a dark machine) would otherwise
    hold the head of the queue on every tick."""
    _, conn = await _claim()

    where = _inner_where(conn.sql)
    assert (
        "AND ( m.left_folder_tried_at IS NULL"
        " OR m.left_folder_tried_at < NOW() - make_interval(secs => %(retry_s)s) )"
    ) in where
    assert conn.params["retry_s"] == LEFT_FOLDER_RETRY_S
    assert "ORDER BY m.left_folder_tried_at NULLS FIRST, m.finished_at LIMIT" in conn.sql


@pytest.mark.asyncio
async def test_only_a_gone_source_or_one_running_on_a_reachable_host_is_due():
    """A gone source costs no dial; a host the provider reports unavailable
    would only time out."""
    _, conn = await _claim()

    assert (
        "AND ( c.computer_id IS NULL OR c.status = 'deleted'"
        " OR (c.status = 'running' AND c.host_unavailable_since IS NULL) )"
    ) in _inner_where(conn.sql)
    assert "AND m.phase = 'done'" in _inner_where(conn.sql)


@pytest.mark.asyncio
async def test_the_claim_keeps_to_the_partial_index_the_hold_reads():
    spec = importlib.util.spec_from_file_location(
        "migration_085", _VERSIONS / "085_owner_file_backup.py"
    )
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    [index] = [s for s in migration._STATEMENTS if "workspace_moves_left_in_place" in s]
    predicate = re.search(r"WHERE (.+)", " ".join(index.split())).group(1)

    _, conn = await _claim()

    qualified = " AND ".join(f"m.{term}" for term in predicate.split(" AND "))
    assert _inner_where(conn.sql).startswith(f"WHERE {qualified} AND ")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("folder", "held", "taken"),
    [
        ("Research", ("Research",), True),
        ("Research", ("research",), True),
        ("research", ("Notes", "RESEARCH"), True),
        ("Straße", ("STRASSE",), True),
        ("Research", ("Research 2", "Notes"), False),
        ("Research", (), False),
    ],
    ids=["same", "renamed-to-a-case-variant", "among-others", "casefold-not-lower", "other", "none"],
)
async def test_a_name_held_by_any_spelling_folding_alike_is_taken(folder, held, taken):
    """A workspace renamed to a case variant lands on the same directory; an
    exact match let the sweep move that live folder into leftovers."""
    conn = object()
    names = AsyncMock(return_value=held)

    with patch(f"{_DB}.get_workspace_dir_names_for_computer", names):
        assert await left_folder_taken(conn, SOURCE, folder) is taken

    # Every name a workspace row holds, a staged row's landings included, and
    # none of the move holds, the sweep's own among them.
    names.assert_awaited_once_with(SOURCE, conn=conn, left_by_moves=False)
