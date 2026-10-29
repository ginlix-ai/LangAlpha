"""A moved workspace keeps the roots of the computers it left.

The agent spells a file under its computer's root, and a computer its owner
brought has a root of its own. Once that computer is removed, the live
computer list no longer names its root, so a link an older turn wrote there
reads as a path outside every workspace. The commit that moves the workspace
is where the root it leaves is known, so the list is written there, in the
same statement that carries its former folders.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database import workspace_move as M
from src.server.database.workspace_move import (
    PREVIOUS_ROOT_DIRS_MAX,
    carried_roots,
)

MOVE_ID = "33333333-3333-4333-8333-333333333333"
RUNNER_ID = "44444444-4444-4444-8444-444444444444"
WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"
SOURCE = "11111111-1111-4111-8111-111111111111"
TARGET = "55555555-5555-4555-8555-555555555555"

SSH_ROOT = "/home/someone/langalpha"
STOCK_ROOT = "/home/workspace"


class TestCarriedRoots:
    def test_the_root_left_leads(self):
        assert carried_roots(SSH_ROOT, STOCK_ROOT, ["/srv/older"]) == [SSH_ROOT, "/srv/older"]

    def test_a_root_the_target_shares_adds_nothing(self):
        # Daytona and E2B share the stock root, so a move between them leaves
        # every path reading as it did.
        assert carried_roots(STOCK_ROOT, STOCK_ROOT, ["/srv/older"]) == ["/srv/older"]
        assert carried_roots(f"{SSH_ROOT}/", SSH_ROOT, []) == []

    def test_a_root_left_again_moves_to_the_front_once(self):
        previous = ["/srv/b", SSH_ROOT, "/srv/c"]
        assert carried_roots(SSH_ROOT, STOCK_ROOT, previous) == [SSH_ROOT, "/srv/b", "/srv/c"]

    def test_the_list_keeps_only_the_newest(self):
        previous = [f"/srv/m{i}" for i in range(PREVIOUS_ROOT_DIRS_MAX)]
        carried = carried_roots(SSH_ROOT, STOCK_ROOT, previous)
        assert len(carried) == PREVIOUS_ROOT_DIRS_MAX
        assert carried[0] == SSH_ROOT
        assert f"/srv/m{PREVIOUS_ROOT_DIRS_MAX - 1}" not in carried

    def test_a_source_with_no_root_keeps_the_list(self):
        assert carried_roots(None, SSH_ROOT, ("/srv/older",)) == ["/srv/older"]
        assert carried_roots("", SSH_ROOT, None) == []


class _Cursor:
    """Answers each statement the commit runs, and records what it was sent."""

    def __init__(self, *, source_root: str, target_root: str, previous: list[str]):
        self.statements: list[tuple[str, object]] = []
        self._last = ""
        self._source_root = source_root
        self._target_root = target_root
        self._previous = previous

    async def execute(self, sql, params=None):
        self._last = " ".join(sql.split())
        self.statements.append((self._last, params))

    async def fetchone(self):
        if self._last.startswith("SELECT * FROM workspace_moves"):
            return {
                "move_id": MOVE_ID,
                "runner_id": RUNNER_ID,
                "user_id": "user-1",
                "phase": "staged",
                "make_primary": False,
                "source_folder": "Research",
            }
        if "FROM workspaces w LEFT JOIN computers c" in self._last:
            return {
                "workspace_id": WORKSPACE_ID,
                "user_id": "user-1",
                "name": "Research",
                "computer_id": SOURCE,
                "previous_dir_names": [],
                "previous_root_dirs": self._previous,
                "status": "stopped",
                "source_root_dir": self._source_root,
            }
        if self._last.startswith("UPDATE workspace_moves"):
            return {"move_id": MOVE_ID, "phase": "committed", "stale": False}
        raise AssertionError(f"unexpected fetch after {self._last[:60]}")

    async def fetchall(self):
        return [
            {
                "computer_id": TARGET,
                "provider_ref": None,
                "layout_version": 0,
                "root_dir": self._target_root,
                "takes_a_workspace": True,
            }
        ]

    def written_roots(self):
        (params,) = [
            p for sql, p in self.statements
            if sql.startswith("UPDATE workspaces w SET previous_dir_names")
        ]
        return params["roots"]


async def _commit(cursor: _Cursor):
    class _Conn:
        @asynccontextmanager
        async def cursor(self, **_kwargs):
            yield cursor

    @asynccontextmanager
    async def _allocation(_computer_id):
        yield _Conn()

    with (
        patch.object(
            M,
            "get_workspace_move",
            AsyncMock(
                return_value={
                    "workspace_id": WORKSPACE_ID,
                    "source_computer_id": SOURCE,
                    "target_computer_id": TARGET,
                }
            ),
        ),
        patch.object(M, "folder_allocation", _allocation),
        patch.object(M, "lock_user_egress_state", AsyncMock()),
        patch.object(M, "get_workspace_dir_names_for_computer", AsyncMock(return_value=[])),
        patch.object(
            M, "bind_workspace_to_computer", AsyncMock(return_value={"dir_name": "Research"})
        ),
        patch.object(M, "bump_workspace_version", AsyncMock()),
        patch.object(M, "retire_workspace_grants", AsyncMock()),
    ):
        return await M.commit_workspace_move(MOVE_ID, RUNNER_ID)


@pytest.mark.asyncio
async def test_a_move_off_its_own_root_records_that_root_as_it_commits():
    cursor = _Cursor(source_root=SSH_ROOT, target_root=STOCK_ROOT, previous=["/srv/older"])
    committed = await _commit(cursor)
    assert committed["phase"] == "committed"
    assert cursor.written_roots() == [SSH_ROOT, "/srv/older"]
    # Written before the move is marked committed, on the one connection the
    # commit's transaction holds, so a move never commits without its root.
    sqls = [sql for sql, _ in cursor.statements]
    roots_at = next(i for i, s in enumerate(sqls) if "previous_root_dirs = %(roots)s" in s)
    committed_at = next(i for i, s in enumerate(sqls) if s.startswith("UPDATE workspace_moves"))
    assert roots_at < committed_at


@pytest.mark.asyncio
async def test_a_move_between_computers_on_one_root_leaves_the_list_alone():
    cursor = _Cursor(source_root=STOCK_ROOT, target_root=STOCK_ROOT, previous=[SSH_ROOT])
    await _commit(cursor)
    assert cursor.written_roots() == [SSH_ROOT]
