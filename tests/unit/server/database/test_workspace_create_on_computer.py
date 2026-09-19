"""Creating a project directly on a machine: one statement, one folder, one shadow.

Instant creation rests on this insert. The row has to be born bound, carrying
the machine's lifecycle fields, because everything downstream reads the project
row and would otherwise see a project that exists on no computer for a window.
The folder name is the other half: ``dir_name`` is unique per computer, and the
name half belongs to the user, so two projects called the same thing have to
settle it on the suffix rather than failing the create.
"""

from __future__ import annotations

import hashlib
import re
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from psycopg.errors import UniqueViolation

from src.server.database import workspace as W

COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"


class _Violation(UniqueViolation):
    """A unique violation that reports a constraint name, as the driver does."""

    def __init__(self, constraint: str | None):
        super().__init__("duplicate key value violates unique constraint")
        self._constraint = constraint

    @property
    def diag(self):
        return SimpleNamespace(constraint_name=self._constraint)


def _row(**overrides):
    row = {
        "workspace_id": WORKSPACE_ID,
        "user_id": "user-1",
        "name": "Research",
        "computer_id": COMPUTER_ID,
        "dir_name": "research-ab12",
        "status": "stopped",
        "resource_tier": "performance",
        "is_always_on": False,
    }
    row.update(overrides)
    return row


@pytest.fixture
def cursor():
    cur = AsyncMock()
    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock(return_value=_row())
    return cur


@pytest.fixture
def db(cursor):
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake(passed=None):
        yield passed if passed is not None else conn

    with patch("src.server.database.workspace.get_db_connection", new=_fake):
        yield cursor


def _sql(cursor, call=-1) -> str:
    return re.sub(r"\s+", " ", cursor.execute.call_args_list[call][0][0])


def _params(cursor, call=-1) -> dict:
    return cursor.execute.call_args_list[call][0][1]


class TestTheStatement:
    @pytest.mark.asyncio
    async def test_the_project_is_born_on_the_machine(self, db):
        """Never a window where the row exists unbound: no second write to lose."""
        await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
        assert db.execute.await_count == 1
        sql = _sql(db)
        assert sql.count("INSERT INTO workspaces") == 1
        assert "comp.computer_id" in sql

    @pytest.mark.asyncio
    async def test_the_lifecycle_fields_are_copied_from_the_computer(self, db):
        """They are shadows of the machine, so the insert reads them, never guesses."""
        await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
        sql = _sql(db)
        assert "comp.status, comp.resource_tier, comp.is_always_on" in sql
        for field in ("status", "resource_tier", "is_always_on"):
            assert f"%({field})s" not in sql

    @pytest.mark.asyncio
    async def test_the_computer_is_read_under_a_share_lock(self, db):
        """A status move that lands mid-insert must not skip the new row: it
        cannot see it, so the row would keep the pre-move status forever."""
        await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
        assert "FOR SHARE" in _sql(db)

    @pytest.mark.asyncio
    async def test_a_tombstoned_computer_creates_nothing(self, db):
        await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
        assert "status <> 'deleted'" in _sql(db)

    @pytest.mark.asyncio
    async def test_a_vanished_computer_returns_none_rather_than_raising(self, db):
        """The caller's signal to resolve a machine again, not to retry this."""
        db.fetchone = AsyncMock(return_value=None)
        assert (
            await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
            is None
        )

    @pytest.mark.asyncio
    async def test_an_unparseable_computer_writes_nothing(self, db):
        assert await W.create_workspace_on_computer("user-1", "R", "not-a-uuid") is None
        db.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_sandbox_is_copied_too_so_the_row_is_a_whole_shadow(self, db):
        """A project created while its machine is already running would
        otherwise name no sandbox, and the attach path reads that as a split
        binding and rebuilds the machine out from under every sibling."""
        await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
        sql = _sql(db)
        assert "comp.provider_ref, comp.platform_secret_version" in sql
        assert "sandbox_id, platform_secret_version" in sql

    @pytest.mark.asyncio
    async def test_the_caller_is_told_its_folder(self, db):
        """The API answers with it, so re-reading the row to learn it is waste."""
        result = await W.create_workspace_on_computer(
            "user-1", "Research", COMPUTER_ID
        )
        assert "dir_name" in _sql(db).split("RETURNING")[1]
        assert result["dir_name"] == "research-ab12"


class TestTheFolderName:
    def test_the_slug_follows_the_sql_rule(self):
        assert W.workspace_dir_name("Q3 Earnings!", WORKSPACE_ID).startswith(
            "q3-earnings-"
        )

    def test_an_empty_name_still_owns_a_folder(self):
        assert W.workspace_dir_name("", WORKSPACE_ID).startswith("workspace-")
        assert W.workspace_dir_name(None, WORKSPACE_ID).startswith("workspace-")

    def test_the_same_name_on_two_projects_gives_two_folders(self):
        """Which is why the suffix exists: the name half is the user's to repeat."""
        a = W.workspace_dir_name("Research", WORKSPACE_ID)
        b = W.workspace_dir_name("Research", COMPUTER_ID)
        assert a != b

    def test_widening_keeps_the_name_and_lengthens_the_suffix(self):
        narrow = W.workspace_dir_name("Research", WORKSPACE_ID, hex_chars=4)
        wide = W.workspace_dir_name("Research", WORKSPACE_ID, hex_chars=8)
        assert wide.startswith(narrow)
        assert len(wide) == len(narrow) + 4

    def test_no_width_can_overflow_the_column(self):
        """dir_name is VARCHAR(64). A long name plus a widened suffix used to
        produce 68 chars, and StringDataRightTruncation is not a
        UniqueViolation, so the re-slug loop above never catches it: a folder
        collision on a long name became a 500 on the create."""
        for hex_chars in (4, 8, 12, 16):
            assert (
                len(W.workspace_dir_name("x" * 120, WORKSPACE_ID, hex_chars=hex_chars))
                <= 64
            )
        # The suffix is what the retry widens, so it must survive intact.
        assert W.workspace_dir_name("x" * 120, WORKSPACE_ID, hex_chars=8).endswith(
            hashlib.md5(WORKSPACE_ID.encode("utf-8")).hexdigest()[:8]
        )


class TestTheCollision:
    @pytest.mark.asyncio
    async def test_a_taken_folder_is_re_slugged_and_retried(self, db):
        """The create must not fail because a sibling took the short suffix."""
        db.execute = AsyncMock(
            side_effect=[_Violation(W._COMPUTER_DIR_INDEX), None]
        )
        result = await W.create_workspace_on_computer(
            "user-1", "Research", COMPUTER_ID, workspace_id=WORKSPACE_ID
        )
        assert result is not None
        assert db.execute.await_count == 2
        first, second = _params(db, 0)["dir_name"], _params(db, 1)["dir_name"]
        assert second != first
        assert second.startswith(first)

    @pytest.mark.asyncio
    async def test_every_attempt_colliding_reaches_the_caller(self, db):
        db.execute = AsyncMock(side_effect=_Violation(W._COMPUTER_DIR_INDEX))
        with pytest.raises(W.WorkspaceDirNameTaken) as caught:
            await W.create_workspace_on_computer(
                "user-1", "Research", COMPUTER_ID, workspace_id=WORKSPACE_ID
            )
        assert caught.value.computer_id == COMPUTER_ID
        assert db.execute.await_count == 3

    @pytest.mark.asyncio
    async def test_another_unique_violation_is_not_re_slugged(self, db):
        """Renaming the folder can never clear a primary-key clash, so retrying
        one would burn every attempt and then report the wrong cause."""
        db.execute = AsyncMock(side_effect=_Violation("workspaces_pkey"))
        with pytest.raises(UniqueViolation):
            await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
        assert db.execute.await_count == 1


class TestAdoptingTheMachinesSandbox:
    """The repair for projects created before the insert carried the sandbox.

    The bind's own shadow arm is fenced on the previous ref, which a project
    that joined afterwards does not carry, so nothing else ever reaches it.
    """

    @pytest.mark.asyncio
    async def test_only_projects_naming_no_sandbox_are_touched(self, db):
        """A project naming a DIFFERENT one is a genuine split binding, and
        resolving that by overwriting is the guess the fence exists to stop."""
        db.fetchall = AsyncMock(return_value=[{"workspace_id": WORKSPACE_ID}])

        repaired = await W.adopt_computer_sandbox_into_workspaces(COMPUTER_ID)

        assert repaired == [WORKSPACE_ID]
        sql = _sql(db)
        assert "w.sandbox_id IS NULL" in sql
        assert "SET sandbox_id = comp.provider_ref" in sql

    @pytest.mark.asyncio
    async def test_it_reaches_every_project_on_the_machine(self, db):
        """One read for the whole machine: the sibling that is about to take a
        turn is in the same state and should not need its own repair."""
        db.fetchall = AsyncMock(return_value=[])
        await W.adopt_computer_sandbox_into_workspaces(COMPUTER_ID)
        assert "w.computer_id = comp.computer_id" in _sql(db)
        assert "w.workspace_id = " not in _sql(db)

    @pytest.mark.asyncio
    async def test_a_machine_with_no_sandbox_of_its_own_writes_nothing(self, db):
        """There is no answer to copy yet, and writing NULL over NULL would
        still flip the status columns this statement carries."""
        db.fetchall = AsyncMock(return_value=[])
        await W.adopt_computer_sandbox_into_workspaces(COMPUTER_ID)
        sql = _sql(db)
        assert "provider_ref IS NOT NULL" in sql
        assert "status = 'running'" in sql

    @pytest.mark.asyncio
    async def test_an_unparseable_machine_writes_nothing(self, db):
        assert await W.adopt_computer_sandbox_into_workspaces("not-a-uuid") == []
        db.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_row_already_naming_this_sandbox_has_its_status_realigned(
        self, db
    ):
        """The second arm, and the reason the idle sweep used to wedge: a row
        whose sandbox_id already equals the machine's provider_ref is the same
        machine, so a 'stopped' status on it is a lag to repair rather than a
        disagreement to respect."""
        db.fetchall = AsyncMock(return_value=[])
        await W.adopt_computer_sandbox_into_workspaces(COMPUTER_ID)
        sql = _sql(db)
        assert (
            "(w.sandbox_id IS NULL OR w.sandbox_id = comp.provider_ref)" in sql
        )
        assert "SET sandbox_id = comp.provider_ref, status = comp.status" in sql

    @pytest.mark.asyncio
    async def test_rows_that_already_agree_are_not_rewritten(self, db):
        """Every cold attach runs this, so a no-op has to return no rows and
        log nothing rather than bump updated_at across the whole machine."""
        db.fetchall = AsyncMock(return_value=[])
        await W.adopt_computer_sandbox_into_workspaces(COMPUTER_ID)
        sql = _sql(db)
        assert "w.sandbox_id IS DISTINCT FROM comp.provider_ref" in sql
        assert "w.status <> comp.status" in sql

    @pytest.mark.asyncio
    async def test_flash_is_out_of_reach_of_the_repair(self, db):
        """A flash workspace has no machine; handing it one of these shadows
        would invent a sandbox it never asked for."""
        db.fetchall = AsyncMock(return_value=[])
        await W.adopt_computer_sandbox_into_workspaces(COMPUTER_ID)
        assert "w.status NOT IN ('deleted', 'flash')" in _sql(db)
