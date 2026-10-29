"""Creating a project directly on a machine: one statement, one folder, one shadow.

Instant creation rests on this insert. The row has to be born bound, carrying
the machine's lifecycle fields, because everything downstream reads the project
row and would otherwise see a project that exists on no computer for a window.
The folder is the workspace's name, and names are unique per user, so a folder
another row still holds (a tombstone awaiting cleanup, a sibling renamed away)
is a wait, not a refusal: the workspace takes a placeholder until it frees.
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
from src.server.database.sql_fences import FENCE_TAKES_WORKSPACES

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
    """The insert's own statements only: the MCP selection a new workspace
    starts with is pinned against a real Postgres in
    tests/integration/test_new_workspace_mcp_selection_db.py."""
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    @asynccontextmanager
    async def _transaction_cm():
        yield

    conn.cursor = _cursor_cm
    conn.transaction = _transaction_cm

    @asynccontextmanager
    async def _transaction():
        yield

    conn.transaction = _transaction

    @asynccontextmanager
    async def _fake(passed=None):
        yield passed if passed is not None else conn

    with (
        patch("src.server.database.workspace.get_db_connection", new=_fake),
        patch("src.server.database.workspace_folders.get_db_connection", new=_fake),
        patch.object(W, "get_workspace_dir_names_for_computer", AsyncMock(return_value=())),
        patch.object(W, "get_computer", AsyncMock(return_value={"provider_ref": "sbx", "layout_version": 4})),
        patch("src.server.database.workspace.lock_user_writes", new=AsyncMock()),
        patch(
            "src.server.database.workspace.start_new_workspace_selection",
            new=AsyncMock(),
        ),
    ):
        yield cursor


def _sql(cursor, call=0) -> str:
    return re.sub(r"\s+", " ", cursor.execute.call_args_list[call][0][0])


def _params(cursor, call=0) -> dict:
    return cursor.execute.call_args_list[call][0][1]


class TestTheStatement:
    @pytest.mark.asyncio
    async def test_the_project_is_born_on_the_machine(self, db):
        """Never a window where the row exists unbound: no second write to lose."""
        await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
        inserts = [
            i for i in range(db.execute.await_count) if "INSERT INTO workspaces" in _sql(db, i)
        ]
        assert inserts == [0]
        assert "comp.computer_id" in _sql(db)

    @pytest.mark.asyncio
    async def test_the_folder_it_takes_stops_being_a_siblings_former_folder(self, db):
        """Every reader folds a former folder into its workspace, so a sibling
        that once lived at this name would take this workspace's paths."""
        # Casefold, as the readers fold: "Reſearch" names this folder too, and
        # Postgres's lower() leaves the long s alone.
        db.fetchall = AsyncMock(return_value=[
            {"folder": "RESEARCH"}, {"folder": "Reſearch"}, {"folder": "Macro"},
        ])
        await W.create_workspace_on_computer(
            "user-1", "Research", COMPUTER_ID, workspace_id=WORKSPACE_ID
        )
        assert "unnest(previous_dir_names)" in _sql(db, 1)
        assert _params(db, 1) == {"id": WORKSPACE_ID, "computer": COMPUTER_ID}
        assert "previous_dir_names &&" in _sql(db, 2)
        assert _params(db, 2) == {
            "id": WORKSPACE_ID, "computer": COMPUTER_ID, "spellings": ["RESEARCH", "Reſearch"],
        }

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
    async def test_a_computer_being_retired_creates_nothing(self, db):
        """Its retirement read it empty, and a workspace landing after the
        marker would go down with the tombstone. The fence sits in the share
        locked read, which the marker's write waits on or is seen by."""
        await W.create_workspace_on_computer("user-1", "Research", COMPUTER_ID)
        sql = _sql(db)
        assert FENCE_TAKES_WORKSPACES in sql
        assert sql.index(FENCE_TAKES_WORKSPACES) < sql.index("FOR SHARE")

    @pytest.mark.asyncio
    async def test_a_bind_onto_a_computer_being_retired_binds_nothing(self, db):
        await W.bind_workspace_to_computer(
            WORKSPACE_ID, COMPUTER_ID, expected_computer_id=None
        )
        sql = _sql(db)
        assert FENCE_TAKES_WORKSPACES in sql
        assert sql.index(FENCE_TAKES_WORKSPACES) < sql.index("FOR SHARE")

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
    @pytest.mark.asyncio
    async def test_the_folder_is_the_name(self, db):
        """The agent and the user see one name for the workspace, so the folder
        is spelled the way the user typed it, spaces and case kept."""
        await W.create_workspace_on_computer("user-1", "Q3 Earnings", COMPUTER_ID)
        assert _params(db)["dir_name"] == "Q3 Earnings"

    @pytest.mark.asyncio
    async def test_the_name_key_is_the_folded_folder(self, db):
        """What the per-user index compares: a case-insensitive disk cannot
        hold Research and research side by side."""
        await W.create_workspace_on_computer("user-1", "  Q3   Earnings ", COMPUTER_ID)
        params = _params(db)
        assert params["name"] == "Q3   Earnings"
        assert params["name_key"] == "q3 earnings"

    @pytest.mark.asyncio
    async def test_an_unusable_name_writes_nothing(self, db):
        with pytest.raises(W.WorkspaceNameInvalid):
            await W.create_workspace_on_computer("user-1", " ./ ", COMPUTER_ID)
        with pytest.raises(W.WorkspaceNameInvalid):
            await W.create_workspace_on_computer("user-1", "x" * 81, COMPUTER_ID)
        db.execute.assert_not_awaited()


    @pytest.mark.asyncio
    async def test_a_sandbox_not_yet_on_the_folder_layout_gets_a_placeholder(self, db):
        """Its origin's files are still at the root, and the move to the folder
        layout skips every root entry a row names: a workspace named "data"
        would take the origin's data folder."""
        with patch.object(W, "get_computer", AsyncMock(return_value={"provider_ref": "sbx", "layout_version": 0})):
            await W.create_workspace_on_computer("user-1", "data", COMPUTER_ID, workspace_id=WORKSPACE_ID)
        digest = hashlib.md5(WORKSPACE_ID.encode()).hexdigest()[:4]
        assert _params(db)["dir_name"] == f"data-{digest}"

    @pytest.mark.asyncio
    async def test_a_computer_with_no_sandbox_yet_takes_the_name(self, db):
        """Its first sandbox starts on the folder layout."""
        with patch.object(W, "get_computer", AsyncMock(return_value={"provider_ref": None, "layout_version": 0})):
            await W.create_workspace_on_computer("user-1", "data", COMPUTER_ID)
        assert _params(db)["dir_name"] == "data"


class TestTheCollision:
    @pytest.mark.asyncio
    async def test_a_held_folder_places_the_workspace_until_it_frees(self, db):
        """A deleted workspace awaiting cleanup, or a sibling renamed away that
        has not moved yet, still holds the folder; the create must not fail on
        it. The placeholder is the name plus a suffix the next settle drops."""
        db.execute = AsyncMock(
            side_effect=[_Violation(W._COMPUTER_DIR_INDEX), None, None, None]
        )
        result = await W.create_workspace_on_computer(
            "user-1", "Research", COMPUTER_ID, workspace_id=WORKSPACE_ID
        )
        assert result is not None
        first, second = _params(db, 0)["dir_name"], _params(db, 1)["dir_name"]
        assert first == "Research"
        assert second == "Research-" + hashlib.md5(WORKSPACE_ID.encode()).hexdigest()[:4]

    @pytest.mark.asyncio
    async def test_a_folder_held_under_another_case_is_held(self, db):
        """The folder index compares case, but a case-insensitive disk (a Docker
        work dir bind-mounted from macOS) keeps "research" and "Research" as
        one folder, so a tombstone's "research" would share its files."""
        with patch.object(
            W, "get_workspace_dir_names_for_computer", AsyncMock(return_value=("research",))
        ):
            await W.create_workspace_on_computer(
                "user-1", "Research", COMPUTER_ID, workspace_id=WORKSPACE_ID
            )
        inserts = [
            i for i in range(db.execute.await_count) if "INSERT INTO workspaces" in _sql(db, i)
        ]
        assert inserts == [0]
        assert _params(db)["dir_name"] == (
            "Research-" + hashlib.md5(WORKSPACE_ID.encode()).hexdigest()[:4]
        )

    @pytest.mark.asyncio
    async def test_every_attempt_colliding_reaches_the_caller(self, db):
        db.execute = AsyncMock(side_effect=_Violation(W._COMPUTER_DIR_INDEX))
        with pytest.raises(W.WorkspaceDirNameTaken) as caught:
            await W.create_workspace_on_computer(
                "user-1", "Research", COMPUTER_ID, workspace_id=WORKSPACE_ID
            )
        assert caught.value.computer_id == COMPUTER_ID
        assert db.execute.await_count == 4

    @pytest.mark.asyncio
    async def test_a_taken_name_names_the_workspace_holding_it(self, db):
        """The client offers the holder, so the refusal carries its id and the
        spelling it was saved under, not the one just typed."""
        db.execute = AsyncMock(side_effect=_Violation(W._USER_NAME_INDEX))
        holder = {"workspace_id": COMPUTER_ID, "name": "Research"}
        with patch.object(
            W, "find_workspace_by_name_key", AsyncMock(return_value=holder)
        ) as find:
            with pytest.raises(W.WorkspaceNameTaken) as caught:
                await W.create_workspace_on_computer("user-1", "RESEARCH", COMPUTER_ID)
        find.assert_awaited_once_with("user-1", "research")
        assert (caught.value.name, caught.value.workspace_id) == ("Research", COMPUTER_ID)
        assert db.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_another_unique_violation_is_not_retried(self, db):
        """Another folder can never clear a primary-key clash, so retrying one
        would burn every attempt and then report the wrong cause."""
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
    async def test_a_repair_leaves_the_gallery_order_alone(self, db):
        """The gallery orders by updated_at, and this reaches every sibling on
        the machine; stamping them would give them all one moment."""
        db.fetchall = AsyncMock(return_value=[])
        await W.adopt_computer_sandbox_into_workspaces(COMPUTER_ID)
        assert "updated_at" not in _sql(db)

    @pytest.mark.asyncio
    async def test_an_unbound_home_is_out_of_reach_of_the_repair(self, db):
        """An unbound Home has no machine; handing it one of these shadows
        would invent a sandbox it never asked for."""
        db.fetchall = AsyncMock(return_value=[])
        await W.adopt_computer_sandbox_into_workspaces(COMPUTER_ID)
        assert "w.status NOT IN ('deleted', 'unbound')" in _sql(db)


class TestDuplicateOwesItsRestore:
    @pytest.mark.asyncio
    async def test_the_copy_owes_its_files_in_the_transaction_that_publishes_it(
        self, db
    ):
        """On a machine its owner brought, a folder without its files is
        otherwise offered to the owner, who never removed anything from it."""
        events = []
        created = {"workspace_id": "ws-copy"}
        with (
            patch.object(
                W,
                "create_workspace_on_computer",
                AsyncMock(side_effect=lambda *_a, **_k: events.append("create") or created),
            ),
            patch(
                "src.server.database.workspace_file.copy_workspace_files",
                AsyncMock(side_effect=lambda *_a, **_k: events.append("copy")),
            ),
            patch.object(
                W,
                "set_files_restore_incomplete",
                AsyncMock(side_effect=lambda *_a, **_k: events.append("owed") or True),
            ) as owed,
        ):
            assert await W.duplicate_workspace_on_computer(
                "ws-source", "user-1", "Copy", COMPUTER_ID
            ) is created

        assert events == ["create", "copy", "owed"]
        assert owed.await_args.args == ("ws-copy", True)
        assert owed.await_args.kwargs["conn"] is not None


class TestRestoreOfferSettle:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("restore", [True, False], ids=["restore", "keep"])
    async def test_either_answer_is_a_new_arrival_of_the_files(self, db, restore):
        """Every worker that attached the folder while the offer stood attaches
        it again, laying the restore owed or queuing the bring-up held back,
        rather than only the worker that took the answer."""
        db.rowcount = 1
        assert await W.settle_files_restore_offer("ws-1", restore=restore)

        sql = _sql(db)
        assert "files_epoch = gen_random_uuid()" in sql
        assert ("files_restore_incomplete_at" in sql) is restore
        assert "files_restore_offered_at IS NOT NULL" in sql
