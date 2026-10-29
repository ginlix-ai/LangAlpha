"""Home in an automation's file: "home" in workspace_id, both ways. A row in
Home reads "home"; a written "home" is Home's id, which an analyst in a
workspace never sees."""

from __future__ import annotations

from uuid import UUID

import pytest

from src.server.services.automations import lifecycle
from tests.unit.server.services.automations.file._support import (
    BRIEF,
    CREATED,
    FILE_NAME,
    HOME,
    NEW,
    NEW_PATH,
    PINNED,
    THREAD,
    WORKSPACE,
    _create,
    _refusal,
    _row,
    _shown,
    _write,
)

_NOT_THE_KEYWORD = ["HOME", "Home", " home", "all_workspaces", "workspace"]


class TestRead:
    def test_a_row_in_home_reads_home(self):
        assert _shown(_row())["workspace_id"] == "home"

    def test_a_row_in_a_workspace_reads_its_id(self):
        assert _shown(_row(workspace_id=UUID(WORKSPACE)))["workspace_id"] == WORKSPACE


class TestCreate:
    @pytest.mark.parametrize("written", ["home", HOME], ids=["keyword", "homes-id"])
    @pytest.mark.asyncio
    async def test_home_reaches_the_lifecycle_as_homes_id(self, db, backend, written):
        await _create(backend, {**NEW, "workspace_id": written})

        data = lifecycle.create_automation.await_args.args[1]
        assert data.workspace_id == UUID(HOME)
        row = db.rows[CREATED]
        assert row["workspace_id"] == UUID(HOME)
        assert _shown(row)["workspace_id"] == "home"

    @pytest.mark.parametrize("written", _NOT_THE_KEYWORD)
    @pytest.mark.asyncio
    async def test_another_string_is_refused_as_no_uuid(self, db, backend, written):
        error = await _refusal(backend, db, {**NEW, "workspace_id": written}, path=NEW_PATH)

        [(path, message)] = error.problems
        assert path == "workspace_id"
        assert message.startswith("Input should be a valid UUID")


class TestUpdate:
    @pytest.mark.asyncio
    async def test_writing_back_a_home_automation_as_read_changes_nothing(self, db, backend):
        db.add(_row())
        entry = _shown(db.list()[0])

        report = await _write(backend, entry)

        assert report.startswith("No changes")
        lifecycle.update_automation.assert_not_awaited()
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_home_moves_a_workspace_automation_to_home(self, db, backend):
        db.add(_row(workspace_id=UUID(WORKSPACE)))
        entry = _shown(db.list()[0])

        report = await _write(backend, {**entry, "workspace_id": "home"})

        assert lifecycle.update_automation.await_args.args[2] == {"workspace_id": UUID(HOME)}
        assert db.rows[BRIEF]["workspace_id"] == UUID(HOME)
        assert ": workspace_id" in report

    @pytest.mark.asyncio
    async def test_a_workspace_id_moves_a_home_automation_there(self, db, backend):
        db.add(_row())
        entry = _shown(db.list()[0])

        await _write(backend, {**entry, "workspace_id": WORKSPACE})

        assert db.rows[BRIEF]["workspace_id"] == UUID(WORKSPACE)

    @pytest.mark.asyncio
    async def test_the_former_agent_mode_is_refused_as_an_unknown_field(self, db, backend):
        """An older note may still name it to move an automation to Home.
        Dropped silently, the save would report success with the move undone."""
        db.add(_row(workspace_id=UUID(WORKSPACE)))
        entry = _shown(db.list()[0])

        error = await _refusal(backend, db, {**entry, "agent_mode": "all_workspaces"})

        [(path, message)] = error.problems
        assert path == "agent_mode"
        assert message.startswith("unknown field")

    @pytest.mark.parametrize("written", _NOT_THE_KEYWORD)
    @pytest.mark.asyncio
    async def test_another_string_is_refused_as_no_uuid(self, db, backend, written):
        db.add(_row())
        entry = _shown(db.list()[0])

        error = await _refusal(backend, db, {**entry, "workspace_id": written})

        [(path, message)] = error.problems
        assert path == "workspace_id"
        assert message.startswith("Input should be a valid UUID")


class TestMoveUnpinsTheThread:
    """A run continues its pinned thread only in the automation's workspace,
    so a move that names no thread unpins it (``lifecycle.update_automation``)."""

    @pytest.mark.asyncio
    async def test_a_moved_automation_continuing_a_conversation_reports_the_thread(self, db, backend):
        db.add(_row(workspace_id=UUID(WORKSPACE), thread_strategy="continue", conversation_thread_id=UUID(PINNED)))
        entry = _shown(db.list()[0])
        assert entry["thread"] == PINNED

        report = await _write(backend, {**entry, "workspace_id": "home"})

        assert db.rows[BRIEF]["conversation_thread_id"] is None
        assert ": workspace_id, thread" in report
        assert _shown(db.list()[0])["thread"] == "persistent"

    @pytest.mark.asyncio
    async def test_a_moved_automation_in_its_own_thread_still_reads_persistent(self, db, backend):
        db.add(_row(thread_strategy="continue", conversation_thread_id=UUID(PINNED), owns_thread=True))
        entry = _shown(db.list()[0])
        assert entry["thread"] == "persistent"

        report = await _write(backend, {**entry, "workspace_id": WORKSPACE})

        assert db.rows[BRIEF]["conversation_thread_id"] is None
        assert report.startswith(f'Saved {FILE_NAME}: updated "Morning brief": workspace_id;')

    @pytest.mark.asyncio
    async def test_another_edit_keeps_the_pin(self, db, backend):
        db.add(_row(thread_strategy="continue", conversation_thread_id=UUID(PINNED)))
        entry = _shown(db.list()[0])

        await _write(backend, {**entry, "instruction": "Summarize the news."})

        assert db.rows[BRIEF]["conversation_thread_id"] == UUID(PINNED)


class TestAPinIsWhereItRuns:
    """A run continues its pinned thread only in the automation's workspace,
    so a pin outside it is refused (``lifecycle.create_automation``)."""

    @pytest.mark.asyncio
    async def test_home_with_this_conversation_from_a_workspace_is_refused(self, db, backend):
        error = await _refusal(backend, db, {**NEW, "workspace_id": "home", "thread": "current"}, path=NEW_PATH)

        assert error.problems == [
            (
                "thread",
                f"thread {THREAD} is in workspace {WORKSPACE}, but the automation runs in the user's Home: "
                f"pin a thread from the user's Home, or run the automation in workspace {WORKSPACE}",
            )
        ]

    @pytest.mark.asyncio
    async def test_a_move_that_pins_a_thread_it_leaves_is_refused(self, db, backend):
        db.add(_row())
        entry = _shown(db.list()[0])

        error = await _refusal(backend, db, {**entry, "workspace_id": WORKSPACE, "thread": PINNED})

        assert [field for field, _ in error.problems] == ["thread"]

    @pytest.mark.asyncio
    async def test_an_edit_that_keeps_the_pin_checks_nothing(self, db, backend):
        db.add(_row(workspace_id=UUID(WORKSPACE), thread_strategy="continue", conversation_thread_id=UUID(PINNED)))
        entry = _shown(db.list()[0])

        await _write(backend, {**entry, "instruction": "Summarize the news."})

        assert db.rows[BRIEF]["instruction"] == "Summarize the news."
        assert db.rows[BRIEF]["conversation_thread_id"] == UUID(PINNED)
