"""Deleting an automation, by ``"status": "deleted"`` or by removing its file,
and renaming its file, which keeps the automation."""

from __future__ import annotations

import json

import pytest

from ptc_agent.agent.backends import ReadOnlyStoreError
from ptc_agent.agent.backends.db_json_route import UserDataValidationError
from tests.unit.server.services.automations.file._support import (
    BRIEF,
    OTHER_FILE_NAME,
    OTHER_PATH,
    PATH,
    ROOT,
    _other_row,
    _row,
    _write,
)

DELETED = 'Deleted "Morning brief" (morning-brief.json), with its run history'


class TestDeleteByStatus:
    @pytest.mark.parametrize("status", ["active", "paused", "completed", "disabled", "executing"])
    @pytest.mark.asyncio
    async def test_status_deleted_deletes_the_automation_from_any_status(self, db, backend, status):
        db.add(_row(status=status), _other_row())

        report = await _write(backend, {"status": "deleted"})

        assert report == DELETED
        assert [r["file_name"] for r in db.list()] == [OTHER_FILE_NAME]
        assert [(w.op, w.automation_id, w.depth) for w in db.writes] == [("delete", BRIEF, 1)]

    @pytest.mark.asyncio
    async def test_a_delete_is_checked_against_the_row_as_locked(self, db, backend):
        """An edit that lands after the Read must not be deleted unseen."""
        db.add(_row())
        await backend.aread_range(PATH)
        db.on_row_lock = lambda: db.rows[BRIEF].update(instruction="Changed on the Automations page.")

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PATH, json.dumps({"status": "deleted"}))

        assert exc.value.error_type == "version_conflict"
        assert BRIEF in db.rows

    @pytest.mark.asyncio
    async def test_through_the_mount_the_file_is_gone_after(self, db, backend):
        db.add(_row())
        _, version = await backend.aread_versioned(PATH)

        stored = await backend.awrite_versioned(PATH, json.dumps({"status": "deleted"}), version)

        assert (stored.report, stored.content, stored.version) == (DELETED, None, None)
        assert await backend.aread_versioned(PATH) is None


class TestRemove:
    @pytest.mark.asyncio
    async def test_removing_the_file_deletes_the_automation_and_reports_it(self, db, backend):
        db.add(_row(), _other_row())

        stored = await backend.adelete_versioned(PATH)

        assert stored.report == DELETED
        assert [r["file_name"] for r in db.list()] == [OTHER_FILE_NAME]
        assert [(w.op, w.automation_id, w.joined) for w in db.writes] == [("delete", BRIEF, True)]

    @pytest.mark.asyncio
    async def test_removing_a_missing_file_finds_nothing(self, db, backend):
        assert await backend.adelete_versioned(PATH) is None
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_the_readme_stays(self, db, backend):
        with pytest.raises(ReadOnlyStoreError):
            await backend.adelete_versioned(f"{ROOT}/README.md")


class TestRename:
    @pytest.mark.asyncio
    async def test_a_rename_refiles_the_same_automation(self, db, backend):
        db.add(_row())

        report = await backend.arename_versioned(PATH, f"{ROOT}/brief.json")

        assert report == "Renamed morning-brief.json to brief.json"
        assert (db.rows[BRIEF]["file_name"], db.rows[BRIEF]["name"]) == ("brief.json", "Morning brief")
        assert [(w.op, w.automation_id) for w in db.writes] == [("rename", BRIEF)]
        assert await backend.aread_versioned(PATH) is None

    @pytest.mark.asyncio
    async def test_a_rename_onto_another_automations_file_is_refused(self, db, backend):
        """The usual source is a helper's temporary copy, which its save made
        into an automation of its own: say so, and how to finish the edit."""
        db.add(_row(), _other_row())

        with pytest.raises(UserDataValidationError) as exc:
            await backend.arename_versioned(PATH, OTHER_PATH)

        assert exc.value.error_type == "exists"
        assert exc.value.hint == (
            f"{OTHER_PATH} already exists, and morning-brief.json is an automation of its own. "
            f"To replace {OTHER_FILE_NAME}, write morning-brief.json's content into {OTHER_FILE_NAME} "
            "and rm morning-brief.json; to keep both, move morning-brief.json to another name."
        )
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_a_rename_that_loses_to_a_delete_on_the_page_finds_nothing(self, db, backend):
        """The page deletes without the user's lock, so the rename holds the
        row itself: once it is gone, nothing moved."""
        db.add(_row())
        db.on_row_lock = lambda: db.rows.pop(BRIEF)

        assert await backend.arename_versioned(PATH, f"{ROOT}/brief.json") is None
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_a_rename_that_moves_no_row_finds_nothing(self, db, backend):
        db.add(_row())
        moved = db.rename_automation_file

        async def deleted_first(automation_id, user_id, file_name, *, conn):
            db.rows.pop(BRIEF)
            return await moved(automation_id, user_id, file_name, conn=conn)

        db.rename_automation_file = deleted_first

        assert await backend.arename_versioned(PATH, f"{ROOT}/brief.json") is None

    @pytest.mark.asyncio
    async def test_a_name_taken_outside_the_users_lock_is_refused_at_commit(self, db, backend):
        db.add(_row(), _other_row())
        seen = db.get_automation_file

        async def blind_to_the_target(user_id, file_name, **kwargs):
            return None if file_name == OTHER_FILE_NAME else await seen(user_id, file_name, **kwargs)

        db.get_automation_file = blind_to_the_target

        with pytest.raises(UserDataValidationError) as exc:
            await backend.arename_versioned(PATH, OTHER_PATH)

        assert exc.value.error_type == "exists"
        assert db.rows[BRIEF]["file_name"] == "morning-brief.json"

    @pytest.mark.parametrize("to", ["brief.json.bak", "brief.txt", ".brief.json"])
    @pytest.mark.asyncio
    async def test_a_rename_to_a_name_no_automation_may_take_is_refused(self, db, backend, to):
        db.add(_row())

        with pytest.raises(UserDataValidationError) as exc:
            await backend.arename_versioned(PATH, f"{ROOT}/{to}")

        assert "1 to 64 letters, digits, - or _" in exc.value.hint
        assert db.rows[BRIEF]["file_name"] == "morning-brief.json"

    @pytest.mark.asyncio
    async def test_the_readme_is_neither_moved_nor_written_over(self, db, backend):
        db.add(_row())

        with pytest.raises(ReadOnlyStoreError):
            await backend.arename_versioned(f"{ROOT}/README.md", f"{ROOT}/notes.json")
        with pytest.raises(UserDataValidationError) as exc:
            await backend.arename_versioned(PATH, f"{ROOT}/README.md")

        assert exc.value.error_type == "exists"

    @pytest.mark.asyncio
    async def test_a_rename_of_a_missing_file_finds_nothing(self, db, backend):
        assert await backend.arename_versioned(PATH, f"{ROOT}/brief.json") is None
