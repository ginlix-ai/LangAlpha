"""The save as a whole: one transaction, the version checks, what a file
name already taken does, and the report."""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from psycopg.errors import CheckViolation, StringDataRightTruncation

from ptc_agent.agent.backends.db_json_route import UserDataValidationError
from src.server.services.automations import lifecycle
from tests.unit.server.services.automations.file._support import (
    BRIEF,
    BRIEF_FILE,
    CALL,
    CREATED,
    FOREIGN_WORKSPACE,
    HOME,
    NEW,
    NEW_FILE_NAME,
    NEW_PATH,
    NEXT_RUN,
    NEXT_RUN_LOCAL,
    PASSED,
    PATH,
    UNKNOWN_THREAD,
    USER,
    WORKSPACE,
    _create,
    _once_row,
    _other_row,
    _refusal,
    _row,
    _shown,
    _write,
)


class TestReport:
    @pytest.mark.asyncio
    async def test_a_create_names_its_file_and_when_it_runs(self, db, backend):
        report = await _create(backend, NEW)

        assert report == f'Saved new.json: created "A"; next run {NEXT_RUN_LOCAL}'
        assert db.rows[CREATED]["file_name"] == NEW_FILE_NAME

    @pytest.mark.asyncio
    async def test_an_update_names_its_file_and_the_fields_it_changed(self, db, backend):
        db.add(_row())

        report = await _write(backend, {**_shown(_row()), "instruction": "Summarize the news.", "max_failures": 5})

        assert report == (
            f'Saved morning-brief.json: updated "Morning brief": instruction, max_failures; next run {NEXT_RUN_LOCAL}'
        )


class TestTransaction:
    @pytest.mark.asyncio
    async def test_each_change_runs_in_the_saves_transaction_under_its_own_savepoint(self, db, backend):
        db.add(_row())

        await _write(backend, {**_shown(_row()), "instruction": "Summarize the news.", "status": "paused"})
        await _create(backend, NEW)

        assert [(w.op, w.depth) for w in db.writes] == [("update", 2), ("update", 2), ("create", 2)]
        assert all(w.joined for w in db.writes)

    @pytest.mark.asyncio
    async def test_the_file_as_read_is_no_change(self, db, backend):
        db.add(_row(last_execution={"status": "completed", "completed_at": NEXT_RUN}))
        served = await backend.aread_range(PATH)

        result = await backend.awrite_text(PATH, served)

        assert result["message"] == "No changes: morning-brief.json already matches the saved automation"
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_several_problems_refuse_the_whole_write_at_once(self, db, backend):
        """One retry can fix everything."""
        db.add(_row())

        error = await _refusal(
            backend, db, {**_shown(_row()), "instruction": None, "colour": "red", "timezone": "EST"}
        )

        assert error.message.splitlines()[0] == "schema_error:morning-brief.json: 3 problems, nothing was saved."
        assert [path for path, _ in error.problems] == ["timezone", "instruction", "colour"]
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_a_change_the_lifecycle_refuses_rolls_the_whole_save_back(self, db, backend):
        """The lifecycle's rules run under a savepoint, so a refusal is
        reported rather than raised mid-save, and nothing is kept."""
        db.add(_once_row(status="paused", next_run_at=PASSED))

        # Its resume would be refused too, since its time has passed.
        error = await _refusal(backend, db, {"workspace_id": FOREIGN_WORKSPACE, "status": "active"})

        assert error.problems == [("workspace_id", "belongs to another user")]
        lifecycle.resume_automation.assert_not_awaited()
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_a_create_the_lifecycle_refuses_is_reported_by_field(self, db, backend):
        error = await _refusal(backend, db, {**NEW, "thread": UNKNOWN_THREAD}, path=NEW_PATH)

        assert error.problems == [("thread", "not found")]


class TestVersion:
    @pytest.mark.asyncio
    async def test_a_write_over_a_stale_read_is_a_conflict(self, db, backend):
        db.add(_row())
        await backend.aread_range(PATH)
        db.rows[BRIEF]["instruction"] = "Changed on the Automations page."

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PATH, json.dumps({**_shown(_row()), "name": "Renamed"}))

        assert exc.value.error_type == "version_conflict"
        assert db.writes == []
        assert db.rows[BRIEF]["name"] == "Morning brief"

    @pytest.mark.asyncio
    async def test_an_edit_that_lands_while_the_save_waits_for_the_lock_is_a_conflict(self, db, backend):
        """The version is checked on the row read under the lock: an edit on
        the Automations page that commits while the save waits would
        otherwise be overwritten."""
        db.add(_row())
        await backend.aread_range(PATH)
        db.on_lock = lambda: db.rows[BRIEF].update(instruction="Changed on the Automations page.")

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PATH, json.dumps({**_shown(_row()), "name": "Renamed"}))

        assert exc.value.error_type == "version_conflict"
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_an_edit_that_lands_before_the_save_locks_its_row_is_a_conflict(self, db, backend):
        """The row is locked only once the save has changes to make, so the
        version is checked again on it as locked: an edit that commits in
        between would otherwise be overwritten."""
        db.add(_row())
        await backend.aread_range(PATH)
        db.on_row_lock = lambda: db.rows[BRIEF].update(instruction="Changed on the Automations page.")

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PATH, json.dumps({**_shown(_row()), "name": "Renamed"}))

        assert exc.value.error_type == "version_conflict"
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_a_save_that_changes_nothing_locks_no_row(self, db, backend):
        db.add(_row())

        await _write(backend, {**_shown(_row()), "state": {"failure_count": 5}})

        assert not any("FOR UPDATE" in sql for sql in db.sql)

    @pytest.mark.asyncio
    async def test_saves_of_two_automations_never_conflict(self, db, backend):
        """Each file has its own version: a save of one leaves the other's
        Read standing."""
        db.add(_row(), _other_row())
        await backend.aread_range(PATH)
        await backend.aread_range(f"{PATH.rsplit('/', 1)[0]}/evening-wrap.json")

        await backend.awrite_text(PATH, json.dumps({"instruction": "Summarize the news."}))
        await backend.awrite_text(
            f"{PATH.rsplit('/', 1)[0]}/evening-wrap.json", json.dumps({"instruction": "Wrap the day."})
        )

        assert [r["instruction"] for r in db.list()] == ["Wrap the day.", "Summarize the news."]


class TestTakenName:
    """Writing a file that wasn't read only creates: a name some automation
    already has is refused, never written over."""

    @pytest.mark.asyncio
    async def test_a_write_without_a_read_onto_an_automations_file_is_refused(self, db, backend):
        db.add(_row())

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PATH, json.dumps(NEW))

        assert exc.value.error_type == "exists"
        assert f"To change it, Read({PATH}) first" in exc.value.hint
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_a_create_where_one_landed_while_it_waited_is_refused(self, db, backend):
        db.on_lock = lambda: db.add(_row(file_name=NEW_FILE_NAME))

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(NEW_PATH, json.dumps(NEW))

        assert exc.value.error_type == "exists"
        lifecycle.create_automation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_name_taken_outside_the_users_lock_is_refused_at_commit(self, db, backend):
        """The Automations page names a create without the lock; the unique
        index refuses the second, which the save answers as a taken name."""
        db.add(_row(file_name=NEW_FILE_NAME))
        db.get_automation_file = AsyncMock(return_value=None)

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(NEW_PATH, json.dumps(NEW))

        assert exc.value.error_type == "exists"
        assert "took this file name meanwhile" in exc.value.hint
        assert CREATED not in db.rows

    @pytest.mark.asyncio
    async def test_a_value_the_column_cannot_hold_is_the_contents_problem_not_an_outage(self, db, backend):
        lifecycle.create_automation.side_effect = StringDataRightTruncation("value too long")

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(NEW_PATH, json.dumps(NEW))

        assert exc.value.error_type == "schema_error"
        assert "too long" in exc.value.hint
        assert CREATED not in db.rows

    @pytest.mark.asyncio
    async def test_a_value_a_constraint_refuses_is_refused_without_quoting_the_row(self, db, backend, caplog):
        refused = CheckViolation("Failing row contains (Sell all TSLA)")
        lifecycle.create_automation.side_effect = refused

        with caplog.at_level(logging.WARNING), pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(NEW_PATH, json.dumps(NEW))

        assert exc.value.error_type == "schema_error"
        assert "(CheckViolation)" in exc.value.hint
        assert "Sell all TSLA" not in exc.value.message
        assert CREATED not in db.rows
        (logged,) = [r for r in caplog.records if "refused by a constraint" in r.getMessage()]
        assert f"CheckViolation sqlstate={refused.sqlstate}" in logged.getMessage()
        assert "Sell all TSLA" not in caplog.text


class TestServedState:
    """``state`` is checked against what the writer was shown: the Read's
    file, or the live row for a save through the file mount."""

    @pytest.mark.asyncio
    async def test_without_a_served_file_state_is_checked_against_the_live_row(self):
        row = _row(failure_count=2)
        content, _ = BRIEF_FILE.render(row)
        edited = json.loads(content)
        edited["state"]["failure_count"] = 0

        unchanged = BRIEF_FILE.plan(CALL, await BRIEF_FILE.parse(USER, CALL, content, None), row).changes
        noted = BRIEF_FILE.plan(CALL, await BRIEF_FILE.parse(USER, CALL, json.dumps(edited), None), row).changes

        assert (bool(unchanged), unchanged.notes) == (False, [])
        assert (bool(noted), noted.notes) == (
            False,
            ["- note: state is kept by the server, so what you wrote in it was ignored"],
        )

    @pytest.mark.asyncio
    async def test_a_save_through_the_mount_answers_with_the_file_it_left(self, db, backend):
        db.add(_row())
        content, version = await backend.aread_versioned(PATH)

        stored = await backend.awrite_versioned(
            PATH, json.dumps({**json.loads(content), "state": {"next_run_at": "2030-01-01T08:30:00-05:00"}, "name": "B"}),
            version,
        )

        assert (stored.content, stored.version) == await backend.aread_versioned(PATH)
        assert stored.report.splitlines() == [
            f'Saved morning-brief.json: updated "B": name; next run {NEXT_RUN_LOCAL}',
            "- note: state is kept by the server, so what you wrote in it was ignored",
        ]

    @pytest.mark.asyncio
    async def test_a_create_through_the_mount_goes_only_where_no_file_is(self, db, backend):
        db.add(_row())

        stored = await backend.awrite_versioned(NEW_PATH, json.dumps(NEW), None)
        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_versioned(PATH, json.dumps(NEW), None)

        assert (stored.content, stored.version) == await backend.aread_versioned(NEW_PATH)
        assert exc.value.error_type == "exists"
        assert [w.op for w in db.writes] == ["create"]


class TestDeliveryNote:
    @pytest.mark.parametrize(("url", "noted"), [("", True), ("https://hooks.example.com/x", False)])
    @pytest.mark.asyncio
    async def test_delivery_without_a_webhook_is_called_out(self, db, backend, monkeypatch, url, noted):
        monkeypatch.setattr(lifecycle, "settings", MagicMock(AUTOMATION_WEBHOOK_URL=url))
        db.add(_row())

        report = await _write(backend, {**_shown(_row()), "delivery": ["slack"]})

        assert ("Note: Delivery was saved, but AUTOMATION_WEBHOOK_URL is not configured" in report) is noted

    @pytest.mark.asyncio
    async def test_a_create_with_delivery_is_called_out_too(self, db, backend, monkeypatch):
        monkeypatch.setattr(lifecycle, "settings", MagicMock(AUTOMATION_WEBHOOK_URL=""))

        report = await _create(backend, {**NEW, "delivery": ["slack"]})

        assert report.splitlines()[1].startswith("Note: Delivery was saved")


_USER_WRITES = "hashtext(%s::text)"
_ROW_LOCK = "FOR UPDATE"


def _locks(db) -> list[str]:
    """The locks the save took, in order: the user's write lock and the row's."""
    return [
        "user" if _USER_WRITES in sql else "row"
        for sql in db.sql
        if _USER_WRITES in sql or _ROW_LOCK in sql
    ]


class TestLockOrder:
    """An update may make the user's Home, which takes the user's write lock;
    a fold of a former Home takes that lock before the automation rows it
    moves. The save takes it before its row too, or the two deadlock."""

    @pytest.mark.asyncio
    async def test_an_update_takes_the_users_write_lock_before_its_row(self, db, backend):
        db.add(_row())

        await _write(backend, {**_shown(_row()), "instruction": "Summarize the news."})

        assert _locks(db) == ["user", "row"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "change", [{"status": "paused"}, {"status": "deleted"}], ids=["pause", "delete"]
    )
    async def test_a_save_that_cannot_make_home_takes_only_its_row(self, db, backend, change):
        db.add(_row())

        await _write(backend, {**_shown(_row()), **change})

        assert _locks(db) == ["row"]

    @pytest.mark.asyncio
    async def test_home_made_in_a_save_is_made_on_its_connection_after_both_locks(
        self, db, backend, monkeypatch
    ):
        """A move to Home makes Home when the user has none yet."""
        db.add(_row(workspace_id=UUID(WORKSPACE)))
        homes: list[Any] = []

        async def no_home_yet(workspace_id: str, conn: Any = None) -> None:
            return None

        async def make_home(user_id: str, conn: Any = None) -> dict[str, Any]:
            homes.append(conn)
            db.sql.append("make home")
            return {"workspace_id": HOME, "user_id": user_id}

        monkeypatch.setattr(lifecycle, "get_workspace", no_home_yet)
        monkeypatch.setattr(lifecycle, "get_or_create_home_workspace", make_home)

        await _write(backend, {**_shown(db.list()[0]), "workspace_id": "home"})

        assert homes == [db.conn]
        made = db.sql.index("make home")
        assert _locks(db) == ["user", "row"]
        assert all(made > i for i, sql in enumerate(db.sql) if _USER_WRITES in sql or _ROW_LOCK in sql)
        assert db.rows[BRIEF]["workspace_id"] == UUID(HOME)
