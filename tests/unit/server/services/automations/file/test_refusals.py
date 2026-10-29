"""Values that would run differently than the agent meant, refused with the fix."""

from __future__ import annotations

import json

import pytest

from ptc_agent.agent.backends.db_json_route import UserDataValidationError
from src.server.services.automations import lifecycle
from tests.unit.server.services.automations.file._support import (
    BRIEF,
    CREATED,
    NEW,
    NEW_PATH,
    NEXT_RUN,
    PASSED,
    PATH,
    ROOT,
    _create,
    _once_row,
    _refusal,
    _row,
    _shown,
    _write,
)


class TestMisreadValues:
    """Values that parse but would run differently than the agent meant are
    refused with the fix, since a saved report reads as success."""

    @pytest.mark.parametrize(
        ("cron", "says"),
        [("0 0 9 * * 5", "has 6 fields"), ("*/90 * * * *", "/90 step"), ("0 */36 * * *", "/36 step")],
    )
    @pytest.mark.asyncio
    async def test_cron_read_another_way_is_refused(self, db, backend, cron, says):
        error = await _refusal(backend, db, {**NEW, "cron_expression": cron}, path=NEW_PATH)

        assert says in error.hint

    @pytest.mark.parametrize("cron", ["*/60 * * * *", "0 */24 * * *", "@daily", "*/15 9-16 * * 1-5"])
    @pytest.mark.asyncio
    async def test_cron_that_means_what_it_says_is_saved(self, db, backend, cron):
        await _create(backend, {**NEW, "cron_expression": cron})

        assert db.rows[CREATED]["cron_expression"] == cron

    @pytest.mark.asyncio
    async def test_a_repeated_key_is_refused_rather_than_last_one_wins(self, db, backend):
        db.add(_row())
        content = json.dumps(_shown(_row())).replace('"status": "active"', '"status": "paused", "status": "active"')
        await backend.aread_range(PATH)

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PATH, content)

        assert exc.value.error_type == "parse_error"
        assert '"status" appears twice in "Morning brief"' in exc.value.hint
        assert db.writes == []

    @pytest.mark.parametrize(
        ("content", "says"),
        [
            ('{"name": "A", "trigger_config": ' + "[" * 100_000 + "]" * 100_000 + "}", "nested more than 64"),
            # Read by every Python, so only the route's own bound refuses it.
            ('{"name": "A", "metadata": {"a": ' + "[" * 64 + "]" * 64 + "}}", "nested more than 64"),
            ('{"name": "A", "max_failures": ' + "9" * 5_000 + "}", "a number has too many digits"),
            # Postgres stores no NUL in text, and its refusal quotes the row.
            ('{"name": "A", "instruction": "buy\\u0000sell"}', "NUL character"),
            ('{"name": "A", "metadata": {"k\\u0000": 1}}', "NUL character"),
        ],
    )
    @pytest.mark.asyncio
    async def test_json_python_cannot_read_is_a_parse_error(self, db, backend, content, says):
        """Under the size cap, but past what every Python reads alike: one
        raises past the decoder's own error, which would reach the agent as
        a server error to retry, and nesting saves on some Pythons only."""
        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(NEW_PATH, content)

        assert exc.value.error_type == "parse_error"
        assert says in exc.value.hint
        assert db.writes == []

    @pytest.mark.parametrize("content", ["[]", '"brief"', "null", '[{"name": "A"}]'])
    @pytest.mark.asyncio
    async def test_a_file_that_is_not_one_object_is_refused(self, db, backend, content):
        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(NEW_PATH, content)

        assert "one JSON object: one automation's fields" in exc.value.hint
        assert db.writes == []

    @pytest.mark.parametrize(("value", "says"), [("2030-10-01", "no time of day"), ("2020-01-01T09:00:00", "already passed")])
    @pytest.mark.asyncio
    async def test_a_one_time_run_needs_a_future_time_of_day(self, db, backend, value, says):
        error = await _refusal(backend, db, {"name": "A", "next_run_at": value, "instruction": "Go."}, path=NEW_PATH)

        [(path, message)] = error.problems
        assert path == "next_run_at" and says in message

    @pytest.mark.asyncio
    async def test_moving_a_one_time_run_into_the_past_is_refused(self, db, backend):
        db.add(_once_row())

        error = await _refusal(backend, db, {"next_run_at": "2021-05-01T09:00:00"})

        assert "already passed" in error.hint

    @pytest.mark.parametrize("zone", ["EST", "MST"])
    @pytest.mark.asyncio
    async def test_a_fixed_offset_zone_is_refused_for_its_region(self, db, backend, zone):
        db.add(_row())

        error = await _refusal(backend, db, {"timezone": zone})

        assert error.field_path == "timezone"
        assert "daylight saving" in error.hint and "America/" in error.hint

    @pytest.mark.asyncio
    async def test_a_new_automation_on_a_fixed_offset_zone_is_refused(self, db, backend):
        error = await _refusal(backend, db, {**NEW, "timezone": "EST"}, path=NEW_PATH)

        assert error.field_path == "timezone"
        assert "use a region zone such as America/New_York" in error.hint

    @pytest.mark.asyncio
    async def test_a_fixed_offset_zone_the_row_already_has_passes(self, db, backend):
        """The page may store one; resent unchanged, it is not the agent's to fix."""
        db.add(_row(timezone="EST"))

        await _write(backend, {**_shown(_row(timezone="EST")), "instruction": "Other."})

        assert (db.rows[BRIEF]["instruction"], db.rows[BRIEF]["timezone"]) == ("Other.", "EST")

    @pytest.mark.asyncio
    async def test_resuming_a_passed_one_time_run_says_to_move_it(self, db, backend):
        """The lifecycle refuses it at commit, against the row as it stands."""
        db.add(_once_row(status="paused", next_run_at=PASSED))

        error = await _refusal(backend, db, {"status": "active"})

        assert error.field_path == "status"
        assert "set next_run_at to a future time" in error.hint

    @pytest.mark.asyncio
    async def test_resuming_with_a_new_time_saves_both(self, db, backend):
        db.add(_once_row(status="paused", next_run_at=PASSED))

        report = await _write(backend, {"status": "active", "next_run_at": "2030-10-01T09:00:00"})

        assert "next_run_at; resumed" in report
        lifecycle.resume_automation.assert_awaited_once()
        assert (db.rows[BRIEF]["status"], db.rows[BRIEF]["next_run_at"]) == ("active", NEXT_RUN)

    @pytest.mark.parametrize("field", ["name", "instruction"])
    @pytest.mark.asyncio
    async def test_an_empty_name_or_instruction_is_refused(self, db, backend, field):
        error = await _refusal(backend, db, {**NEW, field: ""}, path=NEW_PATH)

        assert error.field_path == field

    @pytest.mark.asyncio
    async def test_a_cron_longer_than_its_column_is_refused_naming_the_field(self, db, backend):
        cron = f"{','.join(str(m) for m in range(60))} 9 * * 1-5"

        error = await _refusal(backend, db, {**NEW, "cron_expression": cron}, path=NEW_PATH)

        assert error.field_path == "cron_expression"

    @pytest.mark.asyncio
    async def test_a_blank_delivery_entry_from_the_old_tool_is_dropped_on_save(self, db, backend):
        db.add(_row(delivery_config={"methods": ["slack", ""]}))

        await _write(backend, {**_shown(_row(delivery_config={"methods": ["slack", ""]})), "status": "paused"})

        assert db.rows[BRIEF]["status"] == "paused"


class TestReadmePointer:
    @pytest.mark.asyncio
    async def test_a_refused_file_points_at_the_readme(self, db, backend):
        db.add(_row())

        error = await _refusal(backend, db, {"instruction": None})

        assert error.message.splitlines()[-1] == f"See {ROOT}/README.md for the fields and examples."

    @pytest.mark.asyncio
    async def test_a_refusal_of_the_readme_does_not_point_at_the_readme(self, backend):
        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(f"{ROOT}/README.md", "# notes")

        assert "See " not in exc.value.message
        assert "Write the files beside it instead." in exc.value.message
