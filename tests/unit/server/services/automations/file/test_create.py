"""A new file: what its automation defaults to, what the save reads about
the user to fill those defaults, and which names a file may take."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from ptc_agent.agent.backends.db_json_route import UserDataValidationError
from src.server.services.automations.file import AutomationFile
from tests.unit.server.services.automations.file._support import (
    BRIEF,
    CALL,
    CREATED,
    NEW,
    NEW_FILE_NAME,
    NEXT_RUN_LOCAL,
    NO_ZONE,
    ROOT,
    THREAD,
    USER,
    WORKSPACE,
    _backend,
    _create,
    _refusal,
    _row,
    _shown,
    _write,
)

NEW_FILE = AutomationFile(NEW_FILE_NAME)


class TestCreate:
    @pytest.mark.asyncio
    async def test_a_new_file_takes_the_writers_defaults(self, db, backend):
        await _create(backend, {"name": "Brief", "cron_expression": "0 9 * * 1-5", "instruction": "Go."})

        row = db.rows[CREATED]
        assert row["file_name"] == NEW_FILE_NAME
        assert row["trigger_type"] == "cron"
        assert row["timezone"] == "America/New_York"
        assert row["agent_mode"] == "ptc"
        assert row["workspace_id"] == UUID(WORKSPACE)
        assert (row["thread_strategy"], row["conversation_thread_id"]) == ("new", None)
        assert row["max_failures"] == 3

    @pytest.mark.parametrize(
        ("entry", "kind"),
        [
            ({"next_run_at": "2030-10-01T09:00:00"}, "once"),
            ({"trigger_config": {"symbol": "AAPL", "conditions": [{"type": "price_below", "value": 200}]}}, "price"),
        ],
    )
    @pytest.mark.asyncio
    async def test_the_trigger_type_follows_the_schedule_field(self, db, backend, entry, kind):
        await _create(backend, {"name": "A", "instruction": "Go.", **entry})

        assert db.rows[CREATED]["trigger_type"] == kind

    @pytest.mark.asyncio
    async def test_thread_current_pins_this_conversation(self, db, backend):
        await _create(backend, {**NEW, "thread": "current"})

        row = db.rows[CREATED]
        assert (row["thread_strategy"], row["conversation_thread_id"]) == ("continue", UUID(THREAD))

    @pytest.mark.parametrize(
        ("timezone", "expected"),
        [
            (None, datetime(2030, 10, 1, 13, 0, tzinfo=UTC)),
            ("Asia/Tokyo", datetime(2030, 10, 1, 0, 0, tzinfo=UTC)),
        ],
        ids=["writers-clock", "files-own-clock"],
    )
    @pytest.mark.asyncio
    async def test_a_time_without_an_offset_is_read_on_the_automations_clock(self, db, backend, timezone, expected):
        entry = {"name": "A", "next_run_at": "2030-10-01T09:00:00", "instruction": "Go."}
        if timezone:
            entry["timezone"] = timezone

        await _create(backend, entry)

        assert db.rows[CREATED]["next_run_at"] == expected

    @pytest.mark.parametrize(
        ("entry", "suffix"),
        [
            ({"cron_expression": "0 9 * * *", "status": "paused"}, ", paused"),
            ({"next_run_at": "2030-10-01T09:00:00"}, f"; next run {NEXT_RUN_LOCAL}"),
            (
                {"trigger_config": {"symbol": "AAPL", "conditions": [{"type": "price_below", "value": 200}]}},
                "; watching AAPL as a US stock",
            ),
            # The market is the listing's own, not every stock's US.
            (
                {"trigger_config": {"symbol": "600519.SH", "conditions": [{"type": "price_below", "value": 1500}]}},
                "; watching 600519.SH as a CN stock",
            ),
            # A dotted class share parses to no known venue but trades on the US tape.
            (
                {"trigger_config": {"symbol": "BRK.B", "conditions": [{"type": "price_below", "value": 500}]}},
                "; watching BRK.B as a US stock",
            ),
            (
                {"trigger_config": {"symbol": "PETR4.SA", "conditions": [{"type": "price_below", "value": 40}]}},
                "; watching PETR4.SA as a stock on an unknown venue",
            ),
            (
                {
                    "trigger_config": {
                        "symbol": "HSI",
                        "market": "index",
                        "conditions": [{"type": "price_below", "value": 20000}],
                    }
                },
                "; watching HSI as an HK index",
            ),
        ],
        ids=[
            "paused", "once", "price", "price-cn-stock", "price-us-class-share",
            "price-foreign-unknown-venue", "price-hk-index",
        ],
    )
    @pytest.mark.asyncio
    async def test_a_create_reports_when_it_will_run(self, db, backend, entry, suffix):
        report = await _create(backend, {"name": "New", "instruction": "Go.", **entry})

        assert report == f'Saved new.json: created "New"{suffix}'

    @pytest.mark.asyncio
    async def test_the_name_the_user_sees_need_not_match_the_file(self, db, backend):
        """Names are free-form and may repeat; only the file name is unique."""
        db.add(_row())

        await _create(backend, {**NEW, "name": "Morning brief"})

        assert [(r["name"], r["file_name"]) for r in db.list()] == [
            ("Morning brief", "morning-brief.json"),
            ("Morning brief", NEW_FILE_NAME),
        ]

    @pytest.mark.asyncio
    async def test_status_deleted_on_a_new_file_is_refused(self, db, backend):
        error = await _refusal(backend, db, {**NEW, "status": "deleted"}, path=f"{ROOT}/{NEW_FILE_NAME}")

        assert error.problems == [
            ("status", '"deleted" deletes an existing automation, and none is filed as new.json')
        ]

    @pytest.mark.asyncio
    async def test_the_automation_id_is_no_field_to_write(self, db, backend):
        """It is read-only state; written at the top it would read as naming
        an automation to update, which a file's name does instead."""
        error = await _refusal(backend, db, {**NEW, "automation_id": BRIEF}, path=f"{ROOT}/{NEW_FILE_NAME}")

        [(path, message)] = error.problems
        assert path == "automation_id" and message.startswith("unknown field; allowed: name,")


class TestFileNames:
    @pytest.mark.parametrize(
        "name",
        ["morning-brief.json", "A.json", "x_1-2.json", "9.json", "a" * 64 + ".json"],
    )
    @pytest.mark.asyncio
    async def test_a_name_by_the_rule_creates(self, db, backend, name):
        await _write(backend, NEW, path=f"{ROOT}/{name}")

        assert db.rows[CREATED]["file_name"] == name

    @pytest.mark.parametrize(
        "name",
        [
            "brief.txt",
            "-brief.json",
            "_brief.json",
            "my brief.json",
            "brief.json.bak",
            ".brief.json",
            "brief.JSON",
            "a" * 65 + ".json",
            "café.json",
        ],
    )
    @pytest.mark.asyncio
    async def test_any_other_name_is_refused_with_the_rule(self, db, backend, name):
        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(f"{ROOT}/{name}", json.dumps(NEW))

        assert "1 to 64 letters, digits, - or _, starting with a letter or digit, then .json" in exc.value.hint
        assert db.writes == []

    @pytest.mark.asyncio
    async def test_a_missing_file_reads_as_none(self, db, backend):
        assert await backend.aread_range(f"{ROOT}/{NEW_FILE_NAME}") is None
        assert await backend.aread_text(f"{ROOT}/{NEW_FILE_NAME}") is None


class TestDefaultZone:
    """A create that names no timezone runs on the conversation's clock, else
    on the user's stored zone, else on UTC. The stored zone costs a query, so
    it is read only when the conversation has none."""

    @pytest.mark.asyncio
    async def test_the_calls_zone_comes_first(self, db):
        document = await NEW_FILE.parse(USER, CALL, json.dumps(NEW), None)

        assert document.timezone == "America/New_York"
        db.get_user_timezone.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_without_one_the_users_stored_zone_runs_the_create(self, db):
        document = await NEW_FILE.parse(USER, NO_ZONE, json.dumps(NEW), None)
        plan = NEW_FILE.plan(NO_ZONE, document, None).changes

        assert plan.create.data.timezone == "Europe/Paris"
        db.get_user_timezone.assert_awaited_once_with(USER)

    @pytest.mark.asyncio
    async def test_a_create_that_names_a_zone_keeps_it(self, db):
        document = await NEW_FILE.parse(USER, NO_ZONE, json.dumps({**NEW, "timezone": "Asia/Tokyo"}), None)
        plan = NEW_FILE.plan(NO_ZONE, document, None).changes

        assert plan.create.data.timezone == "Asia/Tokyo"
        db.get_user_timezone.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_with_no_zone_anywhere_it_runs_on_utc(self, db):
        db.get_user_timezone = AsyncMock(return_value=None)

        document = await NEW_FILE.parse(USER, NO_ZONE, json.dumps(NEW), None)

        assert document.timezone == "UTC"

    @pytest.mark.asyncio
    async def test_a_fixed_offset_default_is_the_users_own_choice(self, db):
        """Only a zone the agent writes is held to region zones."""
        db.get_user_timezone = AsyncMock(return_value="EST")

        await _create(_backend(NO_ZONE), NEW)

        assert db.rows[CREATED]["timezone"] == "EST"

    @pytest.mark.asyncio
    async def test_an_update_that_names_no_model_reads_nothing_about_the_user(self, db):
        db.add(_row())

        await _write(_backend(NO_ZONE), {**_shown(_row()), "instruction": "Summarize the news."})

        assert db.depths_read_at == []

    @pytest.mark.asyncio
    async def test_what_a_save_reads_about_the_user_is_read_before_its_locks(self, db):
        """Read under the save's locks, the stored zone and the model
        preferences would each hold a second pool connection while other
        saves wait on those locks."""
        await _create(_backend(NO_ZONE), {**NEW, "llm_model": "model-a"})

        assert db.rows[CREATED]["llm_model"] == "model-a"
        # The stored zone, then the model preferences, each outside any transaction.
        assert db.depths_read_at == [0, 0]
