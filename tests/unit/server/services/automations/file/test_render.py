"""How a row reads as its file: the definition, its state, and the version a
save is checked against."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from src.server.services.automations import file
from tests.unit.server.services.automations.file._support import (
    BRIEF,
    BRIEF_FILE,
    FILE_NAME,
    NEXT_RUN,
    NEXT_RUN_LOCAL,
    OTHER_FILE_NAME,
    PINNED,
    USER,
    _once_row,
    _other_row,
    _price_row,
    _row,
    _shown,
)


class TestFile:
    def test_a_file_is_one_automations_fields_then_its_state(self):
        content, _ = BRIEF_FILE.render(_row())

        shown = json.loads(content)
        assert list(shown) == [
            "name", "description", "status", "trigger_type", "cron_expression", "timezone", "instruction",
            "workspace_id", "thread", "llm_model", "delivery", "max_failures", "state",
        ]
        assert content == json.dumps(shown, indent=2, ensure_ascii=False) + "\n"

    def test_no_row_is_no_file(self):
        assert BRIEF_FILE.render(None) is None

    @pytest.mark.parametrize(
        ("row", "own", "value"),
        [
            (_row(), "cron_expression", "0 9 * * 1-5"),
            (_once_row(), "next_run_at", NEXT_RUN_LOCAL),
            (
                _price_row(),
                "trigger_config",
                {"symbol": "AAPL", "conditions": [{"type": "price_below", "value": 200}]},
            ),
        ],
        ids=["cron", "once", "price"],
    )
    def test_a_file_carries_only_its_own_kinds_schedule_field(self, row, own, value):
        entry = _shown(row)

        assert entry[own] == value
        others = {"cron_expression", "next_run_at", "trigger_config"} - {own}
        assert not others & set(entry)

    @pytest.mark.parametrize(
        ("strategy", "pinned", "own", "shown"),
        [
            ("new", None, False, "new"),
            ("continue", None, False, "persistent"),
            # The thread a persistent automation's first run made and pinned.
            ("continue", UUID(PINNED), True, "persistent"),
            ("continue", UUID(PINNED), False, PINNED),
        ],
    )
    def test_thread_reads_new_persistent_or_the_pinned_id(self, strategy, pinned, own, shown):
        entry = _shown(_row(thread_strategy=strategy, conversation_thread_id=pinned, owns_thread=own))

        assert entry["thread"] == shown

    def test_delivery_is_the_list_of_method_names(self):
        slack, none = _shown(_row(delivery_config={"methods": ["slack"]})), _shown(_other_row())

        assert (slack["delivery"], none["delivery"]) == (["slack"], [])

    def test_a_blank_method_the_old_tool_stored_is_not_shown(self):
        assert _shown(_row(delivery_config={"methods": ["slack", ""]}))["delivery"] == ["slack"]


class TestState:
    def test_state_carries_what_the_server_keeps(self):
        error = "x" * 400
        entry = _shown(
            _row(
                status="disabled",
                failure_count=3,
                disable_reason="max_failures",
                last_execution={
                    "status": "failed",
                    "scheduled_at": datetime(2026, 9, 30, 12, 59, tzinfo=UTC),
                    "started_at": datetime(2026, 9, 30, 13, 0, tzinfo=UTC),
                    "completed_at": datetime(2026, 9, 30, 13, 1, 12, tzinfo=UTC),
                    "conversation_thread_id": UUID(PINNED),
                    "excerpt": "Top movers were",
                    "failure_reason": "model_error",
                    "error_message": error,
                    "dismissed_at": datetime(2026, 9, 30, 14, 0, tzinfo=UTC),
                },
            )
        )

        assert entry["state"] == {
            "automation_id": BRIEF,
            "next_run_at": NEXT_RUN_LOCAL,
            "last_run": {
                "status": "failed",
                "at": "2026-09-30T09:01:12-04:00",
                "thread_id": PINNED,
                "excerpt": "Top movers were",
                "failure_reason": "model_error",
                "error": "x" * 300 + "…",
                "dismissed": True,
            },
            "failure_count": 3,
            "disable_reason": "max_failures",
        }

    def test_a_skipped_run_says_why(self):
        entry = _shown(
            _row(
                last_execution={
                    "status": "skipped",
                    "scheduled_at": NEXT_RUN,
                    "skip_reason": "user_skipped",
                }
            )
        )

        assert entry["state"]["last_run"] == {
            "status": "skipped",
            "at": NEXT_RUN_LOCAL,
            "skip_reason": "user_skipped",
        }

    def test_a_quiet_automation_shows_only_its_id_and_next_cron_run(self):
        cron, once = _shown(_row()), _shown(_once_row())

        assert cron["state"] == {"automation_id": BRIEF, "next_run_at": NEXT_RUN_LOCAL}
        assert once["state"] == {"automation_id": BRIEF}


class TestVersion:
    def test_state_changes_leave_the_version_alone(self):
        """The scheduler moves state on every firing and poll. Hashed, it would
        refuse nearly every write the agent makes between two firings."""
        before = _row()
        after = _row(
            next_run_at=datetime(2026, 10, 2, 13, 0, tzinfo=UTC),
            failure_count=1,
            last_execution={"status": "failed", "completed_at": NEXT_RUN, "error_message": "boom"},
        )

        (content_a, version_a), (content_b, version_b) = BRIEF_FILE.render(before), BRIEF_FILE.render(after)

        assert content_a != content_b
        assert version_a == version_b

    @pytest.mark.parametrize(
        ("before", "after"),
        [
            (_row(), _row(instruction="Summarize the news.")),
            (_row(), _row(status="paused")),
            (_row(), _row(cron_expression="0 10 * * 1-5")),
            (_row(), _row(delivery_config={"methods": ["slack"]})),
            # A one-time run's next_run_at is its schedule, not its state.
            (_once_row(), _once_row(next_run_at=datetime(2026, 10, 2, 13, 0, tzinfo=UTC))),
        ],
        ids=["instruction", "status", "cron_expression", "delivery", "once-next_run_at"],
    )
    def test_a_definition_change_moves_the_version(self, before, after):
        assert BRIEF_FILE.render(before)[1] != BRIEF_FILE.render(after)[1]

    def test_another_automation_with_the_same_content_is_another_version(self):
        """A file deleted and made again under its name is not the one a
        writer read, though it reads the same."""
        again = _row(automation_id=UUID(int=7))

        assert BRIEF_FILE.render(_row())[1] != BRIEF_FILE.render(again)[1]

    def test_a_rename_leaves_the_version_alone(self):
        assert BRIEF_FILE.render(_row())[1] == BRIEF_FILE.render(_row(file_name="renamed.json"))[1]


class TestFetch:
    @pytest.mark.asyncio
    async def test_a_read_outside_a_save_takes_the_row_filed_under_its_name(self, db):
        db.add(_row(), _other_row())

        assert await BRIEF_FILE.fetch(USER) == db.list()[1]
        assert db.reads == [None]

    @pytest.mark.asyncio
    async def test_a_save_reads_its_row_on_its_own_connection(self, db):
        db.add(_row())

        assert await BRIEF_FILE.fetch(USER, db.conn) == db.list()[0]
        assert db.reads == [db.conn]

    @pytest.mark.asyncio
    async def test_the_folder_lists_every_file_by_name(self, db):
        db.add(_row(), _other_row())

        files = await file.rendered_files(USER)

        assert list(files) == [OTHER_FILE_NAME, FILE_NAME]
        assert files[FILE_NAME] == BRIEF_FILE.render(db.list()[1])
