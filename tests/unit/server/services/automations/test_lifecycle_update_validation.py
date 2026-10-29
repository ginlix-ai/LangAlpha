"""What the automation PATCH path refuses, and what a pause and resume keep.

Every automation runs in some workspace, Home's for the user's Home, so the
merged state of a PATCH has to name one; otherwise the row is only rejected by
the executor at run time, which spends a failure against ``max_failures``. The
same goes for a price config the monitor cannot read, and for a trigger kind a
PATCH cannot change.
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.automations.lifecycle import (
    AutomationRefusal,
    pause_automation,
    resume_automation,
    update_automation,
)

OWNER = "user-owner"
AUTOMATION_ID = str(uuid.uuid4())
WORKSPACE_ID = str(uuid.uuid4())
OTHER_WORKSPACE_ID = str(uuid.uuid4())
HOME = get_home_workspace_id(OWNER)
_LIFECYCLE = "src.server.services.automations.lifecycle"
_THREAD_META = "src.server.database.conversation.get_thread_auth_meta"


def _row(**overrides):
    row = {
        "automation_id": AUTOMATION_ID,
        "user_id": OWNER,
        "trigger_type": "cron",
        "cron_expression": "0 9 * * 1-5",
        "timezone": "UTC",
        "status": "active",
        "workspace_id": WORKSPACE_ID,
    }
    row.update(overrides)
    return row


class TestPlacement:
    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    @patch(f"{_LIFECYCLE}.get_workspace")
    async def test_naming_a_workspace_moves_the_automation_there(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_row())
        mock_get_workspace.return_value = {
            "workspace_id": OTHER_WORKSPACE_ID, "user_id": OWNER,
        }

        await update_automation(AUTOMATION_ID, OWNER, {"workspace_id": OTHER_WORKSPACE_ID})

        written = mock_auto_db.update_automation.call_args.kwargs
        assert written["workspace_id"] == OTHER_WORKSPACE_ID
        assert mock_get_workspace.call_args.args == (OTHER_WORKSPACE_ID,)

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_an_edit_that_names_no_workspace_leaves_a_placed_row_alone(
        self, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(AUTOMATION_ID, OWNER, {"name": "renamed"})

        assert mock_auto_db.update_automation.call_args.kwargs == {
            "conn": None, "name": "renamed",
        }


class TestTriggerKindIsFixed:
    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_changing_the_kind_is_refused(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())

        with pytest.raises(ValidationError, match="can't change from 'cron' to 'once'"):
            await update_automation(AUTOMATION_ID, OWNER, {"trigger_type": "once"})

        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_restating_the_same_kind_proceeds(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(
            AUTOMATION_ID, OWNER, {"trigger_type": "cron", "name": "renamed"}
        )

        mock_auto_db.update_automation.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kind", "field", "value"),
        [
            ("price", "next_run_at", "2026-10-01T13:00:00Z"),
            ("once", "cron_expression", "0 9 * * *"),
            ("cron", "trigger_config", {
                "symbol": "AAPL", "conditions": [{"type": "price_above", "value": 200}],
            }),
        ],
    )
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_another_kinds_schedule_field_is_refused(self, mock_auto_db, kind, field, value):
        # A next_run_at stored on a price row would be claimed and fired by
        # the scheduler as if it were due.
        mock_auto_db.get_automation = AsyncMock(return_value=_row(trigger_type=kind))

        with pytest.raises(ValidationError, match=f"{field} doesn't apply to a '{kind}'"):
            await update_automation(AUTOMATION_ID, OWNER, {field: value})

        mock_auto_db.update_automation.assert_not_called()


PRICE_CONFIG = {"symbol": "AAPL", "conditions": [{"type": "price_above", "value": 200}]}


class TestPriceConfigOnUpdate:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "config",
        [
            {**PRICE_CONFIG, "conditions": [{"type": "price_above", "value": 0}]},
            {**PRICE_CONFIG, "symbol": "^SPX"},
            {**PRICE_CONFIG, "symbol": "I:SPX"},
            {**PRICE_CONFIG, "symbol": "ABCDEFGHIJK"},
        ],
        ids=["zero-value", "caret", "I-prefix", "too-long"],
    )
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_a_config_the_monitor_would_skip_is_refused(self, mock_auto_db, config):
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(trigger_type="price", cron_expression=None)
        )

        with pytest.raises(ValueError, match="Invalid price trigger config"):
            await update_automation(AUTOMATION_ID, OWNER, {"trigger_config": config})

        mock_auto_db.update_automation.assert_not_called()


@pytest.mark.asyncio
@patch("src.server.services.automations.lifecycle.auto_db")
async def test_an_empty_description_clears_it(mock_auto_db):
    mock_auto_db.get_automation = AsyncMock(return_value=_row(description="old"))
    mock_auto_db.update_automation = AsyncMock(return_value=_row(description=""))

    await update_automation(AUTOMATION_ID, OWNER, {"description": ""})

    assert mock_auto_db.update_automation.call_args.kwargs["description"] == ""


class TestPause:
    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_a_one_time_automation_keeps_its_run_time(self, mock_auto_db):
        """Resume reads the time back; the scheduler claims only active rows."""
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(trigger_type="once", cron_expression=None)
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row(status="paused"))

        await pause_automation(AUTOMATION_ID, OWNER)

        mock_auto_db.update_automation.assert_awaited_once_with(
            AUTOMATION_ID, OWNER, conn=None, status="paused"
        )

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_a_cron_clears_its_next_run(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_row(status="paused"))

        await pause_automation(AUTOMATION_ID, OWNER)

        mock_auto_db.update_automation.assert_awaited_once_with(
            AUTOMATION_ID, OWNER, conn=None, status="paused", next_run_at=None
        )


class TestResumeOnce:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status, resumes", [("disabled", True), ("paused", False)])
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_a_past_time_resumes_only_what_the_server_switched_off(
        self, mock_auto_db, status, resumes
    ):
        """A switched-off one-time automation already had its run; back on, it
        waits for Run now. A paused one would silently never run."""
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(
                trigger_type="once", cron_expression=None, status=status,
                next_run_at=None,
            )
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        if not resumes:
            with pytest.raises(ValueError, match="scheduled time has passed"):
                await resume_automation(AUTOMATION_ID, OWNER)
            return
        await resume_automation(AUTOMATION_ID, OWNER)
        mock_auto_db.update_automation.assert_awaited_once_with(
            AUTOMATION_ID, OWNER, conn=None, status="active", failure_count=0,
            disable_reason=None, next_run_at=None,
        )


class TestCronEditRecomputesOnlyWhenActive:
    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_active_cron_gets_a_new_next_run(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(AUTOMATION_ID, OWNER, {"cron_expression": "0 10 * * *"})

        assert mock_auto_db.update_automation.call_args.kwargs["next_run_at"] is not None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["paused", "disabled"])
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_an_edit_does_not_give_a_stopped_cron_a_next_run(self, mock_auto_db, status):
        """Resume computes it from the edited expression and zone."""
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(status=status, next_run_at=None)
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row(status=status))

        await update_automation(
            AUTOMATION_ID, OWNER, {"cron_expression": "0 10 * * *", "timezone": "Asia/Tokyo"}
        )

        assert "next_run_at" not in mock_auto_db.update_automation.call_args.kwargs


class TestThreadPin:
    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_a_named_none_unpins_the_thread(self, mock_auto_db):
        """The agent tool's thread='new'. A pin left behind would be resumed
        by a later switch back to 'persistent'."""
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(
                thread_strategy="continue", conversation_thread_id=str(uuid.uuid4())
            )
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(
            AUTOMATION_ID, OWNER, {"thread_strategy": "new", "conversation_thread_id": None}
        )

        written = mock_auto_db.update_automation.call_args.kwargs
        assert written["thread_strategy"] == "new"
        assert written["conversation_thread_id"] is None

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_an_update_that_names_no_thread_keeps_the_pin(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(AUTOMATION_ID, OWNER, {"name": "Renamed"})

        assert "conversation_thread_id" not in mock_auto_db.update_automation.call_args.kwargs


PINNED = str(uuid.uuid4())


def _owned_workspace(workspace_id, conn=None):
    return {"workspace_id": workspace_id, "user_id": OWNER}


class TestMoveUnpinsTheThread:
    """A run continues its pinned thread only in the automation's workspace,
    so a move that names no thread unpins it: the next run starts one in the
    new place."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("row", "target"),
        [
            (_row(), OTHER_WORKSPACE_ID),
            (_row(), HOME),
            (_row(workspace_id=HOME), WORKSPACE_ID),
        ],
        ids=["to-another-workspace", "to-home", "out-of-home"],
    )
    @patch(f"{_LIFECYCLE}.auto_db")
    @patch(f"{_LIFECYCLE}.get_workspace", new=AsyncMock(side_effect=_owned_workspace))
    async def test_a_move_that_names_no_thread_unpins_it(self, mock_auto_db, row, target):
        mock_auto_db.get_automation = AsyncMock(
            return_value={**row, "thread_strategy": "continue", "conversation_thread_id": PINNED}
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        # The Automations page's PATCH: the place and the strategy, no thread.
        await update_automation(
            AUTOMATION_ID, OWNER, {"workspace_id": target, "thread_strategy": "continue"}
        )

        written = mock_auto_db.update_automation.call_args.kwargs
        assert written["workspace_id"] == target
        assert written["conversation_thread_id"] is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("row", "fields"),
        [
            (_row(), {"workspace_id": WORKSPACE_ID, "thread_strategy": "continue"}),
            (_row(workspace_id=HOME), {"workspace_id": HOME}),
        ],
        ids=["same-workspace", "same-home"],
    )
    @patch(f"{_LIFECYCLE}.auto_db")
    @patch(f"{_LIFECYCLE}.get_workspace", new=AsyncMock(side_effect=_owned_workspace))
    async def test_restating_where_it_runs_keeps_the_pin(self, mock_auto_db, row, fields):
        mock_auto_db.get_automation = AsyncMock(
            return_value={**row, "thread_strategy": "continue", "conversation_thread_id": PINNED}
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(AUTOMATION_ID, OWNER, fields)

        assert "conversation_thread_id" not in mock_auto_db.update_automation.call_args.kwargs

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    @patch(f"{_LIFECYCLE}.get_workspace", new=AsyncMock(side_effect=_owned_workspace))
    @patch("src.server.database.conversation.get_thread_owner_id", new=AsyncMock(return_value=OWNER))
    @patch(
        _THREAD_META,
        new=AsyncMock(return_value={"user_id": OWNER, "workspace_id": uuid.UUID(OTHER_WORKSPACE_ID)}),
    )
    async def test_a_move_that_names_a_thread_there_keeps_that_thread(self, mock_auto_db):
        other_thread = str(uuid.uuid4())
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(thread_strategy="continue", conversation_thread_id=PINNED)
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(
            AUTOMATION_ID,
            OWNER,
            {"workspace_id": OTHER_WORKSPACE_ID, "conversation_thread_id": other_thread},
        )

        assert mock_auto_db.update_automation.call_args.kwargs["conversation_thread_id"] == other_thread

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    @patch(f"{_LIFECYCLE}.get_workspace", new=AsyncMock(side_effect=_owned_workspace))
    @patch("src.server.database.conversation.get_thread_owner_id", new=AsyncMock(return_value=OWNER))
    @patch(
        _THREAD_META,
        new=AsyncMock(return_value={"user_id": OWNER, "workspace_id": uuid.UUID(WORKSPACE_ID)}),
    )
    async def test_a_move_that_names_the_thread_it_leaves_is_refused(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(thread_strategy="continue", conversation_thread_id=PINNED)
        )

        with pytest.raises(AutomationRefusal) as exc:
            await update_automation(
                AUTOMATION_ID,
                OWNER,
                {"workspace_id": OTHER_WORKSPACE_ID, "conversation_thread_id": PINNED},
            )

        assert exc.value.field == "conversation_thread_id"
        mock_auto_db.update_automation.assert_not_called()
