"""Ownership gating for caller-supplied automation targets.

Locks the cross-tenant contract: a ``workspace_id`` / ``conversation_thread_id``
naming something the caller does not own is rejected before any row is written.
The gate lives in the lifecycle because the REST router, the agent tools and
the automation files all write through it.
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.home_workspace import get_home_workspace_id
from src.server.database.workspace import HomeWorkspaceTaken
from src.server.services.automations import lifecycle
from src.server.services.automations.lifecycle import (
    AutomationRefusal,
    TargetRefused,
    create_automation,
    update_automation,
)
from src.server.models.automation import AutomationCreate

OWNER = "user-owner"
OTHER = "user-other"
AUTOMATION_ID = str(uuid.uuid4())
WORKSPACE_ID = str(uuid.uuid4())
THREAD_ID = str(uuid.uuid4())
HOME = get_home_workspace_id(OWNER)

_THREAD_OWNER = "src.server.database.conversation.get_thread_owner_id"
_THREAD_META = "src.server.database.conversation.get_thread_auth_meta"


def _thread_in(workspace_id):
    return AsyncMock(return_value={"user_id": OWNER, "workspace_id": uuid.UUID(workspace_id)})


@pytest.fixture(autouse=True)
def _home(monkeypatch):
    """Home's row, which a write naming Home makes sure of, is the caller's."""
    monkeypatch.setattr(lifecycle, "_home", AsyncMock(return_value=HOME))


def _create_data(**overrides):
    # Home, whose row the fixture above stands in for.
    data = {
        "name": "daily digest",
        "instruction": "summarize the watchlist",
        "trigger_type": "cron",
        "cron_expression": "0 9 * * 1-5",
        "timezone": "UTC",
        "workspace_id": HOME,
    }
    data.update(overrides)
    return AutomationCreate(**data)


def _current_row(**overrides):
    row = {
        "automation_id": AUTOMATION_ID,
        "user_id": OWNER,
        "trigger_type": "cron",
        "cron_expression": "0 9 * * 1-5",
        "timezone": "UTC",
        "status": "active",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


class TestCreateTargetOwnership:
    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_rejects_workspace_owned_by_another_user(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_get_workspace.return_value = {
            "workspace_id": WORKSPACE_ID, "user_id": OTHER,
        }

        with pytest.raises(TargetRefused) as exc:
            await create_automation(
                OWNER, _create_data(workspace_id=WORKSPACE_ID),
            )

        assert exc.value.status_code == 403
        assert exc.value.field == "workspace_id"
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_rejects_workspace_that_does_not_exist(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_get_workspace.return_value = None

        with pytest.raises(TargetRefused) as exc:
            await create_automation(
                OWNER, _create_data(workspace_id=WORKSPACE_ID),
            )

        assert exc.value.status_code == 404
        assert exc.value.field == "workspace_id"
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch(_THREAD_OWNER, new_callable=AsyncMock)
    async def test_rejects_pinned_thread_owned_by_another_user(
        self, mock_thread_owner, mock_auto_db,
    ):
        mock_thread_owner.return_value = OTHER

        with pytest.raises(TargetRefused) as exc:
            await create_automation(
                OWNER,
                _create_data(
                    thread_strategy="continue", conversation_thread_id=THREAD_ID,
                ),
            )

        assert exc.value.status_code == 403
        assert exc.value.field == "conversation_thread_id"
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch(_THREAD_OWNER, new_callable=AsyncMock)
    async def test_rejects_pinned_thread_that_does_not_exist(
        self, mock_thread_owner, mock_auto_db,
    ):
        mock_thread_owner.return_value = None

        with pytest.raises(TargetRefused) as exc:
            await create_automation(
                OWNER,
                _create_data(
                    thread_strategy="continue", conversation_thread_id=THREAD_ID,
                ),
            )

        assert exc.value.status_code == 404
        assert exc.value.field == "conversation_thread_id"
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch(_THREAD_OWNER, new_callable=AsyncMock)
    @patch(_THREAD_META, new=_thread_in(WORKSPACE_ID))
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_accepts_targets_the_caller_owns(
        self, mock_get_workspace, mock_thread_owner, mock_auto_db,
    ):
        mock_get_workspace.return_value = {
            "workspace_id": WORKSPACE_ID, "user_id": OWNER,
        }
        mock_thread_owner.return_value = OWNER
        mock_auto_db.create_automation = AsyncMock(
            return_value={"automation_id": AUTOMATION_ID}
        )

        await create_automation(
            OWNER,
            _create_data(
                workspace_id=WORKSPACE_ID,
                thread_strategy="continue",
                conversation_thread_id=THREAD_ID,
            ),
        )

        mock_auto_db.create_automation.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_skips_lookup_when_no_targets_supplied(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.create_automation = AsyncMock(
            return_value={"automation_id": AUTOMATION_ID}
        )

        await create_automation(OWNER, _create_data())

        mock_get_workspace.assert_not_called()


# ---------------------------------------------------------------------------
# update — the second door onto the same columns
# ---------------------------------------------------------------------------


class TestUpdateTargetOwnership:
    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_rejects_repointing_at_another_users_workspace(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_current_row())
        mock_get_workspace.return_value = {
            "workspace_id": WORKSPACE_ID, "user_id": OTHER,
        }

        with pytest.raises(TargetRefused) as exc:
            await update_automation(
                AUTOMATION_ID, OWNER, {"workspace_id": WORKSPACE_ID},
            )

        assert exc.value.status_code == 403
        assert exc.value.field == "workspace_id"
        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch(_THREAD_OWNER, new_callable=AsyncMock)
    async def test_rejects_repointing_at_another_users_thread(
        self, mock_thread_owner, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_current_row())
        mock_thread_owner.return_value = OTHER

        with pytest.raises(TargetRefused) as exc:
            await update_automation(
                AUTOMATION_ID, OWNER, {"conversation_thread_id": THREAD_ID},
            )

        assert exc.value.status_code == 403
        assert exc.value.field == "conversation_thread_id"
        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_untargeted_update_skips_the_lookup(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_current_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_current_row())

        await update_automation(AUTOMATION_ID, OWNER, {"name": "renamed"})

        mock_get_workspace.assert_not_called()
        mock_auto_db.update_automation.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_missing_automation_returns_none_without_checking_targets(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=None)

        result = await update_automation(
            AUTOMATION_ID, OWNER, {"workspace_id": WORKSPACE_ID},
        )

        assert result is None
        mock_get_workspace.assert_not_called()


# ---------------------------------------------------------------------------
# A pinned thread is where the automation runs
# ---------------------------------------------------------------------------


class TestPinnedThreadPlace:
    """Runs continue in the pinned thread, so the thread must be in the
    workspace the row names, or the row would say one place and run in another."""

    @pytest.fixture(autouse=True)
    def _owned(self):
        workspace = AsyncMock(
            side_effect=lambda ws, conn=None: {"workspace_id": ws, "user_id": OWNER}
        )
        with (
            patch("src.server.services.automations.lifecycle.get_workspace", new=workspace),
            patch(_THREAD_OWNER, new=AsyncMock(return_value=OWNER)),
        ):
            yield

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_create_refuses_a_pin_in_another_workspace(self, mock_auto_db):
        with patch(_THREAD_META, new=_thread_in(WORKSPACE_ID)):
            with pytest.raises(AutomationRefusal) as exc:
                await create_automation(
                    OWNER,
                    _create_data(thread_strategy="continue", conversation_thread_id=THREAD_ID),
                )

        assert exc.value.field == "conversation_thread_id"
        assert str(exc.value) == (
            f"thread {THREAD_ID} is in workspace {WORKSPACE_ID}, but the automation "
            "runs in the user's Home: pin a thread from the user's Home, or run the "
            f"automation in workspace {WORKSPACE_ID}"
        )
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_update_refuses_a_new_pin_in_another_workspace(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(
            return_value=_current_row(workspace_id=HOME)
        )

        with patch(_THREAD_META, new=_thread_in(WORKSPACE_ID)):
            with pytest.raises(AutomationRefusal) as exc:
                await update_automation(
                    AUTOMATION_ID, OWNER, {"conversation_thread_id": THREAD_ID},
                )

        assert exc.value.field == "conversation_thread_id"
        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_restating_the_stored_pin_checks_nothing(self, mock_auto_db):
        """Restating the stored pin without a move checks nothing, so an edit
        to anything else still saves."""
        mock_auto_db.get_automation = AsyncMock(
            return_value=_current_row(
                workspace_id=HOME,
                thread_strategy="continue",
                conversation_thread_id=uuid.UUID(THREAD_ID),
            )
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_current_row())
        meta = _thread_in(WORKSPACE_ID)

        with patch(_THREAD_META, new=meta):
            await update_automation(
                AUTOMATION_ID,
                OWNER,
                {"name": "renamed", "workspace_id": HOME, "conversation_thread_id": THREAD_ID},
            )

        meta.assert_not_awaited()
        mock_auto_db.update_automation.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_switching_to_continue_checks_a_pin_left_on_the_row(self, mock_auto_db):
        """A row that started a new thread each run can hold a pin nothing
        checked; continuing it would run one place and name another."""
        mock_auto_db.get_automation = AsyncMock(
            return_value=_current_row(
                workspace_id=HOME,
                thread_strategy="new",
                conversation_thread_id=uuid.UUID(THREAD_ID),
            )
        )

        with patch(_THREAD_META, new=_thread_in(WORKSPACE_ID)):
            with pytest.raises(AutomationRefusal) as exc:
                await update_automation(
                    AUTOMATION_ID, OWNER, {"thread_strategy": "continue"},
                )

        assert exc.value.field == "conversation_thread_id"
        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_a_pin_on_a_row_with_no_workspace_asks_for_one(self, mock_auto_db):
        """A row whose workspace was deleted runs nowhere, so a pin there
        needs a workspace named with it."""
        mock_auto_db.get_automation = AsyncMock(
            return_value=_current_row(workspace_id=None, thread_strategy="new")
        )

        with patch(_THREAD_META, new=_thread_in(WORKSPACE_ID)):
            with pytest.raises(AutomationRefusal) as exc:
                await update_automation(
                    AUTOMATION_ID,
                    OWNER,
                    {"thread_strategy": "continue", "conversation_thread_id": THREAD_ID},
                )

        assert exc.value.field == "workspace_id"
        assert str(exc.value) == lifecycle._NO_WORKSPACE
        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_a_move_to_the_pinned_threads_workspace_keeps_the_pin(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(
            return_value=_current_row(
                workspace_id=HOME,
                thread_strategy="continue",
                conversation_thread_id=uuid.UUID(THREAD_ID),
            )
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_current_row())

        with patch(_THREAD_META, new=_thread_in(WORKSPACE_ID)):
            await update_automation(
                AUTOMATION_ID,
                OWNER,
                {"workspace_id": WORKSPACE_ID, "conversation_thread_id": THREAD_ID},
            )

        written = mock_auto_db.update_automation.call_args.kwargs
        assert (written["workspace_id"], written["conversation_thread_id"]) == (
            WORKSPACE_ID,
            THREAD_ID,
        )


# ---------------------------------------------------------------------------
# Home
# ---------------------------------------------------------------------------


class TestHome:
    """Home may be named before the user ever opened it, as a turn may name
    it; an account merge can leave its id on another account's row."""

    @pytest.fixture(autouse=True)
    def _home(self):
        """The real one, over the module's stand-in."""

    @pytest.fixture
    def upsert(self, monkeypatch):
        upsert = AsyncMock()
        monkeypatch.setattr(lifecycle, "get_or_create_home_workspace", upsert)
        return upsert

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_home_is_made_when_the_user_has_none(
        self, mock_get_workspace, mock_auto_db, upsert,
    ):
        mock_get_workspace.return_value = None
        mock_auto_db.create_automation = AsyncMock(
            return_value={"automation_id": AUTOMATION_ID}
        )

        await create_automation(OWNER, _create_data())

        assert upsert.await_args.args == (OWNER,)
        assert mock_auto_db.create_automation.call_args.kwargs["workspace_id"] == HOME

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_an_existing_home_is_not_written_again(
        self, mock_get_workspace, mock_auto_db, upsert,
    ):
        mock_get_workspace.return_value = {"workspace_id": HOME, "user_id": OWNER}
        mock_auto_db.create_automation = AsyncMock(
            return_value={"automation_id": AUTOMATION_ID}
        )

        await create_automation(OWNER, _create_data())

        upsert.assert_not_awaited()
        mock_auto_db.create_automation.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("held", ["row", "upsert"])
    @patch("src.server.services.automations.lifecycle.auto_db")
    @patch("src.server.services.automations.lifecycle.get_workspace")
    async def test_home_held_by_another_account_is_refused(
        self, mock_get_workspace, mock_auto_db, upsert, held,
    ):
        if held == "row":
            mock_get_workspace.return_value = {"workspace_id": HOME, "user_id": OTHER}
        else:
            mock_get_workspace.return_value = None
            upsert.side_effect = HomeWorkspaceTaken(HOME, OWNER)

        with pytest.raises(TargetRefused) as exc:
            await create_automation(OWNER, _create_data())

        assert (exc.value.status_code, exc.value.field) == (403, "workspace_id")
        mock_auto_db.create_automation.assert_not_called()
