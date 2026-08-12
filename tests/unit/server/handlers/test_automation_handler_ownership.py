"""Ownership gating for caller-supplied automation targets.

Locks the cross-tenant contract: a ``workspace_id`` / ``conversation_thread_id``
naming something the caller does not own is rejected before any row is written.
The gate lives in the handler because the REST router and the agent's
automation tool both write through it.
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.handlers.automation_handler import create_automation, update_automation

OWNER = "user-owner"
OTHER = "user-other"
AUTOMATION_ID = str(uuid.uuid4())
WORKSPACE_ID = str(uuid.uuid4())
THREAD_ID = str(uuid.uuid4())

_THREAD_OWNER = "src.server.database.conversation.get_thread_owner_id"


def _create_data(**overrides):
    data = {
        "name": "daily digest",
        "instruction": "summarize the watchlist",
        "trigger_type": "cron",
        "cron_expression": "0 9 * * 1-5",
        "timezone": "UTC",
    }
    data.update(overrides)
    return data


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
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
    async def test_rejects_workspace_owned_by_another_user(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_get_workspace.return_value = {
            "workspace_id": WORKSPACE_ID, "user_id": OTHER,
        }

        with pytest.raises(HTTPException) as exc:
            await create_automation(
                OWNER, _create_data(agent_mode="ptc", workspace_id=WORKSPACE_ID),
            )

        assert exc.value.status_code == 403
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
    async def test_rejects_workspace_that_does_not_exist(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_get_workspace.return_value = None

        with pytest.raises(HTTPException) as exc:
            await create_automation(
                OWNER, _create_data(agent_mode="ptc", workspace_id=WORKSPACE_ID),
            )

        assert exc.value.status_code == 404
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
    async def test_rejects_foreign_workspace_even_in_flash_mode(
        self, mock_get_workspace, mock_auto_db,
    ):
        """Flash ignores workspace_id at run time, but a later PATCH to
        agent_mode='ptc' activates whatever was stored — so it is gated on the
        way in regardless of mode."""
        mock_get_workspace.return_value = {
            "workspace_id": WORKSPACE_ID, "user_id": OTHER,
        }

        with pytest.raises(HTTPException) as exc:
            await create_automation(
                OWNER, _create_data(agent_mode="flash", workspace_id=WORKSPACE_ID),
            )

        assert exc.value.status_code == 403
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch(_THREAD_OWNER, new_callable=AsyncMock)
    async def test_rejects_pinned_thread_owned_by_another_user(
        self, mock_thread_owner, mock_auto_db,
    ):
        mock_thread_owner.return_value = OTHER

        with pytest.raises(HTTPException) as exc:
            await create_automation(
                OWNER,
                _create_data(
                    thread_strategy="continue", conversation_thread_id=THREAD_ID,
                ),
            )

        assert exc.value.status_code == 403
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch(_THREAD_OWNER, new_callable=AsyncMock)
    async def test_rejects_pinned_thread_that_does_not_exist(
        self, mock_thread_owner, mock_auto_db,
    ):
        mock_thread_owner.return_value = None

        with pytest.raises(HTTPException) as exc:
            await create_automation(
                OWNER,
                _create_data(
                    thread_strategy="continue", conversation_thread_id=THREAD_ID,
                ),
            )

        assert exc.value.status_code == 404
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch(_THREAD_OWNER, new_callable=AsyncMock)
    @patch("src.server.handlers.automation_handler.get_workspace")
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
                agent_mode="ptc",
                workspace_id=WORKSPACE_ID,
                thread_strategy="continue",
                conversation_thread_id=THREAD_ID,
            ),
        )

        mock_auto_db.create_automation.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
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
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
    async def test_rejects_repointing_at_another_users_workspace(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_current_row())
        mock_get_workspace.return_value = {
            "workspace_id": WORKSPACE_ID, "user_id": OTHER,
        }

        with pytest.raises(HTTPException) as exc:
            await update_automation(
                AUTOMATION_ID, OWNER, {"workspace_id": WORKSPACE_ID},
            )

        assert exc.value.status_code == 403
        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch(_THREAD_OWNER, new_callable=AsyncMock)
    async def test_rejects_repointing_at_another_users_thread(
        self, mock_thread_owner, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_current_row())
        mock_thread_owner.return_value = OTHER

        with pytest.raises(HTTPException) as exc:
            await update_automation(
                AUTOMATION_ID, OWNER, {"conversation_thread_id": THREAD_ID},
            )

        assert exc.value.status_code == 403
        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
    async def test_untargeted_update_skips_the_lookup(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_current_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_current_row())

        await update_automation(AUTOMATION_ID, OWNER, {"name": "renamed"})

        mock_get_workspace.assert_not_called()
        mock_auto_db.update_automation.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
    async def test_missing_automation_returns_none_without_checking_targets(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=None)

        result = await update_automation(
            AUTOMATION_ID, OWNER, {"workspace_id": WORKSPACE_ID},
        )

        assert result is None
        mock_get_workspace.assert_not_called()
