"""Mode/workspace consistency on the automation PATCH path.

``create_automation`` refuses ``agent_mode='ptc'`` without a workspace. A PATCH
can flip the mode on its own, so the same requirement has to hold against the
merged state — otherwise the row is only rejected by the executor at run time,
which spends a failure against ``max_failures``.
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest

from src.server.handlers.automation_handler import update_automation

OWNER = "user-owner"
AUTOMATION_ID = str(uuid.uuid4())
WORKSPACE_ID = str(uuid.uuid4())


def _row(**overrides):
    row = {
        "automation_id": AUTOMATION_ID,
        "user_id": OWNER,
        "trigger_type": "cron",
        "cron_expression": "0 9 * * 1-5",
        "timezone": "UTC",
        "status": "active",
        "agent_mode": "flash",
        "workspace_id": None,
    }
    row.update(overrides)
    return row


class TestPtcRequiresAWorkspace:
    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    async def test_activating_ptc_without_any_workspace_is_rejected(self, mock_auto_db):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())

        with pytest.raises(ValueError, match="workspace_id is required"):
            await update_automation(AUTOMATION_ID, OWNER, {"agent_mode": "ptc"})

        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
    async def test_activating_ptc_with_a_workspace_in_the_same_patch_proceeds(
        self, mock_get_workspace, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_row(agent_mode="ptc"))
        mock_get_workspace.return_value = {
            "workspace_id": WORKSPACE_ID, "user_id": OWNER,
        }

        await update_automation(
            AUTOMATION_ID, OWNER,
            {"agent_mode": "ptc", "workspace_id": WORKSPACE_ID},
        )

        mock_auto_db.update_automation.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    @patch("src.server.handlers.automation_handler.get_workspace")
    async def test_activating_ptc_on_a_row_that_already_stores_one_proceeds(
        self, mock_get_workspace, mock_auto_db,
    ):
        """The stored workspace was ownership-checked when it was written, so
        the mode flip alone neither re-looks it up nor needs to."""
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(workspace_id=WORKSPACE_ID)
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row(agent_mode="ptc"))

        await update_automation(AUTOMATION_ID, OWNER, {"agent_mode": "ptc"})

        mock_get_workspace.assert_not_called()
        mock_auto_db.update_automation.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    async def test_leaving_ptc_for_flash_does_not_require_a_workspace(
        self, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_row(agent_mode="ptc"))
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(AUTOMATION_ID, OWNER, {"agent_mode": "flash"})

        mock_auto_db.update_automation.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.server.handlers.automation_handler.auto_db")
    async def test_unrelated_patch_on_a_ptc_row_is_not_falsely_rejected(
        self, mock_auto_db,
    ):
        mock_auto_db.get_automation = AsyncMock(
            return_value=_row(agent_mode="ptc", workspace_id=WORKSPACE_ID)
        )
        mock_auto_db.update_automation = AsyncMock(return_value=_row())

        await update_automation(AUTOMATION_ID, OWNER, {"name": "renamed"})

        mock_auto_db.update_automation.assert_called_once()
