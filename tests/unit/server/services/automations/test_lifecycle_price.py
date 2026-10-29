"""
Tests for price trigger handling in the automation lifecycle.

Covers create_automation and resume_automation for trigger_type='price':
next_run_at behavior and status constraints. What a price config must hold is
AutomationCreate's to check (test_automation_models.py).
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.automations import lifecycle
from src.server.services.automations.lifecycle import create_automation, resume_automation
from src.server.models.automation import AutomationCreate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = datetime.now(timezone.utc)
AUTO_ID = str(uuid.uuid4())
USER_ID = "test-user-price"
WORKSPACE_ID = str(uuid.uuid4())
HOME = get_home_workspace_id(USER_ID)


@pytest.fixture(autouse=True)
def _home(monkeypatch):
    """Home's row is the owner's, so an automation placed in Home needs no lookup."""
    monkeypatch.setattr(lifecycle, "_home", AsyncMock(return_value=HOME))

VALID_TRIGGER_CONFIG = {
    "symbol": "AAPL",
    "conditions": [
        {"type": "price_above", "value": 200.0},
    ],
}

VALID_TRIGGER_CONFIG_MULTI = {
    "symbol": "TSLA",
    "conditions": [
        {"type": "price_above", "value": 300.0},
        {"type": "pct_change_above", "value": 5.0, "reference": "previous_close"},
    ],
    "retrigger": {"mode": "recurring", "cooldown_seconds": 14400},
}


def _make_create_data(**overrides):
    """Build a minimal valid price-trigger create request."""
    data = {
        "name": "Price Alert",
        "trigger_type": "price",
        "instruction": "Notify me when the price crosses threshold",
        "trigger_config": VALID_TRIGGER_CONFIG,
        "timezone": "UTC",
        "workspace_id": HOME,
    }
    data.update(overrides)
    return AutomationCreate(**data)


def _make_automation_row(**overrides):
    """Build a fake automation DB row dict."""
    data = {
        "automation_id": AUTO_ID,
        "user_id": USER_ID,
        "name": "Price Alert",
        "description": None,
        "trigger_type": "price",
        "cron_expression": None,
        "timezone": "UTC",
        "trigger_config": VALID_TRIGGER_CONFIG,
        "next_run_at": None,
        "last_run_at": None,
        "instruction": "Notify me when the price crosses threshold",
        "workspace_id": HOME,
        "llm_model": None,
        "additional_context": None,
        "thread_strategy": "new",
        "conversation_thread_id": None,
        "status": "active",
        "max_failures": 3,
        "failure_count": 0,
        "delivery_config": None,
        "metadata": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# TestCreatePriceAutomation
# ---------------------------------------------------------------------------


class TestCreatePriceAutomation:
    """Tests for create_automation with trigger_type='price'."""

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_create_price_automation_success(self, mock_auto_db):
        """Successfully creates a price automation with valid trigger_config."""
        expected = _make_automation_row()
        mock_auto_db.create_automation = AsyncMock(return_value=expected)

        data = _make_create_data()
        result = await create_automation(USER_ID, data)

        assert result["automation_id"] == AUTO_ID
        assert result["trigger_type"] == "price"
        assert result["trigger_config"] == VALID_TRIGGER_CONFIG

        # Verify create_automation was called with next_run_at=None
        call_kwargs = mock_auto_db.create_automation.call_args.kwargs
        assert call_kwargs["next_run_at"] is None
        assert call_kwargs["trigger_type"] == "price"
        assert call_kwargs["trigger_config"] == VALID_TRIGGER_CONFIG

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_create_price_automation_with_retrigger(self, mock_auto_db):
        """Successfully creates a price automation with multi-condition + retrigger config."""
        expected = _make_automation_row(trigger_config=VALID_TRIGGER_CONFIG_MULTI)
        mock_auto_db.create_automation = AsyncMock(return_value=expected)

        data = _make_create_data(trigger_config=VALID_TRIGGER_CONFIG_MULTI)
        result = await create_automation(USER_ID, data)

        assert result["trigger_config"] == VALID_TRIGGER_CONFIG_MULTI
        mock_auto_db.create_automation.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_create_price_automation_next_run_at_is_none(self, mock_auto_db):
        """Price triggers set next_run_at to None (event-driven, not scheduled)."""
        expected = _make_automation_row()
        mock_auto_db.create_automation = AsyncMock(return_value=expected)

        data = _make_create_data()
        await create_automation(USER_ID, data)

        call_kwargs = mock_auto_db.create_automation.call_args.kwargs
        assert call_kwargs["next_run_at"] is None

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_create_price_automation_requires_workspace_id(self, mock_auto_db):
        """Every automation runs in some workspace, Home's for the user's Home."""
        data = _make_create_data(workspace_id=None)

        with pytest.raises(ValueError, match="workspace_id is required"):
            await create_automation(USER_ID, data)

        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.get_workspace")
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_create_price_automation_with_workspace_id(
        self, mock_auto_db, mock_get_workspace,
    ):
        """Successfully creates price automation when workspace_id provided."""
        expected = _make_automation_row(workspace_id=WORKSPACE_ID)
        mock_auto_db.create_automation = AsyncMock(return_value=expected)
        mock_get_workspace.return_value = {
            "workspace_id": WORKSPACE_ID, "user_id": USER_ID,
        }

        data = _make_create_data(workspace_id=uuid.UUID(WORKSPACE_ID))
        result = await create_automation(USER_ID, data)

        assert result["workspace_id"] == WORKSPACE_ID


# ---------------------------------------------------------------------------
# TestResumePriceAutomation
# ---------------------------------------------------------------------------


class TestResumePriceAutomation:
    """Tests for resume_automation with trigger_type='price'."""

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_resume_paused_price_automation(self, mock_auto_db):
        """Successfully resumes a paused price automation without next_run_at recalculation."""
        paused_row = _make_automation_row(status="paused")
        resumed_row = _make_automation_row(status="active", failure_count=0)

        mock_auto_db.get_automation = AsyncMock(return_value=paused_row)
        mock_auto_db.update_automation = AsyncMock(return_value=resumed_row)

        result = await resume_automation(AUTO_ID, USER_ID)

        assert result["status"] == "active"

        # Verify update was called with status=active, failure_count=0,
        # and NO next_run_at (price triggers don't use it)
        call_kwargs = mock_auto_db.update_automation.call_args.kwargs
        assert call_kwargs["status"] == "active"
        assert call_kwargs["failure_count"] == 0
        assert "next_run_at" not in call_kwargs

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_resume_disabled_price_automation(self, mock_auto_db):
        """Successfully resumes a disabled price automation (e.g. after max failures)."""
        disabled_row = _make_automation_row(status="disabled", failure_count=3)
        resumed_row = _make_automation_row(status="active", failure_count=0)

        mock_auto_db.get_automation = AsyncMock(return_value=disabled_row)
        mock_auto_db.update_automation = AsyncMock(return_value=resumed_row)

        result = await resume_automation(AUTO_ID, USER_ID)

        assert result["status"] == "active"

        call_kwargs = mock_auto_db.update_automation.call_args.kwargs
        assert call_kwargs["failure_count"] == 0
        assert "next_run_at" not in call_kwargs
        # Why it was switched off no longer applies, and goes in the same write.
        assert call_kwargs["disable_reason"] is None

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_resume_active_price_automation_raises(self, mock_auto_db):
        """Cannot resume a price automation that is already active."""
        active_row = _make_automation_row(status="active")
        mock_auto_db.get_automation = AsyncMock(return_value=active_row)

        with pytest.raises(ValueError, match="Cannot resume automation in 'active' status"):
            await resume_automation(AUTO_ID, USER_ID)

        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_resume_completed_price_automation_raises(self, mock_auto_db):
        """Cannot resume a completed price automation."""
        completed_row = _make_automation_row(status="completed")
        mock_auto_db.get_automation = AsyncMock(return_value=completed_row)

        with pytest.raises(ValueError, match="Cannot resume automation in 'completed' status"):
            await resume_automation(AUTO_ID, USER_ID)

        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.automations.lifecycle.auto_db")
    async def test_resume_not_found_returns_none(self, mock_auto_db):
        """Returns None when the automation does not exist."""
        mock_auto_db.get_automation = AsyncMock(return_value=None)

        result = await resume_automation(str(uuid.uuid4()), USER_ID)

        assert result is None
        mock_auto_db.update_automation.assert_not_called()
