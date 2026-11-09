"""An automation that names a bot runs as it, in that bot's own thread.

The binding is what keeps scheduled work inside the bot's conversation: the
same thread every run (unless the caller asked for per-run threads), and the
same identity/role the bot answers chat with.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.automation import AutomationCreate
from src.server.services.automations.lifecycle import effective_thread_strategy
from src.server.services.automation_executor import _automation_bot, _turn_place

BOT_ID = "44444444-4444-4444-4444-444444444444"
WS = "22222222-2222-2222-2222-222222222222"
BOT = {"bot_id": BOT_ID, "workspace_id": WS, "name": "Quant", "status": "active", "role": "analyst"}
WORKSPACE = {"workspace_id": WS, "user_id": "u-1", "status": "running", "name": "Research"}


def _create(bot_id=None, **extra) -> AutomationCreate:
    return AutomationCreate(
        name="Daily",
        trigger_type="cron",
        cron_expression="0 9 * * 1-5",
        instruction="do it",
        bot_id=bot_id,
        **extra,
    )


def test_a_bot_owned_automation_defaults_to_its_own_continuous_thread():
    assert effective_thread_strategy(_create(bot_id=BOT_ID)) == "continue"


def test_naming_new_keeps_per_run_threads():
    assert effective_thread_strategy(_create(bot_id=BOT_ID, thread_strategy="new")) == "new"


def test_without_a_bot_the_old_default_stays():
    assert effective_thread_strategy(_create()) == "new"


def test_the_model_still_reports_its_schema_default():
    assert _create().thread_strategy == "new"


@pytest.mark.asyncio
async def test_a_continue_run_lands_in_the_bots_private_thread():
    automation = {
        "automation_id": "a-1",
        "user_id": "u-1",
        "workspace_id": WS,
        "bot_id": BOT_ID,
        "thread_strategy": "continue",
    }
    with patch(
        "src.server.services.automation_executor._stored_workspace",
        AsyncMock(return_value=WORKSPACE),
    ), patch(
        "src.server.database.bots.get_bot", AsyncMock(return_value=BOT)
    ), patch(
        "src.server.services.bot_messages.delivery.ensure_private_thread",
        AsyncMock(return_value="th-private"),
    ) as ensure:
        place = await _turn_place(automation)
    assert place.thread_id == "th-private"
    assert place.bot["bot_id"] == BOT_ID
    ensure.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_paused_bot_stops_identifying_the_runs():
    with patch(
        "src.server.database.bots.get_bot", AsyncMock(return_value={**BOT, "status": "paused"})
    ):
        assert await _automation_bot({"automation_id": "a-1", "bot_id": BOT_ID}, WORKSPACE) is None


@pytest.mark.asyncio
async def test_a_bot_of_another_workspace_does_not_drag_the_run_there():
    with patch(
        "src.server.database.bots.get_bot",
        AsyncMock(return_value={**BOT, "workspace_id": "other-ws"}),
    ):
        assert await _automation_bot({"automation_id": "a-1", "bot_id": BOT_ID}, WORKSPACE) is None


@pytest.mark.asyncio
async def test_a_new_thread_strategy_does_not_force_the_private_thread():
    automation = {
        "automation_id": "a-1",
        "user_id": "u-1",
        "workspace_id": WS,
        "bot_id": BOT_ID,
        "thread_strategy": "new",
    }
    with patch(
        "src.server.services.automation_executor._stored_workspace",
        AsyncMock(return_value=WORKSPACE),
    ), patch(
        "src.server.database.bots.get_bot", AsyncMock(return_value=BOT)
    ), patch(
        "src.server.services.bot_messages.delivery.ensure_private_thread",
        AsyncMock(return_value="th-private"),
    ) as ensure:
        place = await _turn_place(automation)
    assert place.thread_id is None
    ensure.assert_not_awaited()
    assert place.bot["bot_id"] == BOT_ID
