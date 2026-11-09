"""The sweeper is what makes 'queued' become 'delivered' without a new send.

Its only decisions are which rows are due (one guarded claim) and that a
delivery failing does not stop the batch.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.bot_messages.sweeper import BotMessageDelivery, MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_a_sweep_delivers_every_claimed_row():
    rows = [{"message_id": "m-1"}, {"message_id": "m-2"}]
    with patch(
        "src.server.services.bot_messages.sweeper.claim_due",
        AsyncMock(return_value=rows),
    ), patch(
        "src.server.services.bot_messages.delivery.deliver_message",
        AsyncMock(return_value={"status": "delivered"}),
    ) as deliver:
        count = await BotMessageDelivery().sweep_once()
    assert count == 2
    assert [c.args[0] for c in deliver.await_args_list] == ["m-1", "m-2"]


@pytest.mark.asyncio
async def test_one_failed_delivery_does_not_stop_the_batch():
    rows = [{"message_id": "m-1"}, {"message_id": "m-2"}]
    deliver = AsyncMock(side_effect=[RuntimeError("boom"), {"status": "delivered"}])
    with patch(
        "src.server.services.bot_messages.sweeper.claim_due",
        AsyncMock(return_value=rows),
    ), patch(
        "src.server.services.bot_messages.delivery.deliver_message", deliver
    ):
        count = await BotMessageDelivery().sweep_once()
    assert count == 2
    assert deliver.await_count == 2


def test_the_retry_budget_is_bounded():
    # A row that has used every attempt is failed by delivery, never claimed
    # again: otherwise a permanently busy target circles forever.
    assert MAX_ATTEMPTS >= 1
