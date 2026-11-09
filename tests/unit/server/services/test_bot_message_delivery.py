"""Delivery turns one queued message into one framed turn, or leaves it queued.

The frame is the contract the model reads; the outcome map is what keeps a
busy target's message from being lost or delivered twice.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.bot_messages.delivery import deliver_message, sender_frame

USER = "user-1"
WS = "22222222-2222-2222-2222-222222222222"
THREAD = "33333333-3333-3333-3333-333333333333"
SENDER = {"bot_id": "b-1", "name": "Quant"}
TARGET = {"bot_id": "b-2", "name": "Scout", "user_id": USER, "workspace_id": WS, "status": "active"}

MESSAGE = {
    "message_id": "m-1",
    "user_id": USER,
    "from_type": "bot",
    "from_id": "b-1",
    "to_bot_id": "b-2",
    "kind": "request",
    "content": "check the filings",
    "payload": {"depth": 2},
    "root_id": "99999999-9999-9999-9999-999999999999",
    "status": "queued",
    "thread_id": None,
}


def test_the_frame_names_the_sender_and_the_kind():
    out = sender_frame("Quant", "request")
    assert out.startswith("[Request from Quant]")
    assert "asking you to do this" in out
    assert sender_frame("Quant", "message").startswith("[Message from Quant]")


def _world(*, target=TARGET, outcome="dispatched"):
    return (
        patch(
            "src.server.database.bot_messages.get_message",
            AsyncMock(return_value=dict(MESSAGE)),
        ),
        patch(
            "src.server.database.bots.get_bot",
            AsyncMock(side_effect=lambda bot_id: SENDER if bot_id == "b-1" else target),
        ),
        patch(
            "src.server.services.bot_messages.delivery.ensure_private_thread",
            AsyncMock(return_value=THREAD),
        ),
        patch(
            "src.server.services.report_back.notify_turn.post_notification_turn",
            AsyncMock(return_value=(outcome, "run-1")),
        ),
        patch(
            "src.server.database.bot_messages.set_status",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.server.database.bot_messages.end_attempt",
            AsyncMock(),
        ),
        patch(
            "src.server.database.runs.lifecycle.get_latest_attempt",
            AsyncMock(return_value=None),
        ),
        patch(
            "src.server.services.report_back.notify_turn._holds_notifications",
            AsyncMock(return_value=False),
        ),
    )


@pytest.mark.asyncio
async def test_a_dispatched_message_is_recorded_delivered_with_its_thread():
    get, bots_get, thread, post, status, end, latest, holds = _world()
    with get, bots_get, thread, post as posted, status as mark, end, latest, holds:
        result = await deliver_message("m-1")
    assert result["status"] == "delivered"
    mark.assert_awaited_once_with("m-1", "delivered", thread_id=THREAD)
    body = posted.call_args.kwargs["body"]
    assert body["workspace_id"] == WS
    assert body["messages"][0]["content"].startswith("[Request from Quant]")
    # The wire's request_key is a UUID column: a prefixed string is a 422.
    import uuid as _uuid

    from src.server.models.chat import ChatRequest

    _uuid.UUID(body["request_key"])
    ChatRequest(
        messages=[{"role": "user", "content": "x"}],
        request_key=body["request_key"],
    )
    assert body["suppress_push"] is True
    # Display only: the thread draws "Request from Quant", not a user bubble.
    assert body["bot_message_source"] == {
        "kind": "bot_message",
        "message_id": "m-1",
        "from_bot_id": "b-1",
        "from_name": "Quant",
        "message_kind": "request",
        "body": "check the filings",
        "root_id": "99999999-9999-9999-9999-999999999999",
        "depth": 2,
    }


@pytest.mark.asyncio
async def test_a_busy_target_leaves_the_message_queued():
    get, bots_get, thread, post, status, end, latest, holds = _world(outcome="cap")
    with get, bots_get, thread, post, status as mark, end as finish, latest, holds:
        result = await deliver_message("m-1")
    assert result["status"] == "queued"
    mark.assert_not_awaited()
    finish.assert_awaited_once_with("m-1")


@pytest.mark.asyncio
async def test_a_dropped_message_fails_rather_than_circling():
    get, bots_get, thread, post, status, end, latest, holds = _world(outcome="drop")
    with get, bots_get, thread, post, status as mark, end, latest, holds:
        result = await deliver_message("m-1")
    assert result["status"] == "failed"
    mark.assert_awaited_once_with("m-1", "failed")


@pytest.mark.asyncio
async def test_a_paused_target_is_not_delivered():
    paused = {**TARGET, "status": "paused"}
    get, bots_get, thread, post, status, end, latest, holds = _world(target=paused)
    with get, bots_get, thread, post, status as mark, end, latest, holds:
        result = await deliver_message("m-1")
    assert result == {"status": "failed", "thread_id": None, "outcome": "target_paused"}
    mark.assert_awaited_once_with("m-1", "failed")


@pytest.mark.asyncio
async def test_a_retried_message_that_is_no_longer_queued_is_not_posted_again():
    done = {**MESSAGE, "status": "delivered", "thread_id": THREAD}
    with patch(
        "src.server.database.bot_messages.get_message", AsyncMock(return_value=done)
    ), patch(
        "src.server.services.report_back.notify_turn.post_notification_turn",
        AsyncMock(side_effect=AssertionError("must not post twice")),
    ):
        result = await deliver_message("m-1")
    assert result["status"] == "delivered"
    assert result["outcome"] == "already"


@pytest.mark.asyncio
async def test_a_thread_holding_a_card_keeps_the_message_queued_without_spending_an_attempt():
    from src.server.database.bot_messages import release_message

    get, bots_get, thread, post, status, end, latest, holds = _world()
    with get, bots_get, thread, post, status as mark, end, latest, patch(
        "src.server.services.report_back.notify_turn._holds_notifications",
        AsyncMock(return_value=True),
    ), patch(
        "src.server.database.bot_messages.release_message",
        AsyncMock(),
    ) as released:
        result = await deliver_message("m-1")
    assert result == {"status": "queued", "thread_id": THREAD, "outcome": "busy"}
    mark.assert_not_awaited()
    released.assert_awaited_once_with("m-1")
    del release_message


@pytest.mark.asyncio
async def test_an_attempt_past_the_retry_ceiling_fails_the_row():
    get, bots_get, thread, post, status, end, latest, holds = _world(outcome="cap")
    with get, bots_get, thread, post, status as mark, end as finish, latest, holds:
        result = await deliver_message("m-1", attempts=5)
    assert result["status"] == "failed"
    assert result["outcome"] == "attempts_exhausted"
    mark.assert_awaited_once_with("m-1", "failed")
    finish.assert_not_awaited()
