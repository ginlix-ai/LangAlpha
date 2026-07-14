"""F5 cross-worker cancel nudge (v4 Phase 2.4e).

Pins the nudge contract: best-effort publish keyed to the durable-intent
outcome, and a listener that only ever signals a RUN-TARGETED local cancel
(an untargeted one could kill a newer run than the intent was stamped on).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.turn_cancel_pubsub import (
    CANCEL_CHANNEL,
    TurnCancelListener,
    publish_cancel_nudge,
)

CACHE = "src.utils.cache.redis_cache.get_cache_client"


def _cache(enabled: bool = True) -> MagicMock:
    cache = MagicMock()
    cache.enabled = enabled
    cache.client = MagicMock() if enabled else None
    if enabled:
        cache.client.publish = AsyncMock()
    return cache


@pytest.mark.asyncio
async def test_publish_sends_thread_and_run_on_the_channel():
    cache = _cache()
    with patch(CACHE, return_value=cache):
        await publish_cancel_nudge("t-1", "r-1")

    channel, payload = cache.client.publish.await_args.args
    assert channel == CANCEL_CHANNEL
    assert json.loads(payload) == {"thread_id": "t-1", "run_id": "r-1"}


@pytest.mark.asyncio
async def test_publish_is_best_effort():
    disabled = _cache(enabled=False)
    with patch(CACHE, return_value=disabled):
        await publish_cancel_nudge("t-1", "r-1")  # no raise

    erroring = _cache()
    erroring.client.publish = AsyncMock(side_effect=ConnectionError("down"))
    with patch(CACHE, return_value=erroring):
        await publish_cancel_nudge("t-1", "r-1")  # no raise


@pytest.mark.asyncio
async def test_listener_dispatches_run_targeted_local_cancel():
    listener = TurnCancelListener()
    btm = MagicMock()
    btm.cancel_workflow = AsyncMock(return_value=True)
    with patch(
        "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
        return_value=btm,
    ):
        await listener._handle(
            json.dumps({"thread_id": "t-1", "run_id": "r-1"}).encode()
        )

    btm.cancel_workflow.assert_awaited_once_with("t-1", "r-1")


@pytest.mark.asyncio
async def test_listener_drops_untargeted_and_malformed_payloads():
    listener = TurnCancelListener()
    btm = MagicMock()
    btm.cancel_workflow = AsyncMock()
    with patch(
        "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
        return_value=btm,
    ):
        await listener._handle(json.dumps({"thread_id": "t-1"}))  # no run_id
        await listener._handle(json.dumps({"run_id": "r-1"}))  # no thread_id
        await listener._handle(b"\xff not json")  # undecodable

    btm.cancel_workflow.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_workflow_publishes_only_for_unhandled_stamped_intent():
    """The /cancel handler nudges iff durable intent landed AND no local
    executor consumed the signal — the remote-owner case."""
    from src.server.handlers import workflow_handler as wh

    manager = MagicMock()
    manager.has_active_task_for_thread = AsyncMock(return_value=True)

    async def _run(cancel_success: bool, intent_state: str):
        manager.cancel_workflow = AsyncMock(return_value=cancel_success)
        publish = AsyncMock()
        with (
            patch(
                "src.server.services.background_task_manager."
                "BackgroundTaskManager.get_instance",
                return_value=manager,
            ),
            patch(
                "src.server.database.turn_lifecycle.get_active_run",
                AsyncMock(
                    return_value={"conversation_response_id": "r-1"}
                ),
            ),
            patch(
                "src.server.database.turn_lifecycle.request_run_cancel",
                AsyncMock(return_value={"state": intent_state}),
            ),
            patch(
                "src.server.services.turn_cancel_pubsub.publish_cancel_nudge",
                publish,
            ),
        ):
            await wh.cancel_workflow("t-1")
        return publish

    # Remote owner: intent stamped, no local task -> nudge.
    publish = await _run(cancel_success=False, intent_state="requested")
    publish.assert_awaited_once_with("t-1", "r-1")

    # Local owner handled it -> no nudge.
    publish = await _run(cancel_success=True, intent_state="requested")
    publish.assert_not_awaited()

    # Already terminal -> nothing to nudge.
    publish = await _run(cancel_success=False, intent_state="already_terminal")
    publish.assert_not_awaited()
