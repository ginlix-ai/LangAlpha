"""The automation event a delivery receiver reads.

Its keys are a contract with the receiver, which reads the report from
``run_id`` rather than the thread's latest turn and words a failure the user
acts on from ``failure_reason``.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.webhook_client import WebhookClient

_AUTOMATION = {
    "automation_id": "auto-1",
    "name": "Morning briefing",
    "user_id": "user-1",
    "agent_mode": "flash",
    "delivery_config": {"methods": ["slack", "discord"]},
}


@pytest.mark.asyncio
async def test_each_method_gets_the_event_with_its_run_and_reason():
    client = WebhookClient()
    with patch("src.config.settings.AUTOMATION_WEBHOOK_URL", "http://hook.example.com"), \
         patch("src.config.settings.AUTOMATION_WEBHOOK_SECRET", ""), \
         patch.object(client, "fire", AsyncMock(side_effect=[True, False])) as fire:
        results = await client.fire_event(
            "automation.failed", _AUTOMATION, "exec-1", "thread-1", "ws-1",
            error="Out of credits.", run_id="run-1", failure_reason="usage_limit",
        )

    assert results == [
        {"method": "slack", "success": True},
        {"method": "discord", "success": False},
    ]
    payloads = [c.args[1] for c in fire.await_args_list]
    assert [p["config"] for p in payloads] == [{"channel": "slack"}, {"channel": "discord"}]
    assert {k: payloads[0][k] for k in (
        "event", "automation_id", "execution_id", "thread_id", "run_id",
        "failure_reason", "user_id", "workspace_id", "error",
    )} == {
        "event": "automation.failed",
        "automation_id": "auto-1",
        "execution_id": "exec-1",
        "thread_id": "thread-1",
        "run_id": "run-1",
        "failure_reason": "usage_limit",
        "user_id": "user-1",
        "workspace_id": "ws-1",
        "error": "Out of credits.",
    }


@pytest.mark.asyncio
async def test_no_method_sends_nothing():
    client = WebhookClient()
    with patch.object(client, "fire", AsyncMock()) as fire:
        assert await client.fire_event(
            "automation.completed", {**_AUTOMATION, "delivery_config": {}},
            "exec-1", None, None,
        ) is None
    fire.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_post_that_raises_is_a_failed_method_not_an_error():
    # ``fire`` is the only guard between a delivery and the settle that sent it.
    with patch(
        "src.server.services.webhook_client.httpx.AsyncClient",
        side_effect=OSError("connection refused"),
    ):
        assert await WebhookClient().fire(
            "http://hook.example.com", {"event": "automation.failed"}, "secret"
        ) is False
