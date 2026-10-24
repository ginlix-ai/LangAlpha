"""Pins how the SMTP method routes beside webhook methods, and where it is refused."""

import pytest

from src.config import settings
from src.server.services import webhook_client
from src.server.services.automations.lifecycle import DeliveryRefused, check_delivery
from src.server.services.webhook_client import WebhookClient

AUTOMATION = {
    "automation_id": "a1",
    "user_id": "u1",
    "name": "n",
    "delivery_config": {"methods": ["smtp", "slack"]},
}


@pytest.fixture
def sends(monkeypatch):
    calls = {"email": 0, "webhook": []}

    async def fake_email(self, automation, thread_id, run_id):
        calls["email"] += 1
        return {"method": "smtp", "success": True}

    async def fake_fire(self, url, payload, secret=None):
        calls["webhook"].append(payload["config"]["channel"])
        return True

    monkeypatch.setattr(WebhookClient, "_email_with_deadline", fake_email)
    monkeypatch.setattr(WebhookClient, "fire", fake_fire)
    monkeypatch.setattr(settings, "AUTOMATION_WEBHOOK_URL", "http://hook.test")
    return calls


@pytest.mark.asyncio
async def test_smtp_runs_beside_webhook_methods(sends):
    result = await WebhookClient().fire_event("automation.completed", AUTOMATION, "e", "t", None, run_id="r")
    assert sends == {"email": 1, "webhook": ["slack"]}
    assert {r["method"] for r in result} == {"smtp", "slack"}


@pytest.mark.asyncio
async def test_smtp_alone_is_not_sent_for_a_failed_run(sends):
    automation = {**AUTOMATION, "delivery_config": {"methods": ["smtp"]}}
    result = await WebhookClient().fire_event("automation.failed", automation, "e", "t", None)
    assert result is None
    assert sends == {"email": 0, "webhook": []}


@pytest.mark.asyncio
async def test_no_methods_sends_nothing(sends):
    automation = {**AUTOMATION, "delivery_config": {"methods": []}}
    assert await WebhookClient().fire_event("automation.completed", automation, "e", "t", None) is None
    assert sends == {"email": 0, "webhook": []}


def test_the_deadline_outlasts_four_socket_operations():
    assert webhook_client.EMAIL_DEADLINE_SECONDS > 4 * 10


@pytest.mark.asyncio
async def test_saving_smtp_is_refused_outside_oss(monkeypatch):
    monkeypatch.setattr(settings, "HOST_MODE", "platform")
    with pytest.raises(DeliveryRefused):
        await check_delivery("u1", ["smtp"])
    # A method already saved is left alone, whatever the mode now is.
    assert await check_delivery("u1", ["smtp"], stored=["smtp"]) == ["smtp"]


@pytest.mark.asyncio
async def test_saving_smtp_is_accepted_in_oss(monkeypatch):
    monkeypatch.setattr(settings, "HOST_MODE", "oss")
    assert await check_delivery("u1", ["smtp"]) == ["smtp"]
