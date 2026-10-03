"""Fixtures for the tests of an automation's delivery chats."""

from __future__ import annotations

import httpx
import pytest

from src.config import env
from src.tools.messaging import tools as messaging
from tests.unit.server.services.automations._check_target import FakeCheckTarget


@pytest.fixture
def check_target(monkeypatch) -> FakeCheckTarget:
    """A deployment with the channel gateway, which checks every chat."""
    fake = FakeCheckTarget()
    transport = httpx.MockTransport(fake.handle)
    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "http://gateway.test/api/prefix")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "svc-token")
    monkeypatch.setattr(
        messaging,
        "_client",
        lambda timeout: httpx.AsyncClient(transport=transport, timeout=timeout),
    )
    return fake


@pytest.fixture
def no_gateway(monkeypatch) -> None:
    """A deployment without the channel gateway."""
    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
    monkeypatch.delenv("INTERNAL_SERVICE_TOKEN", raising=False)
