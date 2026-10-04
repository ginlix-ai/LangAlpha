"""The flash tools' ``thread`` and ``delivery`` arguments, which read the
same rules as the automation files through the model and the lifecycle."""

from __future__ import annotations

import json

import pytest

from src.config import env
from src.tools.automation import tools

AUTOMATION_ID = "00000000-0000-4000-8000-0000000000a1"
THREAD_ID = "00000000-0000-4000-8000-0000000000b2"
CURRENT_THREAD_ID = "00000000-0000-4000-8000-0000000000c3"


def _config(thread_id: str | None = None) -> dict:
    configurable = {"user_id": "user-fake-1", "timezone": "America/New_York"}
    if thread_id:
        configurable["thread_id"] = thread_id
    return {"configurable": configurable}


def _row(**overrides) -> dict:
    row = {
        "automation_id": AUTOMATION_ID, "name": "A", "status": "active",
        "trigger_type": "cron", "cron_expression": "0 9 * * 1-5", "next_run_at": None,
    }
    row.update(overrides)
    return row


@pytest.fixture(autouse=True)
def _no_gateway(monkeypatch):
    """No messaging service, whatever the environment says: with one, a
    run's agent sends the results and nothing is warned about."""
    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
    monkeypatch.delenv("INTERNAL_SERVICE_TOKEN", raising=False)


@pytest.fixture
def created(monkeypatch):
    """What the tool hands the lifecycle to create."""
    seen = []

    async def create(user_id, data, **_kwargs):
        seen.append(data)
        return _row()

    monkeypatch.setattr(tools.lifecycle, "create_automation", create)
    return seen


@pytest.fixture
def updated(monkeypatch):
    """The changes the tool hands the lifecycle to apply."""
    seen = []

    async def update(automation_id, user_id, data):
        seen.append(data)
        return _row()

    monkeypatch.setattr(tools.lifecycle, "update_automation", update)
    return seen


async def _create(thread_id: str | None = None, **kwargs):
    content, _ = await tools.create_automation.coroutine(
        name="A", instruction="x", schedule="0 9 * * 1-5", config=_config(thread_id), **kwargs
    )
    return json.loads(content)


async def _update(thread_id: str | None = None, **kwargs):
    return await tools.manage_automation.coroutine(
        automation_id=AUTOMATION_ID, action="update", config=_config(thread_id), **kwargs
    )


# ---------------------------------------------------------------------------
# thread
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_pins_a_thread_named_by_its_id(created):
    result = await _create(thread=THREAD_ID.upper())

    assert result["success"] is True
    assert created[0].thread_strategy == "continue"
    assert str(created[0].conversation_thread_id) == THREAD_ID


@pytest.mark.asyncio
async def test_create_with_current_pins_the_conversation_it_is_written_from(created):
    await _create(thread_id=CURRENT_THREAD_ID, thread="current")

    assert created[0].thread_strategy == "continue"
    assert str(created[0].conversation_thread_id) == CURRENT_THREAD_ID


@pytest.mark.asyncio
async def test_create_without_a_thread_posts_each_run_to_a_new_one(created):
    await _create(thread_id=CURRENT_THREAD_ID)

    assert created[0].thread_strategy == "new"
    assert created[0].conversation_thread_id is None


@pytest.mark.parametrize(
    ("thread", "thread_id", "error"),
    [
        ("latest", CURRENT_THREAD_ID, 'thread: must be "new", "persistent", "current" or a thread id'),
        ("current", None, 'thread: "current" needs a conversation thread, and this run has none'),
    ],
)
@pytest.mark.asyncio
async def test_create_refuses_a_thread_it_cannot_name(created, thread, thread_id, error):
    assert await _create(thread_id=thread_id, thread=thread) == {"error": error}
    assert created == []


@pytest.mark.asyncio
async def test_update_pins_a_thread_named_by_its_id(updated):
    result = await _update(thread=THREAD_ID)

    assert result["success"] is True
    assert updated == [{"thread_strategy": "continue", "conversation_thread_id": THREAD_ID}]


def _stored(monkeypatch, **thread) -> None:
    async def get_automation(automation_id, user_id, **_kwargs):
        return _row(**thread)

    monkeypatch.setattr(tools.auto_db, "get_automation", get_automation)


@pytest.mark.parametrize(
    "thread",
    [
        {"thread_strategy": "new", "conversation_thread_id": None},
        {"thread_strategy": "continue", "conversation_thread_id": CURRENT_THREAD_ID, "owns_thread": False},
    ],
)
@pytest.mark.asyncio
async def test_update_to_persistent_clears_a_pinned_conversation(updated, monkeypatch, thread):
    _stored(monkeypatch, **thread)

    await _update(thread_id=CURRENT_THREAD_ID, thread="persistent")

    assert updated == [{"thread_strategy": "continue", "conversation_thread_id": None}]


@pytest.mark.parametrize(
    "thread",
    [
        {"thread_strategy": "continue", "conversation_thread_id": None},
        {"thread_strategy": "continue", "conversation_thread_id": THREAD_ID, "owns_thread": True},
    ],
)
@pytest.mark.asyncio
async def test_update_restating_persistent_keeps_its_own_thread(updated, monkeypatch, thread):
    """Clearing the pin would start the next run in a new thread."""
    _stored(monkeypatch, **thread)

    result = await _update(thread="persistent")

    assert result["success"] is True
    assert updated == [{"thread_strategy": "continue"}]


@pytest.mark.parametrize(
    ("thread", "thread_id", "error"),
    [
        ("latest", CURRENT_THREAD_ID, 'thread: must be "new", "persistent", "current" or a thread id'),
        ("current", None, 'thread: "current" needs a conversation thread, and this run has none'),
    ],
)
@pytest.mark.asyncio
async def test_update_refuses_a_thread_it_cannot_name(updated, thread, thread_id, error):
    assert await _update(thread_id=thread_id, thread=thread) == {"error": error}
    assert updated == []


# ---------------------------------------------------------------------------
# delivery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_warns_in_the_lifecycles_words_when_no_webhook_is_set(created, monkeypatch):
    monkeypatch.setattr(tools.lifecycle.settings, "AUTOMATION_WEBHOOK_URL", "")

    result = await _create(delivery="slack, ")

    assert created[0].delivery_config.methods == ["slack"]
    assert result["warning"] == tools.lifecycle.delivery_warning(["slack"])
    assert "AUTOMATION_WEBHOOK_URL is not configured" in result["warning"]


@pytest.mark.asyncio
async def test_update_warns_in_the_lifecycles_words_when_no_webhook_is_set(updated, monkeypatch):
    monkeypatch.setattr(tools.lifecycle.settings, "AUTOMATION_WEBHOOK_URL", "")

    result = await _update(delivery="slack")

    assert updated == [{"delivery_config": {"methods": ["slack"]}}]
    assert result["warning"] == tools.lifecycle.delivery_warning(["slack"])


@pytest.mark.asyncio
async def test_no_warning_once_the_webhook_is_set(created, updated, monkeypatch):
    monkeypatch.setattr(tools.lifecycle.settings, "AUTOMATION_WEBHOOK_URL", "https://example.com/hook")

    assert "warning" not in await _create(delivery="slack")
    assert "warning" not in await _update(delivery="slack")


@pytest.mark.asyncio
async def test_no_warning_without_delivery(created, updated, monkeypatch):
    monkeypatch.setattr(tools.lifecycle.settings, "AUTOMATION_WEBHOOK_URL", "")

    assert "warning" not in await _create()
    assert "warning" not in await _update(remove_delivery=True)
    assert updated == [{"delivery_config": {}}]
