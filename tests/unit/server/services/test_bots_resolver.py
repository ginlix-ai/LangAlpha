"""Which bot a turn runs as: the thread's own, else the place's default.

The resolver is the one seam between the bot layer and the turn: it must never
strand a legacy thread (stamp once, keep going), never let a thread carry a bot
from a place it no longer belongs to, and refuse a paused bot before any
durable work happens.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.bots.resolver import resolve_turn_bot

USER = "u-1"
HOME = get_home_workspace_id(USER)
WORKSPACE = {"workspace_id": "ws-1", "user_id": USER, "status": "running", "name": "Research"}
ANALYST = {"bot_id": "b-1", "user_id": USER, "name": "Research", "role": "analyst", "workspace_id": "ws-1", "status": "active"}
CHIEF = {"bot_id": "b-2", "user_id": USER, "name": "Chief of Staff", "role": "chief_of_staff", "workspace_id": HOME, "status": "active"}


def _patched(*, carried=None, place_default=None):
    """Patch the DB seams the resolver reaches; yields the context managers."""
    return (
        patch(
            "src.server.database.bots.get_bot_for_user",
            new=AsyncMock(return_value=carried),
        ),
        patch(
            "src.server.database.bots.ensure_workspace_bot",
            new=AsyncMock(return_value=place_default),
        ),
        patch(
            "src.server.database.bots.ensure_home_bot",
            new=AsyncMock(return_value=place_default),
        ),
        patch(
            "src.server.database.bots.bind_thread_bot",
            new=AsyncMock(),
        ),
    )


@pytest.mark.asyncio
async def test_a_thread_that_carries_its_bot_uses_it_without_a_default_read():
    get, ws_default, home_default, bind = _patched(carried=ANALYST)
    with get, ws_default as ensure_workspace, home_default, bind as bind_mock:
        bot = await resolve_turn_bot(
            USER, thread_id="t-1", thread_meta={"bot_id": "b-1"}, workspace=WORKSPACE
        )
    assert bot == ANALYST
    ensure_workspace.assert_not_awaited()
    bind_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_legacy_thread_is_stamped_with_the_place_default_once():
    get, ws_default, home_default, bind = _patched(place_default=ANALYST)
    with get, ws_default as ensure_workspace, home_default, bind as bind_mock:
        bot = await resolve_turn_bot(
            USER, thread_id="t-1", thread_meta={"bot_id": None}, workspace=WORKSPACE
        )
    assert bot == ANALYST
    ensure_workspace.assert_awaited_once()
    bind_mock.assert_awaited_once_with("t-1", "b-1", force=True)


@pytest.mark.asyncio
async def test_home_resolves_the_chief_of_staff_bot():
    get, ws_default, home_default, bind = _patched(place_default=CHIEF)
    with get, ws_default, home_default as ensure_home, bind:
        bot = await resolve_turn_bot(USER, thread_id="t-2", thread_meta={}, workspace=None)
    assert bot == CHIEF
    ensure_home.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_bot_from_another_workspace_does_not_drag_into_this_one():
    get, ws_default, home_default, bind = _patched(
        carried={**ANALYST, "workspace_id": "ws-old"}, place_default=ANALYST
    )
    with get, ws_default as ensure_workspace, home_default, bind:
        bot = await resolve_turn_bot(
            USER, thread_id="t-1", thread_meta={"bot_id": "b-1"}, workspace=WORKSPACE
        )
    assert bot["workspace_id"] == "ws-1"
    ensure_workspace.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_thread_whose_bot_is_gone_is_rebound_to_the_default():
    get, ws_default, home_default, bind = _patched(place_default=ANALYST)
    with get, ws_default, home_default, bind as bind_mock:
        bot = await resolve_turn_bot(
            USER, thread_id="t-1", thread_meta={"bot_id": "b-gone"}, workspace=WORKSPACE
        )
    assert bot == ANALYST
    bind_mock.assert_awaited_once_with("t-1", "b-1", force=True)


@pytest.mark.asyncio
async def test_a_thread_whose_bot_moved_is_rebound_to_this_place():
    get, ws_default, home_default, bind = _patched(
        carried={**ANALYST, "workspace_id": "ws-old"}, place_default=ANALYST
    )
    with get, ws_default, home_default, bind as bind_mock:
        bot = await resolve_turn_bot(
            USER, thread_id="t-1", thread_meta={"bot_id": "b-1"}, workspace=WORKSPACE
        )
    assert bot["workspace_id"] == "ws-1"
    bind_mock.assert_awaited_once_with("t-1", "b-1", force=True)


@pytest.mark.asyncio
async def test_a_paused_bot_is_refused_before_any_work():
    paused = {**ANALYST, "status": "paused"}
    get, ws_default, home_default, bind = _patched(carried=paused)
    with get, ws_default, home_default, bind:
        with pytest.raises(HTTPException) as caught:
            await resolve_turn_bot(
                USER, thread_id="t-1", thread_meta={"bot_id": "b-1"}, workspace=WORKSPACE
            )
    assert caught.value.status_code == 409
    assert caught.value.detail["type"] == "bot_paused"
