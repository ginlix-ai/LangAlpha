"""Who may start a Claude sign-in (the ``claude_oauth`` flag, or one stored),
and what the Claude status says of it: that, and the day it ends on a
deployment that set one."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.config.models import ClaudeCodeConfig
from src.server.app import oauth as oauth_app
from src.server.app.oauth import ClaudeCallbackRequest
from src.server.services import claude_oauth

_TODAY = datetime.now(timezone.utc).date()


@pytest.fixture
def flag(monkeypatch):
    enabled = AsyncMock(return_value=False)
    monkeypatch.setattr(oauth_app, "user_feature_enabled", enabled)
    return enabled


@pytest.fixture
def stored(monkeypatch):
    row = AsyncMock(return_value=None)
    monkeypatch.setattr(oauth_app, "get_oauth_connection", row)
    return row


def _ends_on(monkeypatch, day: date | None):
    monkeypatch.setattr(claude_oauth, "get_claude_code_config", MagicMock(return_value=ClaudeCodeConfig(claude_oauth_ends_on=day)))


@pytest.mark.asyncio
async def test_a_user_the_flag_leaves_out_cannot_start_a_sign_in(flag, stored, monkeypatch):
    sign_in = MagicMock()
    monkeypatch.setattr(oauth_app, "PendingSignIn", sign_in)
    with pytest.raises(HTTPException) as raised:
        await oauth_app.claude_initiate("u1")
    assert raised.value.status_code == 403
    assert raised.value.error["code"] == "unavailable"
    flag.assert_awaited_once_with("u1", "claude_oauth")
    sign_in.assert_not_called()


@pytest.mark.asyncio
async def test_nor_finish_one_begun_before(flag, stored):
    with patch("src.utils.cache.redis_cache.get_cache_client") as cache:
        with pytest.raises(HTTPException) as raised:
            await oauth_app.claude_callback("u1", ClaudeCallbackRequest(callback_input="code#state"))
    assert raised.value.error["code"] == "unavailable"
    cache.return_value.acquire_lock.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("granted, row", [(True, None), (False, {"account_id": "a"})])
async def test_the_flag_or_a_stored_sign_in_lets_one_start(flag, stored, monkeypatch, granted, row):
    flag.return_value = granted
    stored.return_value = row
    sign_in = MagicMock()
    sign_in.return_value.start = AsyncMock(return_value={"authorize_url": "u"})
    monkeypatch.setattr(oauth_app, "PendingSignIn", sign_in)
    assert await oauth_app.claude_initiate("u1") == {"authorize_url": "u"}


@pytest.mark.asyncio
@pytest.mark.parametrize("granted, row, can", [(False, None, False), (True, None, True), (False, {"account_id": "a"}, True)])
async def test_the_claude_status_says_whether_the_user_may_sign_in(flag, stored, monkeypatch, granted, row, can):
    flag.return_value = granted
    stored.return_value = row
    _ends_on(monkeypatch, None)
    assert (await oauth_app.oauth_status("claude", "u1")).can_sign_in is can


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "day, served",
    [(_TODAY + timedelta(days=18), True), (_TODAY, True), (_TODAY - timedelta(days=1), False), (None, False)],
)
async def test_the_claude_status_carries_the_day_it_ends_until_it_passes(flag, stored, monkeypatch, day, served):
    _ends_on(monkeypatch, day)
    status = await oauth_app.oauth_status("claude", "u1")
    assert status.ends_on == (day.isoformat() if served else None)


@pytest.mark.asyncio
async def test_other_sign_ins_are_open_to_everyone_and_carry_no_end(flag, stored, monkeypatch):
    _ends_on(monkeypatch, _TODAY + timedelta(days=18))
    status = await oauth_app.oauth_status("codex", "u1")
    assert status.can_sign_in is True and status.ends_on is None
    flag.assert_not_awaited()
