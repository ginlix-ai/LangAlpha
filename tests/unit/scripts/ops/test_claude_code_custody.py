"""The operator's purge reaches every user it names, and only those."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

import pytest

from scripts.ops import claude_code_custody as script


def _purging(drop: AsyncMock, users: list[str]) -> ExitStack:
    stack = ExitStack()
    pool = SimpleNamespace(open=AsyncMock(), close=AsyncMock())
    for target, value in [
        ("src.server.database.pool.get_or_create_pool", lambda: pool),
        ("src.server.database.claude_code_hosted.stored_users", AsyncMock(return_value=users)),
        ("src.server.services.claude_code.hosted.drop_stored", drop),
        ("src.utils.cache.redis_cache.init_cache", AsyncMock()),
        ("src.utils.cache.redis_cache.close_cache", AsyncMock()),
    ]:
        stack.enter_context(patch(target, value))
    return stack


@pytest.mark.asyncio
async def test_purge_carries_on_past_a_user_whose_drop_fails_and_exits_failed():
    drop = AsyncMock(side_effect=[RuntimeError("a turn did not stop"), 2])
    with _purging(drop, ["user-1", "user-2"]):
        code = await script.run(argparse.Namespace(command="purge", all=True, user=None))
    assert drop.await_args_list == [call("user-1"), call("user-2")]
    assert code == 1


def test_purge_refuses_a_blank_user_rather_than_purging_everyone(monkeypatch):
    monkeypatch.setattr("sys.argv", ["claude_code_custody.py", "purge", "--user", ""])
    with (
        patch("src.config.settings.HOST_MODE", "platform"),
        patch.object(script, "point_app_pool_at"),
        patch.object(script, "build_db_uri", lambda: "postgresql://ops"),
        patch.object(script.asyncio, "run") as ran,
        pytest.raises(SystemExit) as exited,
    ):
        script.main()
    assert exited.value.code == 2
    ran.assert_not_called()
