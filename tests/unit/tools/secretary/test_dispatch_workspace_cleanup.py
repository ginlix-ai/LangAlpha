"""Auto-created workspace lifecycle around dispatch cap rejection.

Dispatch without a ``workspace_id`` provisions a workspace (real sandbox,
~8-10s) before ``reserve()`` admits the dispatch. A cap-rejected dispatch must
not leak that sandbox: a deterministic cap hit is pre-checked BEFORE
provisioning (``check_dispatch_capacity``), and the residual pre-check/reserve
race deletes the just-created workspace on ``slot.error`` — the dispatch HTTP
was never sent, so the workspace is provably unused.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat import report_back as rb
from src.tools.secretary.tools import ptc_agent
from tests.unit.server.handlers.chat.redis_fakes import FakeCache as _FakeCache

USER_ID = "user-1"
FLASH_THREAD_ID = "flash-thread-1"
NEW_WORKSPACE_ID = "33333333-3333-3333-3333-333333333333"


def _tool_call(args: dict, call_id: str = "call_test") -> dict:
    return {"name": "ptc_agent", "args": args, "id": call_id, "type": "tool_call"}


def _config() -> dict:
    # thread_id = the dispatching flash thread -> report_back wiring is live.
    return {"configurable": {"user_id": USER_ID, "thread_id": FLASH_THREAD_ID}}


def _payload(result) -> dict:
    return json.loads(result.update["messages"][0].content)


def _manager(delete: AsyncMock | None = None) -> MagicMock:
    mgr = MagicMock()
    mgr.create_workspace = AsyncMock(return_value={"workspace_id": NEW_WORKSPACE_ID})
    mgr.delete_workspace = delete or AsyncMock(return_value=True)
    return mgr


@pytest.fixture
def cache(monkeypatch):
    c = _FakeCache()
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: c)
    return c


def _fill_flash_cap(cache) -> None:
    cache.client.sets[rb.flash_watch_key(FLASH_THREAD_ID)] = {
        f"p{i}" for i in range(rb.MAX_DISPATCH_PER_FLASH)
    }


@pytest.mark.asyncio
async def test_precheck_rejection_skips_workspace_creation(cache):
    """A deterministic cap hit fails BEFORE any sandbox is provisioned."""
    _fill_flash_cap(cache)
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch must not run")),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert "too many concurrent analyses" in payload["error"]
    mgr.create_workspace.assert_not_awaited()
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_reserve_rejection_deletes_auto_created_workspace(cache):
    """The pre-check/reserve race path: the cap fills between the pre-check and
    reserve(), so the just-created workspace must be deleted, not leaked."""
    _fill_flash_cap(cache)
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        # Simulate the race: the pre-check saw capacity, reserve() did not.
        "src.server.handlers.chat.report_back.check_dispatch_capacity",
        new=AsyncMock(return_value=None),
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch must not run")),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert "too many concurrent analyses" in payload["error"]
    mgr.create_workspace.assert_awaited_once()
    mgr.delete_workspace.assert_awaited_once_with(NEW_WORKSPACE_ID)


@pytest.mark.asyncio
async def test_cleanup_failure_still_returns_the_cap_error(cache):
    """A failed best-effort delete must not mask the cap rejection."""
    _fill_flash_cap(cache)
    mgr = _manager(delete=AsyncMock(side_effect=RuntimeError("sandbox teardown failed")))
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "src.server.handlers.chat.report_back.check_dispatch_capacity",
        new=AsyncMock(return_value=None),
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch must not run")),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert "too many concurrent analyses" in payload["error"]
    mgr.delete_workspace.assert_awaited_once()
