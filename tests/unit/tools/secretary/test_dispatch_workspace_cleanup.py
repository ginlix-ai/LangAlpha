"""Auto-created workspace lifecycle around failed dispatches.

Dispatch without a ``workspace_id`` provisions a workspace (real sandbox,
~8-10s) before ``reserve()`` admits the dispatch. A failed dispatch must not
leak that sandbox when the failure proves the run never started: a
deterministic cap hit is pre-checked BEFORE provisioning
(``check_dispatch_capacity``), the residual pre-check/reserve race deletes the
just-created workspace on ``slot.error`` (no HTTP was sent), and a >=400
dispatch response does too (the endpoint's error paths all precede its
create_task). Timeout/connection failures deliberately KEEP the workspace —
the run may have started server-side.
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


class _FakeResp:
    def __init__(self, status: int = 200, body: dict | None = None) -> None:
        self.status = status
        self._body = body if body is not None else {"status": "dispatched"}

    async def __aenter__(self) -> "_FakeResp":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    async def json(self) -> dict:
        return self._body


class _FakeSession:
    def __init__(
        self, resp: _FakeResp | None = None, post_exc: Exception | None = None
    ) -> None:
        self._resp = resp
        self._post_exc = post_exc

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    def post(self, *_args, **_kwargs) -> _FakeResp:
        if self._post_exc is not None:
            raise self._post_exc
        return self._resp


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


@pytest.mark.asyncio
async def test_dispatch_error_status_deletes_auto_created_workspace(cache):
    """A >=400 dispatch response (e.g. the credit gate) proves the run never
    started — the endpoint's error paths all precede its create_task — so the
    just-created workspace is deleted, not leaked."""
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(_FakeResp(status=402))
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    mgr.create_workspace.assert_awaited_once()
    mgr.delete_workspace.assert_awaited_once_with(NEW_WORKSPACE_ID)


@pytest.mark.asyncio
async def test_dispatch_timeout_keeps_auto_created_workspace(cache):
    """A timed-out dispatch may have started the run server-side — the
    workspace must NOT be deleted out from under it."""
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(post_exc=TimeoutError())
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_timeout"
    mgr.create_workspace.assert_awaited_once()
    mgr.delete_workspace.assert_not_awaited()


# ---------------------------------------------------------------------------
# A configured INTERNAL_SERVICE_TOKEN is the normal production state and is now
# a precondition for dispatch (the ptc_agent / _post_report_back preflight guard
# aborts without it, to avoid a silent foreground credit burn). These tests
# exercise the dispatch path itself, not the guard, so give the whole module a
# token. The dedicated guard test module asserts the unset behaviour separately.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _internal_service_token(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "test-internal-service-token")
