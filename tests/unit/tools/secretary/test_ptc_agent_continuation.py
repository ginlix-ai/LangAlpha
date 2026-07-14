"""Regression coverage for ``ptc_agent`` continuation ownership check.

The continuation branch (``thread_id`` provided) used to verify ownership by
reading ``thread.get("user_id")`` off ``get_thread_by_id``. But that helper
never selects ``user_id`` (and ``conversation_threads`` has no such column —
ownership lives on ``workspaces.user_id`` via the FK), so the value was always
``None`` and every continuation errored with "thread not found" before any
dispatch could happen.

The fix reuses ``_verify_thread_owner`` (which JOINs ``workspaces``). These
tests pin that a legitimate owner reaches dispatch and a non-owner is rejected.
"""

from __future__ import annotations

import contextlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.tools.secretary.tools import ptc_agent
from tests.unit.server.handlers.chat.redis_fakes import FakeCache as _FakeCache

USER_ID = "user-1"
PTC_THREAD_ID = "11111111-1111-1111-1111-111111111111"
WORKSPACE_ID = "22222222-2222-2222-2222-222222222222"


def _tool_call(args: dict, call_id: str = "call_test") -> dict:
    """Build a ToolCall-shaped dict so ``ainvoke`` injects ``tool_call_id``."""
    return {"name": "ptc_agent", "args": args, "id": call_id, "type": "tool_call"}


def _config(user_id: str | None = USER_ID) -> dict:
    # Deliberately omit ``thread_id`` so the report_back Redis branch is a
    # no-op (flash_thread_id is None) and we don't have to mock the cache.
    return {"configurable": {"user_id": user_id}}


def _payload(result) -> dict:
    """Decode the JSON body of the ToolMessage carried by the Command."""
    message = result.update["messages"][0]
    return json.loads(message.content)


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
    def __init__(self, resp: _FakeResp) -> None:
        self._resp = resp

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    def post(self, *_args, **_kwargs) -> _FakeResp:
        return self._resp


@pytest.mark.asyncio
async def test_continuation_owner_match_reaches_dispatch():
    """A thread the user owns proceeds past the ownership check to dispatch."""
    owner = AsyncMock(return_value=USER_ID)
    by_id = AsyncMock(return_value={
        "conversation_thread_id": PTC_THREAD_ID,
        "workspace_id": WORKSPACE_ID,
    })

    with patch(
        "src.server.database.conversation.get_thread_owner_id", owner
    ), patch(
        "src.server.database.conversation.get_thread_by_id", by_id
    ), patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(_FakeResp())
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "follow up please", "thread_id": PTC_THREAD_ID}),
            config=_config(),
        )

    payload = _payload(result)
    assert payload.get("success") is True, payload
    assert payload.get("status") == "dispatched", payload
    # Continuation preserves the existing thread and resolves its workspace.
    assert payload.get("thread_id") == PTC_THREAD_ID
    assert payload.get("workspace_id") == WORKSPACE_ID
    owner.assert_awaited_once_with(PTC_THREAD_ID)


@pytest.mark.asyncio
async def test_continuation_owner_mismatch_returns_thread_not_found():
    """A thread owned by someone else is rejected before dispatch."""
    owner = AsyncMock(return_value="someone-else")
    by_id = AsyncMock(return_value={
        "conversation_thread_id": PTC_THREAD_ID,
        "workspace_id": WORKSPACE_ID,
    })
    # If ownership were (wrongly) accepted, this would blow up — proving the
    # guard returned before any dispatch attempt.
    dispatch = MagicMock(side_effect=AssertionError("dispatch must not run"))

    with patch(
        "src.server.database.conversation.get_thread_owner_id", owner
    ), patch(
        "src.server.database.conversation.get_thread_by_id", by_id
    ), patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession", dispatch
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "follow up please", "thread_id": PTC_THREAD_ID}),
            config=_config(),
        )

    payload = _payload(result)
    assert payload.get("success") is False, payload
    assert "thread not found" in payload.get("error", ""), payload
    dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_continuation_normalizes_noncanonical_thread_id():
    """A non-canonical id (urn:uuid:) is normalized once so the owner check and
    the fetch bind the same canonical form — the fetch can't 22P02 after the
    owner check (which normalizes internally) has already passed."""
    raw = f"urn:uuid:{PTC_THREAD_ID}"
    owner = AsyncMock(return_value=USER_ID)
    by_id = AsyncMock(return_value={
        "conversation_thread_id": PTC_THREAD_ID,
        "workspace_id": WORKSPACE_ID,
    })

    with patch(
        "src.server.database.conversation.get_thread_owner_id", owner
    ), patch(
        "src.server.database.conversation.get_thread_by_id", by_id
    ), patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(_FakeResp())
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "follow up please", "thread_id": raw}),
            config=_config(),
        )

    payload = _payload(result)
    assert payload.get("success") is True, payload
    # Both lookups receive the canonical form, not the urn:uuid: input.
    owner.assert_awaited_once_with(PTC_THREAD_ID)
    by_id.assert_awaited_once_with(PTC_THREAD_ID)


def _f1_origin() -> dict:
    return {
        "origin": "flash",
        "flash_thread_id": "flash-F1",
        "flash_workspace_id": "ws-F1",
        "ptc_thread_id": PTC_THREAD_ID,
        "ptc_workspace_id": WORKSPACE_ID,
        "report_back": True,
        "user_id": USER_ID,
    }


@contextlib.contextmanager
def _dispatch_env(store: dict, *, resp: _FakeResp | None = None):
    """Patch the continuation dispatch's collaborators; yields the fake cache."""
    cache = _FakeCache()
    cache.kv.update(store)
    with patch(
        "src.server.database.conversation.get_thread_owner_id",
        AsyncMock(return_value=USER_ID),
    ), patch(
        "src.server.database.conversation.get_thread_by_id",
        AsyncMock(return_value={
            "conversation_thread_id": PTC_THREAD_ID,
            "workspace_id": WORKSPACE_ID,
        }),
    ), patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.utils.cache.redis_cache.get_cache_client",
        return_value=cache,
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(resp or _FakeResp())
    ):
        yield cache


async def _dispatch_from(flash: str) -> dict:
    result = await ptc_agent.ainvoke(
        _tool_call({"question": "follow up", "thread_id": PTC_THREAD_ID}),
        config={"configurable": {
            "user_id": USER_ID,
            "thread_id": flash,
            "workspace_id": f"ws-{flash}",
        }},
    )
    return _payload(result)


@pytest.mark.asyncio
async def test_cross_flash_reuse_does_not_strand_other_flashs_origin():
    """A second flash's failed dispatch must not clobber/delete F1's origin."""
    store = {f"ptc_origin:{PTC_THREAD_ID}": _f1_origin()}

    with _dispatch_env(store, resp=_FakeResp(status=500)) as cache:
        payload = await _dispatch_from("flash-F2")

    assert payload.get("success") is False, payload  # dispatch failed -> rollback
    # F1's origin survives untouched: not overwritten to F2, not deleted.
    assert cache.kv.get(f"ptc_origin:{PTC_THREAD_ID}") == _f1_origin()


@pytest.mark.asyncio
async def test_cross_flash_reuse_proceeds_unwired_touching_nothing():
    """A cross-flash reuse can't be wired: the atomic reserve detects the
    other flash's origin BEFORE adding anything, so there is no reservation
    to leak, the dispatch still runs, and F1's origin is untouched."""
    store = {f"ptc_origin:{PTC_THREAD_ID}": _f1_origin()}

    with _dispatch_env(store) as cache:
        payload = await _dispatch_from("flash-F2")

    assert payload.get("success") is True, payload  # dispatch still succeeds
    # Unwired (another flash owns the origin): must not promise a report-back.
    assert payload.get("report_back") is False, payload
    assert cache.kv.get(f"ptc_origin:{PTC_THREAD_ID}") == _f1_origin()
    assert PTC_THREAD_ID not in cache.client.sets.get("flash_watch:flash-F2", set())
    assert PTC_THREAD_ID not in cache.client.sets.get(
        f"flash_user_pending:{USER_ID}", set()
    )


@pytest.mark.asyncio
async def test_same_flash_reuse_keeps_origin_ownership():
    """A same-flash continuation refreshes its origin with a NEW generation."""
    same = _f1_origin()
    same["flash_thread_id"] = "flash-F2"  # owned by the dispatching flash
    store = {f"ptc_origin:{PTC_THREAD_ID}": dict(same)}

    with _dispatch_env(store) as cache:
        payload = await _dispatch_from("flash-F2")

    assert payload.get("success") is True, payload
    refreshed = cache.kv.get(f"ptc_origin:{PTC_THREAD_ID}")
    assert refreshed["flash_thread_id"] == "flash-F2"
    assert refreshed.get("dispatch_gen")  # committed refresh keeps the new gen
    # Same flash owns the wired origin + watch member, so report-back is real.
    assert payload.get("report_back") is True, payload
    assert store[f"ptc_origin:{PTC_THREAD_ID}"]["flash_thread_id"] == "flash-F2"


@pytest.mark.asyncio
async def test_unwired_reserve_reports_back_false_but_still_dispatches():
    """A fail-open reserve (Redis off by config) still dispatches but must not
    promise a report-back the completion gate would silently drop."""
    with _dispatch_env({}) as cache:
        cache.enabled = False
        cache.client = None
        payload = await _dispatch_from("flash-F1")

    assert payload.get("success") is True, payload
    assert payload.get("status") == "dispatched", payload
    assert payload.get("report_back") is False, payload


@pytest.mark.asyncio
async def test_continuation_non_uuid_thread_id_short_circuits():
    """A non-UUID id (e.g. an agent file/dir name) is rejected before any DB
    lookup, so it can't reach get_thread_by_id and raise."""
    owner = AsyncMock(return_value=USER_ID)
    by_id = AsyncMock(return_value={})

    with patch(
        "src.server.database.conversation.get_thread_owner_id", owner
    ), patch(
        "src.server.database.conversation.get_thread_by_id", by_id
    ), patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "follow up please", "thread_id": "results"}),
            config=_config(),
        )

    payload = _payload(result)
    assert payload.get("success") is False, payload
    assert "thread not found" in payload.get("error", ""), payload
    owner.assert_not_awaited()
    by_id.assert_not_awaited()


# ---------------------------------------------------------------------------
# A configured INTERNAL_SERVICE_TOKEN is the normal production state and, with
# auth enabled, a precondition for dispatch (the preflight guard aborts without
# it). These tests exercise the dispatch path itself, not the guard, so give
# the whole module a token regardless of the ambient environment. The guard
# test module asserts the unset behaviour separately.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _internal_service_token(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "test-internal-service-token")
