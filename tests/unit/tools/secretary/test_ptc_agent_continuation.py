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

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.tools.secretary.tools import ptc_agent

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
