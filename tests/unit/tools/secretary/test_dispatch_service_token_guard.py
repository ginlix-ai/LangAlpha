"""Internal-service-token preflight guard on background dispatch.

A background dispatch (``X-Dispatch: background``) is only honoured by the
/messages endpoint when the request carries a matching ``INTERNAL_SERVICE_TOKEN``
(``is_internal`` in src/server/app/threads.py). With the token unset the
endpoint silently downgrades to a *foreground* SSE stream that runs the whole
workflow synchronously and burns model credits for a result the caller can
never parse. Both internal dispatch call sites must therefore refuse to fire a
dispatch they can't have honoured, rather than degrade into that credit sink.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat import report_back as rb
from src.tools.secretary.tools import ptc_agent

USER_ID = "user-1"
FLASH_THREAD_ID = "flash-thread-1"
PTC_THREAD_ID = "ptc-thread-1"


def _tool_call(args: dict, call_id: str = "call_test") -> dict:
    return {"name": "ptc_agent", "args": args, "id": call_id, "type": "tool_call"}


def _config() -> dict:
    return {"configurable": {"user_id": USER_ID, "thread_id": FLASH_THREAD_ID}}


def _payload(result) -> dict:
    return json.loads(result.update["messages"][0].content)


@pytest.mark.asyncio
async def test_ptc_agent_aborts_when_service_token_unset(monkeypatch):
    """ptc_agent must fail loud (no HTTP, no workspace) when the token is unset:
    firing the dispatch would run the workflow foreground and burn credits."""
    monkeypatch.delenv("INTERNAL_SERVICE_TOKEN", raising=False)

    # If any of these run, the guard failed to short-circuit early enough.
    with patch(
        "src.tools.secretary.tools._hitl_confirm",
        side_effect=AssertionError("HITL must not be reached"),
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch HTTP must not run")),
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        MagicMock(side_effect=AssertionError("workspace must not be created")),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "internal_service_token_missing"


@pytest.mark.asyncio
async def test_ptc_agent_blank_service_token_is_treated_as_unset(monkeypatch):
    """A whitespace-only token is not a real secret -> still aborts."""
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "   ")
    with patch(
        "src.tools.secretary.tools._hitl_confirm",
        side_effect=AssertionError("HITL must not be reached"),
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch HTTP must not run")),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    assert _payload(result)["error"] == "internal_service_token_missing"


@pytest.mark.asyncio
async def test_report_back_drops_when_service_token_unset(monkeypatch):
    """The report-back dispatcher drops (no retry, no HTTP) when unset."""
    monkeypatch.delenv("INTERNAL_SERVICE_TOKEN", raising=False)

    origin = {
        "user_id": USER_ID,
        "flash_workspace_id": "ws-flash",
        "ptc_workspace_id": "ws-ptc",
    }
    cache = MagicMock()  # guard returns before cache is touched
    with patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("report-back HTTP must not run")),
    ):
        status, run_id = await rb._post_report_back(
            cache, FLASH_THREAD_ID, PTC_THREAD_ID, origin
        )

    assert status == "drop"
    assert run_id is None
