"""Internal-service-token preflight guard on background dispatch.

With auth enabled (HOST_MODE != "oss") and ``INTERNAL_SERVICE_TOKEN`` unset,
the /messages endpoint rejects ``X-Dispatch: background`` with 403. Both
internal dispatch call sites preflight the token and fail loud before any side
effect (HITL prompt, workspace creation, cap reservation, HTTP). In oss mode
with no token the endpoint trusts the self-dispatch, so the guard must not
fire.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.server.services.report_back.flash import executor as rb_executor
from src.tools.secretary.tools import ptc_agent

USER_ID = "user-1"
FLASH_THREAD_ID = "flash-thread-1"
PTC_THREAD_ID = "ptc-thread-1"

_ORIGIN = {
    "user_id": USER_ID,
    "flash_workspace_id": "ws-flash",
    "ptc_workspace_id": "ws-ptc",
}


def _tool_call(args: dict, call_id: str = "call_test") -> dict:
    return {"name": "ptc_agent", "args": args, "id": call_id, "type": "tool_call"}


def _config() -> dict:
    return {"configurable": {"user_id": USER_ID, "thread_id": FLASH_THREAD_ID}}


def _payload(result) -> dict:
    return json.loads(result.update["messages"][0].content)


def _unset_token(monkeypatch):
    # setenv("") rather than delenv: a lazy import may re-run load_dotenv(),
    # which repopulates a deleted var from .env but never overrides an
    # existing one. Empty and unset are equivalent to the guard.
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "")


class _FakeResp:
    def __init__(self, status, json_data):
        self.status = status
        self._json_data = json_data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._json_data


class _FakeSession:
    def __init__(self, resp):
        self._resp = resp
        self.post_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, *args, **kwargs):
        self.post_calls += 1
        return self._resp


# ---------------------------------------------------------------------------
# Auth enabled (platform): unset token aborts before any side effect
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ptc_agent_aborts_when_service_token_unset(monkeypatch):
    """ptc_agent must fail loud (no HITL, no workspace, no HTTP) when the
    token is unset: the endpoint would reject the dispatch with 403."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    _unset_token(monkeypatch)

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
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
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
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    _unset_token(monkeypatch)

    cache = MagicMock()  # guard returns before cache is touched
    with patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("report-back HTTP must not run")),
    ):
        status, run_id = await rb_executor._post_report_back(
            cache, FLASH_THREAD_ID, PTC_THREAD_ID, _ORIGIN
        )

    assert status == "drop"
    assert run_id is None


# ---------------------------------------------------------------------------
# oss mode: no token is the trusted zero-config path — guard must not fire
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ptc_agent_oss_mode_proceeds_without_token(monkeypatch):
    """The flow must reach the HITL confirm (declined here to stop it)."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    _unset_token(monkeypatch)

    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(False, {})
    ) as hitl:
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    hitl.assert_called_once()
    assert result.update["messages"][0].content == "User declined PTC agent dispatch."


@pytest.mark.asyncio
async def test_report_back_oss_mode_posts_without_token(monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    _unset_token(monkeypatch)

    session = _FakeSession(_FakeResp(200, {"run_id": "rid-1"}))
    with patch("aiohttp.ClientSession", MagicMock(return_value=session)):
        outcome = await rb_executor._post_report_back(
            cache=None,
            flash_thread_id=FLASH_THREAD_ID,
            ptc_thread_id=PTC_THREAD_ID,
            origin=_ORIGIN,
        )

    assert outcome == ("dispatched", "rid-1")
    assert session.post_calls == 1
