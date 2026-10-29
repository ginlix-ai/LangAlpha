"""Hand-offs and new workspaces the user approved in advance.

The setting is read when the model proposes the call, not when the tool runs:
a tool that paused for its card re-runs from the top on resume, so reading it
there would let a setting flipped while the card was pending skip an answer
the user already gave.
"""

from __future__ import annotations

import json
from typing import Annotated
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from langchain.agents import create_agent
from langchain.tools import InjectedState
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from src.tools.secretary._commands import InjectedToolCallId, hitl_confirm
from src.server.models.user import OtherPreference
from src.tools.secretary.approvals import (
    AUTO_APPROVE_HANDOFFS,
    AUTO_APPROVE_KEYS,
    AUTO_APPROVE_WORKSPACE_CREATION,
    StandingApprovalMiddleware,
    preapproved,
)
from src.tools.secretary.chief_of_staff import delegate_to_analyst
from src.tools.secretary.tools import workspaces_create
from tests.unit.server.handlers.chat.redis_fakes import FakeCache

from .conftest import FakeResp, FakeSession, workspace_manager

USER_ID = "user-1"


@pytest.fixture
def other_preference(monkeypatch) -> dict:
    """The user's ``other_preference``; tests set approval keys on it."""
    other: dict = {}

    async def get_user_preferences(user_id):
        return {"other_preference": other}

    monkeypatch.setattr(
        "src.server.database.user.get_user_preferences", get_user_preferences
    )
    return other


# --- The decision is frozen at proposal time ---------------------------------


@pytest.fixture
def ran() -> list[str]:
    """The questions the stand-in hand-off actually dispatched."""
    return []


class _Model(FakeMessagesListChatModel):
    def bind_tools(self, tools, **kwargs):
        return self


def _agent(ran: list[str], *questions: str):
    @tool("delegate_to_analyst")
    async def handoff(
        question: str,
        workspace_id: str,
        state: Annotated[dict, InjectedState],
        tool_call_id: Annotated[str, InjectedToolCallId] = "",
    ) -> str:
        """Hand off, asking first unless approved in advance."""
        if not preapproved(state, tool_call_id):
            approved, _ = hitl_confirm("ptc_agent", {"question": question})
            if not approved:
                return f"declined {question}"
        ran.append(question)
        return f"dispatched {question}"

    model = _Model(responses=[
        AIMessage(content="", tool_calls=[
            {
                "name": "delegate_to_analyst",
                "args": {"question": q, "workspace_id": "ws-1"},
                "id": f"call-{q}",
            }
            for q in questions
        ]),
        AIMessage(content="done"),
    ])
    return create_agent(
        model,
        tools=[handoff],
        middleware=[StandingApprovalMiddleware(USER_ID)],
        checkpointer=InMemorySaver(),
    )


def _tool_results(out: dict) -> list[str]:
    return [m.content for m in out["messages"] if m.type == "tool"]


@pytest.mark.asyncio
async def test_a_handoff_approved_in_advance_runs_without_a_card(other_preference, ran):
    other_preference[AUTO_APPROVE_HANDOFFS] = True
    await _agent(ran, "a").ainvoke(
        {"messages": [("user", "go")]}, {"configurable": {"thread_id": "t1"}}
    )

    assert ran == ["a"]


@pytest.mark.asyncio
async def test_turning_the_setting_on_while_a_card_waits_keeps_the_users_no(
    other_preference, ran
):
    agent = _agent(ran, "b")
    config = {"configurable": {"thread_id": "t2"}}
    out = await agent.ainvoke({"messages": [("user", "go")]}, config)
    (pending,) = out["__interrupt__"]

    other_preference[AUTO_APPROVE_HANDOFFS] = True
    out = await agent.ainvoke(
        Command(resume={pending.id: {"decisions": [{"type": "reject"}]}}), config
    )

    assert ran == []
    assert _tool_results(out) == ["declined b"]


@pytest.mark.asyncio
async def test_parallel_cards_are_each_answered_by_the_user(other_preference, ran):
    agent = _agent(ran, "c", "d")
    config = {"configurable": {"thread_id": "t3"}}
    out = await agent.ainvoke({"messages": [("user", "go")]}, config)
    # By question, not position: LangGraph does not promise the cards' order.
    card = {i.value["action_requests"][0]["question"]: i for i in out["__interrupt__"]}

    other_preference[AUTO_APPROVE_HANDOFFS] = True
    out = await agent.ainvoke(
        Command(resume={
            card["c"].id: {"decisions": [{"type": "approve"}]},
            card["d"].id: {"decisions": [{"type": "reject"}]},
        }),
        config,
    )

    assert ran == ["c"]
    assert sorted(_tool_results(out)) == ["declined d", "dispatched c"]


def test_the_keys_read_are_the_keys_settings_save():
    """The web saves these names and the validator accepts them; a key the
    middleware reads under another name leaves every card asking."""
    assert AUTO_APPROVE_KEYS == ("auto_approve_handoffs", "auto_approve_workspace_creation")
    assert set(AUTO_APPROVE_KEYS) <= set(OtherPreference.model_fields)


# --- Which calls the middleware records --------------------------------------


async def _recorded(tool_calls: list[dict], user_id: str | None = USER_ID):
    state = {"messages": [AIMessage(content="", tool_calls=tool_calls)]}
    return await StandingApprovalMiddleware(user_id).aafter_model(state, None)


def _call(tool_name: str, call_id: str, **args) -> dict:
    return {"name": tool_name, "args": args, "id": call_id}


@pytest.mark.asyncio
async def test_each_setting_covers_only_its_own_action(other_preference):
    other_preference[AUTO_APPROVE_WORKSPACE_CREATION] = True
    update = await _recorded([
        _call("manage_workspaces", "create", action="create", name="Research"),
        _call("manage_workspaces", "list", action="list"),
        _call("delegate_to_analyst", "handoff", question="q", workspace_id="ws-1"),
    ])

    assert update == {"preapproved_calls": ["create"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("ids", [{"workspace_id": "ws-1"}, {"thread_id": "t-1"}])
async def test_a_handoff_to_a_named_workspace_or_thread_needs_only_its_own_setting(
    other_preference, ids
):
    other_preference[AUTO_APPROVE_HANDOFFS] = True
    update = await _recorded(
        [_call("delegate_to_analyst", "handoff", question="q", **ids)]
    )

    assert update == {"preapproved_calls": ["handoff"]}


@pytest.mark.asyncio
async def test_only_a_true_setting_approves(other_preference):
    other_preference[AUTO_APPROVE_HANDOFFS] = "yes"
    update = await _recorded(
        [_call("delegate_to_analyst", "handoff", question="q", workspace_id="ws-1")]
    )

    assert update == {"preapproved_calls": []}


@pytest.mark.asyncio
async def test_an_unreadable_setting_asks_instead_of_failing_the_turn(monkeypatch):
    monkeypatch.setattr(
        "src.server.database.user.get_user_preferences",
        AsyncMock(side_effect=RuntimeError("pool closed")),
    )
    update = await _recorded(
        [_call("delegate_to_analyst", "handoff", question="q", workspace_id="ws-1")]
    )

    assert update == {"preapproved_calls": []}


@pytest.mark.asyncio
async def test_a_turn_without_a_user_or_an_approvable_call_records_nothing(
    other_preference,
):
    other_preference[AUTO_APPROVE_HANDOFFS] = True

    assert await _recorded(
        [_call("delegate_to_analyst", "h", question="q", workspace_id="ws-1")], None
    ) is None
    assert await _recorded([_call("manage_threads", "t", action="list")]) is None


# --- The tools skip the card only on the recorded call ------------------------


def _payload(result: Command) -> dict:
    return json.loads(result.update["messages"][0].content)


@pytest.mark.asyncio
async def test_the_real_handoff_tool_asks_unless_its_call_was_recorded(
    other_preference, monkeypatch
):
    """The tool never reads the setting: on resume it re-runs from the top,
    so only the record made when the model proposed this call may skip it."""
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "test-internal-service-token")
    other_preference.update(
        {AUTO_APPROVE_HANDOFFS: True, AUTO_APPROVE_WORKSPACE_CREATION: True}
    )
    confirm = MagicMock(return_value=(False, {}))
    with patch("src.tools.secretary.dispatch.hitl_confirm", confirm), patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "name": "Research"}),
    ):
        result = await delegate_to_analyst.ainvoke(
            {
                "name": "delegate_to_analyst",
                "args": {
                    "question": "q",
                    "workspace_id": "ws-1",
                    "state": {"preapproved_calls": ["an-earlier-call"]},
                },
                "id": "call-1",
                "type": "tool_call",
            },
            config={"configurable": {"user_id": USER_ID, "thread_id": "home-1"}},
        )

    confirm.assert_called_once()
    assert result.update["messages"][0].content.startswith("User declined the hand-off.")


_NEVER_ASK = MagicMock(side_effect=AssertionError("must not ask"))


@pytest.mark.asyncio
async def test_a_workspace_created_without_asking_carries_its_card():
    mgr = workspace_manager({"workspace_id": "ws-1", "name": "Research"})
    with patch("src.tools.secretary.tools.hitl_confirm", _NEVER_ASK), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ):
        result = await workspaces_create(
            USER_ID, "Research", "Q3 notes", "call-1", preapproved=True
        )

    assert _payload(result) == {
        "success": True,
        "workspace_id": "ws-1",
        "workspace_name": "Research",
        "preapproved": True,
        "workspace_description": "Q3 notes",
    }


@pytest.mark.asyncio
async def test_a_workspace_the_user_approved_on_its_card_is_not_marked():
    mgr = workspace_manager({"workspace_id": "ws-1", "name": "Research"})
    with patch(
        "src.tools.secretary.tools.hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ):
        result = await workspaces_create(USER_ID, "Research", None, "call-1")

    assert "preapproved" not in _payload(result)


@pytest.mark.asyncio
async def test_a_handoff_approved_in_advance_names_its_workspace(monkeypatch):
    """No card asked for it, so the result carries what the card would show."""
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "test-internal-service-token")
    cache = FakeCache()
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: cache)
    with patch("src.tools.secretary.dispatch.hitl_confirm", _NEVER_ASK), patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "name": "Research"}),
    ), patch("aiohttp.ClientSession", return_value=FakeSession(FakeResp())):
        result = await delegate_to_analyst.ainvoke(
            {
                "name": "delegate_to_analyst",
                "args": {
                    "question": "analyze this",
                    "workspace_id": "ws-1",
                    "state": {"preapproved_calls": ["call-1"]},
                },
                "id": "call-1",
                "type": "tool_call",
            },
            config={"configurable": {"user_id": USER_ID, "thread_id": "home-1"}},
        )

    payload = _payload(result)
    assert payload["status"] == "dispatched"
    assert payload["preapproved"] is True
    assert payload["workspace_name"] == "Research"
    assert payload["report_back"] is True


_WORKSPACE = "22222222-2222-2222-2222-222222222222"
_PTC_THREAD = "11111111-1111-1111-1111-111111111111"


def _fill_thread_cap(cache: FakeCache) -> None:
    from src.server.services.report_back.handoff import keys, reserve

    members = {f"p{i}" for i in range(reserve.MAX_DISPATCH_PER_THREAD)}
    cache.client.sets[keys.watch_key("home-1")] = members
    # Live origins, or the over-cap retry reaps them as orphans.
    for member in members:
        cache.kv[keys.ptc_origin_key(member)] = {
            "home_thread_id": "home-1",
            "report_back": True,
        }


async def _handoff_that_fails(
    monkeypatch, *, session, approved_in_advance=True, thread_id=None, cache=None
) -> dict:
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "test-internal-service-token")
    cache = cache or FakeCache()
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: cache)
    target = {"thread_id": thread_id} if thread_id else {"workspace_id": _WORKSPACE}
    asks = (
        _NEVER_ASK if approved_in_advance else MagicMock(return_value=(True, {}))
    )
    with patch("src.tools.secretary.dispatch.hitl_confirm", asks), patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "name": "Research"}),
    ), patch(
        "src.server.database.conversation.threads_read.get_thread_owner_id",
        AsyncMock(return_value=USER_ID),
    ), patch(
        "src.server.database.conversation.threads_read.get_thread_by_id",
        AsyncMock(return_value={"workspace_id": _WORKSPACE}),
    ), patch(
        "src.server.database.runs.lifecycle.get_latest_attempt",
        AsyncMock(return_value=None),
    ), patch(
        "src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0
    ), patch("aiohttp.ClientSession", return_value=session):
        result = await delegate_to_analyst.ainvoke(
            {
                "name": "delegate_to_analyst",
                "args": {
                    "question": "analyze this",
                    **target,
                    "state": (
                        {"preapproved_calls": ["call-1"]} if approved_in_advance else {}
                    ),
                },
                "id": "call-1",
                "type": "tool_call",
            },
            config={"configurable": {"user_id": USER_ID, "thread_id": "home-1"}},
        )
    return _payload(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("session", "error"),
    [
        (FakeSession(FakeResp(status=402)), "dispatch_failed"),
        (FakeSession(post_exc=aiohttp.InvalidURL("x")), "dispatch_failed"),
    ],
    ids=["refused", "never-sent"],
)
async def test_a_handoff_approved_in_advance_that_fails_says_where_it_was_going(
    monkeypatch, session, error
):
    """No card asked for it, so the failure carries what the card would show.
    A new thread's id names nothing until its run starts, so it stays out."""
    payload = await _handoff_that_fails(monkeypatch, session=session)

    assert payload == {
        "success": False,
        "error": error,
        "preapproved": True,
        "workspace_id": _WORKSPACE,
        "workspace_name": "Research",
    }


@pytest.mark.asyncio
async def test_a_handoff_approved_in_advance_over_the_cap_says_where_it_was_going(
    monkeypatch,
):
    cache = FakeCache()
    _fill_thread_cap(cache)
    payload = await _handoff_that_fails(
        monkeypatch,
        session=MagicMock(side_effect=AssertionError("dispatch must not run")),
        cache=cache,
    )

    assert "too many concurrent analyses" in payload["error"]
    assert payload["preapproved"] is True
    assert payload["workspace_name"] == "Research"
    assert "thread_id" not in payload


@pytest.mark.asyncio
async def test_a_continuation_approved_in_advance_that_fails_names_its_thread(
    monkeypatch,
):
    from src.tools.secretary.dispatch import _THREAD_BUSY

    payload = await _handoff_that_fails(
        monkeypatch,
        session=FakeSession(FakeResp(status=409)),
        thread_id=_PTC_THREAD,
    )

    assert payload == {
        "success": False,
        "error": _THREAD_BUSY,
        "preapproved": True,
        "workspace_id": _WORKSPACE,
        "workspace_name": "Research",
        "thread_id": _PTC_THREAD,
    }


@pytest.mark.asyncio
async def test_a_handoff_approved_in_advance_with_an_unknown_outcome_is_marked(
    monkeypatch,
):
    payload = await _handoff_that_fails(
        monkeypatch, session=FakeSession(post_exc=TimeoutError())
    )

    assert payload["outcome"] == "unknown_retained"
    assert payload["preapproved"] is True
    assert payload["workspace_name"] == "Research"
    # The minted thread, where the run may have started.
    assert payload["thread_id"]


@pytest.mark.asyncio
async def test_a_handoff_approved_on_its_card_fails_as_before(monkeypatch):
    """Its card already shows where it was going."""
    payload = await _handoff_that_fails(
        monkeypatch,
        session=FakeSession(FakeResp(status=402)),
        approved_in_advance=False,
    )

    assert payload == {"success": False, "error": "dispatch_failed"}


def test_a_declined_hand_off_keeps_the_work_unrouted_and_the_users_reason():
    """A decline says where the work stands, since a bare one read to the model
    as leave to build it in Home, and quotes only what the user wrote: the
    resume path's no-feedback sentence is not theirs. The web reads it by its
    prefix."""
    from src.tools.secretary._commands import decline_routing_command

    from src.server.models.chat import _format_rejection_message

    bare = decline_routing_command(
        "the hand-off",
        {"decisions": [{"type": "reject", "message": _format_rejection_message(None)}]},
        "c1",
    )
    said = decline_routing_command(
        "workspace creation",
        {
            "decisions": [
                {"type": "reject", "message": _format_rejection_message(" use my NVDA one ")}
            ]
        },
        "c2",
    )

    assert bare.update["messages"][0].content == (
        "User declined the hand-off. Unless they said to do the work here, "
        "do not do it yourself instead: ask them where they want it."
    )
    assert said.update["messages"][0].content.startswith(
        'User declined workspace creation, saying: "use my NVDA one". Unless'
    )
