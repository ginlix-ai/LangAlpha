from __future__ import annotations

import pytest
from langchain_core.messages import HumanMessage, ToolMessage

from ptc_agent.agent.claude_code.session import (
    Arrivals,
    CheckpointSessionStore,
    SessionDrift,
    pending_deferral,
    resume_from,
    session_records_reducer,
)


def write(sid: str, *uuids: str | None) -> dict:
    return {"session_id": sid, "entries": [{"uuid": u, "n": i} if u else {"n": i} for i, u in enumerate(uuids)]}


def test_records_are_kept_once_each_in_write_order():
    state = session_records_reducer(None, [write("s1", "a", "b"), write("s1", "b", "c", None)])
    assert [e.get("uuid") for e in state["entries"]] == ["a", "b", "c", None]
    assert state["session_id"] == "s1"


def test_folding_writes_separately_or_together_gives_the_same_value():
    w1, w2 = write("s1", "a", "b"), write("s1", "b", "c")
    together = session_records_reducer(None, [w1, w2])
    apart = session_records_reducer(session_records_reducer(None, [w1]), [[w2]])
    assert together == apart


def test_a_write_naming_another_session_starts_it_over():
    state = session_records_reducer(session_records_reducer(None, [write("s1", "a")]), [write("s2", "x")])
    assert state["session_id"] == "s2"
    assert [e["uuid"] for e in state["entries"]] == ["x"]


@pytest.mark.asyncio
async def test_the_store_holds_appends_until_drained_and_says_when_they_arrived():
    store = CheckpointSessionStore()
    await store.append({"session_id": "s1"}, [{"uuid": "b"}, {"type": "title"}])
    assert await store.arrived.wait(["b"], timeout=0.1) == ["b"]
    with pytest.raises(SessionDrift):
        await store.arrived.wait(["never"], timeout=0.01)
    assert store.drain() == [{"session_id": "s1", "entries": [{"uuid": "b"}, {"type": "title"}]}]
    assert store.drain() == []


@pytest.mark.asyncio
async def test_a_step_drains_only_through_its_own_records():
    store = CheckpointSessionStore()
    await store.append({"session_id": "s1"}, [{"uuid": "result"}, {"type": "marker"}])
    await store.append({"session_id": "s1"}, [{"uuid": "next-reply"}])
    assert store.drain(through=["result"]) == [{"session_id": "s1", "entries": [{"uuid": "result"}]}]
    assert store.drain(through=["result"]) == []
    assert store.drain() == [{"session_id": "s1", "entries": [{"type": "marker"}, {"uuid": "next-reply"}]}]


@pytest.mark.asyncio
async def test_a_subagent_transcript_is_drift():
    with pytest.raises(SessionDrift):
        await CheckpointSessionStore().append({"session_id": "s1", "subpath": "subagents/agent-1"}, [{"uuid": "k"}])


@pytest.mark.asyncio
async def test_a_wait_returns_once_every_key_has_arrived():
    arrivals = Arrivals("tool result records")
    await arrivals.add({"t1": "u1"})
    with pytest.raises(SessionDrift, match="t2"):
        await arrivals.wait(["t1", "t2"], timeout=0.01)
    await arrivals.add({"t2": "u1"})
    assert await arrivals.wait(["t1", "t2"], timeout=0.1) == ["u1", "u1"]


def _deferred(tool_use_id: str) -> dict:
    return {"type": "attachment", "uuid": "d-" + tool_use_id, "attachment": {"type": "hook_deferred_tool", "toolUseID": tool_use_id}}


def _result(tool_use_id: str) -> dict:
    return {"type": "user", "uuid": "r-" + tool_use_id,
            "message": {"content": [{"type": "tool_result", "tool_use_id": tool_use_id, "content": "ok"}]}}


def test_the_call_a_session_holds_deferred_is_its_last_deferral_no_result_followed():
    text = {"type": "user", "uuid": "u", "message": {"content": "plain text"}}
    assert pending_deferral({"entries": [_deferred("a"), text]}) == "a"
    assert pending_deferral({"entries": [_deferred("a"), _result("a")]}) is None
    assert pending_deferral({"entries": [_deferred("a"), _result("a"), _deferred("b")]}) == "b"
    assert pending_deferral(None) is None


def test_a_cli_resumes_on_the_answer_to_the_call_it_deferred_or_on_a_session_that_never_deferred_it():
    records = {"entries": [_result("x"), _deferred("b"), _deferred("a")]}
    answer = ToolMessage("User answered: apple", tool_call_id="a")
    assert resume_from(records, [answer]) == (records["entries"], answer)
    assert resume_from(records, [ToolMessage("other", tool_call_id="x")]) == ([_result("x"), _deferred("b")], None)
    assert resume_from({"entries": [_result("x")]}, []) == ([_result("x")], None)


def test_a_cli_resumed_on_a_message_after_the_answer_cuts_the_call_off():
    records = {"entries": [_result("x"), _deferred("a")]}
    answer = ToolMessage("User answered: apple", tool_call_id="a")
    assert resume_from(records, [answer, HumanMessage("actually, banana")]) == ([_result("x")], None)
