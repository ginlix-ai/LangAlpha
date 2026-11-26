from __future__ import annotations

import asyncio

import pytest
from langchain_core.messages import HumanMessage, ToolMessage

from ptc_agent.agent.claude_code.session import (
    ENDED,
    Arrivals,
    CheckpointSessionStore,
    Reach,
    SessionDrift,
    answered_in,
    pending_deferral,
    resume_from,
    session_records_reducer,
)
from src.llms.claude_code import COMPUTER_LOST, ClaudeTurnFailed


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


def test_an_ended_session_leaves_nothing_to_resume():
    held = session_records_reducer(None, [{**write("s1", "a"), "pinned": {"system_prompt": "p", "tools": []}, "baseline": "b1"}])
    ended = session_records_reducer(held, [ENDED])
    assert not ended.get("session_id") and not ended["entries"]
    assert ended.get("pinned") is None and ended.get("baseline") is None


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
    arrivals = Arrivals("tool result records", Reach())
    await arrivals.add({"t1": "u1"})
    with pytest.raises(SessionDrift, match="t2"):
        await arrivals.wait(["t1", "t2"], timeout=0.01)
    await arrivals.add({"t2": "u1"})
    assert await arrivals.wait(["t1", "t2"], timeout=0.1) == ["u1", "u1"]


@pytest.mark.asyncio
async def test_a_wait_the_cli_went_out_of_reach_during_is_given_its_time_again():
    """Nothing the CLI reports can arrive while its transport reattaches, so
    the wait runs on past its time until the CLI is back, then gets it whole."""
    store = CheckpointSessionStore()
    waiting = asyncio.ensure_future(store.results.wait(["t1"], timeout=0.05))
    await asyncio.sleep(0.01)
    store.reach.lost()
    await asyncio.sleep(0.2)
    assert not waiting.done()
    store.reach.back()
    await asyncio.sleep(0.02)
    await store.results.add({"t1": "u1"})
    assert await waiting == ["u1"]
    with pytest.raises(SessionDrift, match="t2"):
        await store.results.wait(["t2"], timeout=0.05)


@pytest.mark.asyncio
async def test_a_wait_begun_with_the_cli_out_of_reach_gets_its_whole_time_once_it_is_back():
    store = CheckpointSessionStore()
    store.reach.lost()
    waiting = asyncio.ensure_future(store.results.wait(["t1"], timeout=0.3))
    await asyncio.sleep(0.25)
    store.reach.back()
    await asyncio.sleep(0.15)
    assert not waiting.done(), "its time ran only from when the CLI was back"
    await store.results.add({"t1": "u1"})
    assert await waiting == ["u1"]


@pytest.mark.asyncio
async def test_a_wait_woken_as_the_cli_went_out_of_reach_again_waits_for_it_to_be_back():
    store = CheckpointSessionStore()
    store.reach.lost()
    waiting = asyncio.ensure_future(store.results.wait(["t1"], timeout=0.3))
    await asyncio.sleep(0.01)
    store.reach.back()
    store.reach.lost()
    await asyncio.sleep(0.2)
    store.reach.back()
    await asyncio.sleep(0.15)
    assert not waiting.done(), "its time ran only from when the CLI was back for good"
    await store.results.add({"t1": "u1"})
    assert await waiting == ["u1"]


def test_a_reach_given_up_on_is_never_lost_again():
    """A late probe that fails after the transport gave up must not hide the
    failure from the waits."""
    reach = Reach()
    reach.gone(ClaudeTurnFailed(COMPUTER_LOST, "the computer did not answer again"))
    reach.lost()
    assert reach.up.is_set() and reach.drops == 0


@pytest.mark.asyncio
async def test_a_wait_ends_with_the_lost_computer_once_its_transport_gives_up():
    """A wait paused for a reattach that never comes ends with the turn's
    lost computer, rather than waiting for a reach that never returns."""
    store = CheckpointSessionStore()
    waiting = asyncio.ensure_future(store.arrived.wait(["b1"], timeout=0.05))
    store.reach.lost()
    await asyncio.sleep(0.1)
    store.reach.gone(ClaudeTurnFailed(COMPUTER_LOST, "the computer did not answer again"))
    with pytest.raises(ClaudeTurnFailed) as lost:
        await asyncio.wait_for(waiting, 1)
    assert lost.value.kind == COMPUTER_LOST


@pytest.mark.asyncio
async def test_a_wait_that_runs_out_asks_whether_the_machine_still_answers():
    """A connection gone silent is not yet noticed by its keepalive: a wait
    that ran out with the CLI in reach asks first, and one whose machine
    does not answer waits for the reattach instead of drifting."""
    store = CheckpointSessionStore()

    async def silent() -> bool:
        store.reach.lost()
        return False

    store.reach.probe = silent
    waiting = asyncio.ensure_future(store.results.wait(["t1"], timeout=0.05))
    await asyncio.sleep(0.2)
    assert not waiting.done()
    store.reach.back()
    await store.results.add({"t1": "u1"})
    assert await asyncio.wait_for(waiting, 1) == ["u1"]

    async def answers() -> bool:
        return True

    store.reach.probe = answers
    with pytest.raises(SessionDrift, match="t2"):
        await store.results.wait(["t2"], timeout=0.05)

    async def arrives_meanwhile() -> bool:
        await store.results.add({"t3": "u3"})
        return True

    store.reach.probe = arrives_meanwhile
    assert await store.results.wait(["t3"], timeout=0.05) == ["u3"], "a record that came while it asked is no drift"


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


def test_a_cli_resumed_on_a_message_after_the_answer_holds_the_answer_in_place_of_the_deferral():
    records = {"entries": [_result("x"), _deferred("a")]}
    answer = ToolMessage("User answered: apple", tool_call_id="a")
    assert resume_from(records, [answer, HumanMessage("actually, banana")]) == (answered_in(records["entries"], answer), None)
