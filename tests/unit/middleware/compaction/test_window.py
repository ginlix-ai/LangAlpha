"""The window's pieces: where it may cut, what a trim writes, the carries that
keep counts over trimmed runs, the transcript's numbering base, and when the
middleware stands down. ``test_window_identity`` proves the whole."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.types import Overwrite

from ptc_agent.agent.middleware.compaction import middleware as middleware_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.notes import notes_window_carry
from ptc_agent.agent.middleware.compaction.window import trim, window_cut
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
)
from ptc_agent.agent.middleware.subagent_switch import (
    SUBAGENTS_ROW_KIND,
    SUBAGENTS_SCHEMA_VERSION,
    last_announced,
    subagents_window_carry,
)
from ptc_agent.agent.transcript import (
    EarlierTurnsMissing,
    Window,
    build_directory,
    load_manifest,
)
from ptc_agent.agent.transcript import identity
from tests.unit.middleware.compaction.window_harness import NOTES_DIR, THREAD

INDEX_LIMIT = identity._INDEX_LIMIT


def _run(n: int, *, calls: int = 1, write_to: str | None = None) -> list:
    out = [HumanMessage(f"Question {n}", id=f"h-{n}")]
    for k in range(calls):
        call_id = f"c-{n}-{k}"
        path = write_to if write_to and k == 0 else f"/home/workspace/work/{n}-{k}.py"
        out.append(
            AIMessage(
                "",
                id=f"ai-{n}-{k}",
                tool_calls=[{"name": "Write", "id": call_id, "args": {"file_path": path}}],
            )
        )
        out.append(ToolMessage("ok", tool_call_id=call_id, id=f"t-{n}-{k}"))
    out.append(AIMessage(f"Answer {n}", id=f"ai-{n}-end"))
    return out


def _thread(runs: int, first: int = 1) -> list:
    return [m for n in range(first, first + runs) for m in _run(n)]


def _event(messages: list, anchor_id: str | None) -> dict:
    index = next((i for i, m in enumerate(messages) if m.id == anchor_id), 0)
    event = {"cutoff_index": index, "summary_message": HumanMessage("summary", id="s-1")}
    if anchor_id is not None:
        event["anchor_message_id"] = anchor_id
    return event


def _index(messages: list, message_id: str) -> int:
    return next(i for i, m in enumerate(messages) if m.id == message_id)


def _trim(state: dict, cut: int, carries=()) -> dict:
    """``trim`` with the head the middleware hands it."""
    return trim(state, cut, carries, head=Window.of(state).extend(state["messages"][:cut]))


# ------------------------------------------------------------ window_cut


def test_the_cut_is_the_start_of_the_run_holding_the_boundary() -> None:
    messages = _thread(4)
    assert window_cut(messages, _event(messages, "t-3-0")) == _index(messages, "h-3")
    assert window_cut(messages, _event(messages, "h-3")) == _index(messages, "h-3")


def test_nothing_is_cut_inside_the_first_run() -> None:
    messages = _thread(3)
    assert window_cut(messages, _event(messages, "ai-1-end")) == 0


def test_an_unanchored_or_unresolvable_boundary_cuts_nothing() -> None:
    messages = _thread(3)
    assert window_cut(messages, None) == 0
    assert window_cut(messages, {"cutoff_index": 9}) == 0
    assert window_cut(messages, {"cutoff_index": 9, "anchor_message_id": "gone"}) == 0


def test_a_stamped_injection_opens_no_run() -> None:
    messages = _thread(2)
    notice = HumanMessage(
        "Background task finished.", id="orch", name="orchestrator",
        additional_kwargs={"lc_source": "orchestrator"},
    )
    messages = [*messages, notice, AIMessage("more", id="ai-more")]
    assert window_cut(messages, _event(messages, "ai-more")) == _index(messages, "h-2")


def test_messages_before_the_first_question_belong_to_the_first_run() -> None:
    messages = [AIMessage("hello", id="greeting"), *_thread(3)]
    assert window_cut(messages, _event(messages, "t-2-0")) == _index(messages, "h-2")
    assert Window().extend(messages[: _index(messages, "h-2")]).runs == 1


# ------------------------------------------------------------ trim


def test_a_trim_keeps_the_window_and_counts_what_went() -> None:
    messages = _thread(4)
    cut = _index(messages, "h-3")
    update = _trim({"messages": messages}, cut)
    assert isinstance(update["messages"], Overwrite)
    assert [m.id for m in update["messages"].value] == [m.id for m in messages[cut:]]
    window = Window.of(update)
    assert window.runs == 2
    assert window.earlier == {1: "Question 1", 2: "Question 2"}
    assert window.holds(messages[:cut])


def test_trims_count_on_from_the_last() -> None:
    first = _trim({"messages": _thread(4)}, _index(_thread(4), "h-3"))
    state = {"messages": first["messages"].value, **{k: v for k, v in first.items() if k != "messages"}}
    second = Window.of(_trim(state, _index(state["messages"], "h-4")))
    assert second.runs == 3
    assert second.earlier == {1: "Question 1", 2: "Question 2", 3: "Question 3"}
    # The digest runs on over both trims, as one trim of both would leave it.
    whole = _thread(4)
    assert second == Window().extend(whole[: _index(whole, "h-4")])


# ------------------------------------------------------------ the head


def test_the_head_is_its_messages_in_order_whatever_their_ids() -> None:
    """A message saved before ids were stable loads with another id, or
    none, from one checkpoint to the next; its words and calls do not."""
    messages = _thread(2)
    head = Window().extend(messages)
    assert (head.runs, head.messages) == (2, len(messages))
    reloaded = [m.model_copy(update={"id": None}) for m in messages]
    assert head.holds(reloaded)
    assert not head.holds(messages[:-1])
    assert not head.holds([*messages[:-2], messages[-1], messages[-2]])
    edited = [*messages[:-1], AIMessage("Answer 2, edited", id="ai-2-end")]
    assert not head.holds(edited)


def test_the_earlier_index_keeps_only_what_a_summary_lists() -> None:
    messages = _thread(INDEX_LIMIT + 5)
    update = _trim({"messages": messages}, _index(messages, f"h-{INDEX_LIMIT + 4}"))
    window = Window.of(update)
    assert window.runs == INDEX_LIMIT + 3
    assert sorted(window.earlier) == list(range(4, INDEX_LIMIT + 4))


def test_kept_messages_get_ids_the_overwrite_would_not_stamp() -> None:
    messages = [*_thread(1), HumanMessage("Question 2"), AIMessage("Answer 2")]
    update = _trim({"messages": messages}, len(_run(1)))
    assert all(m.id for m in update["messages"].value)


def test_the_recorded_offloads_shrink_to_the_window() -> None:
    messages = _thread(3)
    state = {
        "messages": messages,
        "_offloaded_tool_call_ids": {"c-1-0", "c-3-0"},
        "_offloaded_read_result_ids": {"c-3-0"},
    }
    update = _trim(state, _index(messages, "h-2"))
    assert update["_offloaded_tool_call_ids"] == {"c-3-0"}
    assert "_offloaded_read_result_ids" not in update


def test_each_carry_reads_the_trimmed_runs() -> None:
    seen = []

    def carry(trimmed, state):
        seen.append([m.id for m in trimmed])
        return {"_carried": len(trimmed)}

    messages = _thread(3)
    update = _trim({"messages": messages}, _index(messages, "h-2"), [carry])
    assert seen == [[m.id for m in _run(1)]]
    assert update["_carried"] == len(_run(1))


# ------------------------------------------------------------ the carries


def test_the_notes_count_runs_back_through_trimmed_runs() -> None:
    carry = notes_window_carry(THREAD)
    wrote = [*_run(1, calls=3, write_to=f"{NOTES_DIR}/plan.md")]
    # A write in the trimmed runs: the count restarts after it.
    assert carry(wrote, {}) == {"_notes_calls_trimmed": 2}
    # No write: the count adds to what earlier trims left.
    assert carry(_run(2, calls=2), {"_notes_calls_trimmed": 2}) == {"_notes_calls_trimmed": 4}
    assert carry([HumanMessage("hi", id="h")], {"_notes_calls_trimmed": 2}) == {}


def _notice(allowed: bool, message_id: str) -> HumanMessage:
    row = DurableUpdate(
        kind=SUBAGENTS_ROW_KIND,
        schema_version=SUBAGENTS_SCHEMA_VERSION,
        text="",
        provenance={"source": "user", "allowed": allowed},
    )
    message = build_update_message(row)
    message.id = message_id
    return message


def test_a_trimmed_notice_is_carried() -> None:
    trimmed = [*_run(1), _notice(False, "n-1"), *_run(2)]
    assert subagents_window_carry(trimmed, {}) == {"_subagents_trimmed": True}
    assert subagents_window_carry(trimmed, {"_subagents_trimmed": True}) == {}
    assert subagents_window_carry(_run(1), {}) == {}


def test_a_trimmed_notice_is_restated() -> None:
    """The model may still believe what a summarized notice said, so a
    switch with no notice in view is restated whichever side it is."""
    assert last_announced({"messages": _run(3)}) is True
    assert last_announced({"messages": _run(3), "_subagents_trimmed": True}) is None
    # A notice summarized but not yet trimmed.
    messages = [*_run(1), _notice(False, "n-1"), *_run(2)]
    assert last_announced({"messages": messages}) is False
    summarized = {"messages": messages, "_summarization_event": _event(messages, "h-2")}
    assert last_announced(summarized) is None


# ------------------------------------------------------------ the transcript


def test_a_trimmed_render_carries_the_stored_files() -> None:
    whole = _thread(5)
    stored = build_directory(whole[: _index(whole, "h-5")], window=Window())
    cut = _index(whole, "h-3")
    windowed = build_directory(
        whole[cut:],
        previous=load_manifest(stored.manifest),
        window=Window().extend(whole[:cut]),
    )
    assert windowed.manifest == build_directory(whole, window=Window()).manifest
    assert sorted(windowed.rendered) == ["turn-0005.jsonl"]


def test_a_trimmed_render_without_the_stored_files_asks_for_them() -> None:
    whole = _thread(4)
    cut = _index(whole, "h-3")
    window = Window().extend(whole[:cut])
    with pytest.raises(EarlierTurnsMissing):
        build_directory(whole[cut:], window=window)
    short = build_directory(whole[: _index(whole, "h-2")], window=Window())
    with pytest.raises(EarlierTurnsMissing):
        build_directory(whole[cut:], previous=load_manifest(short.manifest), window=window)


def test_a_copy_from_another_branch_is_not_carried() -> None:
    """A live save mid-turn is labelled with the commit pointer, which after
    an edit is the fork point both branches share, so a stored copy can be
    another branch's under the same turn numbers."""
    whole = _thread(4)
    cut = _index(whole, "h-3")
    other = [*_run(1), *_run(2)[:-1], AIMessage("Answer 2, regenerated", id="ai-2-end"), *_run(3)]
    stored = load_manifest(build_directory(other, window=Window()).manifest)
    with pytest.raises(EarlierTurnsMissing):
        build_directory(whole[cut:], previous=stored, window=Window().extend(whole[:cut]))
    # Its own branch's copy is carried.
    stored = load_manifest(build_directory(whole[: _index(whole, "h-4")], window=Window()).manifest)
    windowed = build_directory(
        whole[cut:], previous=stored, window=Window().extend(whole[:cut])
    )
    assert windowed.manifest == build_directory(whole, window=Window()).manifest


def test_a_copy_from_before_entries_named_their_messages_is_not_carried() -> None:
    whole = _thread(4)
    cut = _index(whole, "h-3")
    stored = load_manifest(build_directory(whole[: _index(whole, "h-4")], window=Window()).manifest)
    for entry in stored["segments"]:
        del entry["through"]
    with pytest.raises(EarlierTurnsMissing):
        build_directory(whole[cut:], previous=stored, window=Window().extend(whole[:cut]))
    # A full render reuses its files and names them.
    again = build_directory(whole, previous=stored, window=Window())
    assert again.manifest == build_directory(whole, window=Window()).manifest
    assert sorted(again.rendered) == ["turn-0004.jsonl"]


# ------------------------------------------------------------ the middleware


def _middleware(coverage=None) -> CompactionMiddleware:
    middleware = CompactionMiddleware(
        Summarizer(None, limit=10**9, counter=len), token_threshold=10**9, keep_messages=4
    )
    return middleware.with_window(coverage) if coverage else middleware


def _summarized(runs: int = 4) -> dict:
    messages = _thread(runs)
    return {"messages": messages, "_summarization_event": _event(messages, f"t-{runs - 1}-0")}


def test_covered_runs_are_trimmed() -> None:
    asked = []

    async def coverage(head):
        asked.append(head)
        return True

    state = _summarized()
    update = asyncio.run(_middleware(coverage)._trim(state))
    head = Window().extend(state["messages"][: _index(state["messages"], "h-3")])
    assert asked == [head] and head.runs == 2
    assert update is not None and Window.of(update) == head


def test_a_refusal_is_kept_until_a_summary_moves_the_cut() -> None:
    """Kept in the checkpoint, so every worker reads it; a thread whose
    slices are backfilled since trims at its next summary."""
    answers = [False, True]
    asked = []

    async def coverage(head):
        asked.append(head.runs)
        return answers.pop(0)

    middleware = _middleware(coverage)
    state = _summarized()
    refused = asyncio.run(middleware._trim(state))
    cut = _index(state["messages"], "h-3")
    assert refused == {"_window_refused": {"anchor": "t-3-0", "cut": cut}}
    state = {**state, **refused}
    assert asyncio.run(middleware._trim(state)) is None
    assert asked == [2]

    messages = state["messages"]
    state = {**state, "_summarization_event": _event(messages, "t-4-0")}
    update = asyncio.run(middleware._trim(state))
    assert asked == [2, 3]
    assert update is not None and Window.of(update).runs == 3


@pytest.mark.parametrize("answer", [False, RuntimeError("db down"), "slow"])
def test_nothing_is_trimmed_the_server_does_not_confirm(answer) -> None:
    async def coverage(runs):
        if answer == "slow":
            await asyncio.sleep(1)
        if isinstance(answer, Exception):
            raise answer
        return answer

    with patch.object(middleware_module, "_COVERAGE_TIMEOUT_S", 0.01):
        update = asyncio.run(_middleware(coverage)._trim(_summarized()))
    # Only an answer is kept as a refusal; a failed or slow check asks again.
    if answer is False:
        assert set(update) == {"_window_refused"}
    else:
        assert update is None


def test_without_the_server_nothing_is_trimmed() -> None:
    assert asyncio.run(_middleware()._trim(_summarized())) is None
