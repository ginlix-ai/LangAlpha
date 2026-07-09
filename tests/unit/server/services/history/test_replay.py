"""build_checkpoint_replay_items: coverage guards, merging, image resolution."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.large_result_eviction import TOO_LARGE_TOOL_MSG
from src.server.services.history.reader import ThreadHistory, TurnSlice
from src.server.services.history.replay import (
    CheckpointReplayUnavailable,
    build_checkpoint_replay_items,
    build_sse_replay_items,
)

_EVICTION_POINTER = TOO_LARGE_TOOL_MSG.format(
    tool_call_id="tc-1",
    file_path=".agents/large_tool_results/tc-1.md",
    content_sample="     1\tpreview",
)

pytestmark = pytest.mark.asyncio

THREAD = "thread-r"


def _turn(ordinal, messages, user="hello", turn_index=None, new_ui_records=None):
    return TurnSlice(
        turn_ordinal=ordinal,
        input_checkpoint_id=f"cp-in-{ordinal}",
        end_checkpoint_id=f"cp-end-{ordinal}",
        user_message=HumanMessage(content=user, id=f"h-{ordinal}"),
        messages=messages,
        turn_index=turn_index,
        new_ui_records=new_ui_records or [],
    )


def _query(turn_index, content="hello", qtype="user"):
    return {"turn_index": turn_index, "content": content, "type": qtype, "created_at": "t0"}


def _response(turn_index, sse_events=None, status="completed"):
    return {
        "conversation_response_id": f"resp-{turn_index}",
        "sse_events": sse_events or [],
        "status": status,
    }


def _mock_reader(monkeypatch, history, task_messages=None):
    reader = MagicMock()
    reader.aget_thread_history = AsyncMock(return_value=history)
    reader.aget_task_messages = AsyncMock(return_value=task_messages or [])
    monkeypatch.setattr(
        "src.server.services.history.replay.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    return reader


async def test_legacy_backfilled_steering_falls_back(monkeypatch):
    # Legacy steering-backfilled turns (type="steering" query rows) have a
    # completed response but no source=input boundary — the pairing guard
    # raises and auto mode serves them from stored events.
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, [])])
    _mock_reader(monkeypatch, history)
    with pytest.raises(CheckpointReplayUnavailable, match="no checkpoint boundary"):
        await build_checkpoint_replay_items(
            THREAD,
            [_query(0), _query(1, qtype="steering")],
            {0: _response(0), 1: _response(1)},
        )


async def test_steered_turn_projects_delivered_event(monkeypatch):
    # Modern steering shape: one turn whose slice carries the stamped steering
    # HumanMessage mid-run. With no stored events (post-cutover), the
    # projector re-emits steering_delivered in position.
    delivered = {
        "count": 1,
        "messages": [{"content": "also cover bonds", "user_id": "u-1", "timestamp": 1.0}],
        "timestamp": 2.0,
    }
    turn_msgs = [
        AIMessage(content="starting", id="ai-1"),
        HumanMessage(
            content="[Steering from User]\nalso cover bonds",
            id="h-steer",
            additional_kwargs={
                "lc_source": "steering",
                "steering_delivered": delivered,
            },
        ),
        AIMessage(content="done, with bonds", id="ai-2"),
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    events = [i["event"] for i in items]
    assert events == ["user_message", "message_chunk", "steering_delivered", "message_chunk"]
    steer = items[2]["data"]
    assert steer["messages"] == delivered["messages"]
    assert steer["turn_index"] == 0
    # The raw "[Steering from User]" text never surfaces as content.
    assert all(
        "[Steering from User]" not in (i["data"].get("content") or "") for i in items
    )


async def test_stored_events_preferred_over_projected_signals(monkeypatch):
    # Transition rule: while the dual-write is on, a turn with stored events
    # replays its steering_delivered/context_window from storage (richer
    # payloads) — the projected copies are dropped, not duplicated.
    turn_msgs = [
        HumanMessage(
            content="[Steering from User]\ngo deeper",
            id="h-steer",
            additional_kwargs={"lc_source": "steering"},
        ),
        AIMessage(
            content="deeper",
            id="ai-1",
            usage_metadata={"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
        ),
    ]
    stored = [
        {
            "event": "steering_delivered",
            "data": {"count": 1, "messages": [{"content": "go deeper", "user_id": "u"}]},
        },
        {"event": "message_chunk", "data": {"id": "ai-1", "content_type": "text", "content": "deeper"}},
        {
            "event": "context_window",
            "data": {"action": "token_usage", "input_tokens": 10, "threshold": 120000},
        },
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    steering = [i for i in items if i["event"] == "steering_delivered"]
    assert len(steering) == 1
    assert steering[0]["data"]["messages"][0]["user_id"] == "u"  # the stored copy
    cws = [i for i in items if i["event"] == "context_window"]
    assert len(cws) == 1
    assert cws[0]["data"]["threshold"] == 120000


async def test_projected_token_usage_gets_threshold(monkeypatch):
    turn_msgs = [
        AIMessage(
            content="answer",
            id="ai-1",
            usage_metadata={"input_tokens": 50, "output_tokens": 5, "total_tokens": 55},
        )
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    monkeypatch.setattr(
        "src.server.services.history.replay.resolve_token_threshold", lambda: 99000
    )
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    cw = next(i for i in items if i["event"] == "context_window")
    assert cw["data"]["action"] == "token_usage"
    assert cw["data"]["input_tokens"] == 50
    assert cw["data"]["threshold"] == 99000


async def test_summarization_event_reemitted_at_turn_head(monkeypatch):
    # Compaction's summary message lives in _summarization_event state, never
    # the messages channel — the reader surfaces the turn it landed in and
    # replay re-emits summarize/complete before the turn's model output.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    summary_message = build_summary_message(
        "earlier turns condensed", None, original_message_count=30
    )
    turn = _turn(1, [AIMessage(content="fresh answer", id="ai-9")])
    turn.new_summarization_event = {
        "summary_message": summary_message,
        "cutoff_index": 12,
    }
    turn.newly_offloaded_args = 3
    turns = [_turn(0, [AIMessage(content="a0", id="ai-0")]), turn]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0), _query(1)], {0: _response(0), 1: _response(1)}
    )
    turn1 = [i for i in items if i["data"].get("turn_index") == 1]
    assert [i["event"] for i in turn1] == [
        "user_message",
        "context_window",
        "context_window",
        "message_chunk",
    ]
    offload = turn1[1]["data"]
    assert offload["action"] == "offload"
    assert offload["signal"] == "complete"
    assert offload["kind"] == "args"
    assert offload["offloaded_args"] == 3
    data = turn1[2]["data"]
    assert data["action"] == "summarize"
    assert data["signal"] == "complete"
    assert data["summary_text"] == "earlier turns condensed"
    assert data["original_message_count"] == 30
    # Turn 0 predates the compaction — no signal there.
    assert not [
        i for i in items
        if i["event"] == "context_window" and i["data"].get("turn_index") == 0
    ]


async def test_completed_turn_without_boundary_unavailable(monkeypatch):
    # A completed response always persists its boundary pointer first — a
    # completed row the checkpoints can't pair means they can't cover the
    # thread (e.g. a dangling row, or ordinal drift on an unstamped thread).
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, [])])
    _mock_reader(monkeypatch, history)
    with pytest.raises(CheckpointReplayUnavailable, match="no checkpoint boundary"):
        await build_checkpoint_replay_items(
            THREAD, [_query(0), _query(1)], {1: _response(1)}
        )


async def test_inflight_active_turn_replays_as_stub(monkeypatch):
    # The in-flight seam: query rows exist through turn 3, but the committed
    # branch only covers turns 0-2. The active turn must replay as its
    # user_message only (the frontend attaches to the live run), never by
    # borrowing the previous turn's content.
    turns = [_turn(i, [AIMessage(content=f"a{i}", id=f"ai-{i}")]) for i in range(3)]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    responses = {i: _response(i) for i in range(3)}
    responses[3] = _response(3, status="streaming")
    items = await build_checkpoint_replay_items(
        THREAD, [_query(i) for i in range(4)], responses
    )
    users = [i["data"]["turn_index"] for i in items if i["event"] == "user_message"]
    assert users == [0, 1, 2, 3]
    turn3_items = [i for i in items if i["data"].get("turn_index") == 3]
    assert [i["event"] for i in turn3_items] == ["user_message"]


async def test_inflight_windowed_keeps_absolute_pairing(monkeypatch):
    # Regression for the live-proven mislabel: ?limit=N during a streaming
    # turn must not staple the previous turn's answer under the active turn's
    # index. Window covers boundaries 1-2 of a thread whose turn 3 is live.
    turns = [_turn(i, [AIMessage(content=f"a{i}", id=f"ai-{i}")]) for i in (1, 2)]
    reader = MagicMock()
    reader.aget_recent_history = AsyncMock(
        return_value=ThreadHistory(thread_id=THREAD, turns=turns)
    )
    reader.aget_task_messages = AsyncMock(return_value=[])
    monkeypatch.setattr(
        "src.server.services.history.replay.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    responses = {i: _response(i) for i in range(3)}
    responses[3] = _response(3, status="streaming")
    items = await build_checkpoint_replay_items(
        THREAD, [_query(i) for i in range(4)], responses, last_n_turns=2
    )
    chunks = [i["data"] for i in items if i["event"] == "message_chunk"]
    assert [(c["turn_index"], c["content"]) for c in chunks] == [
        (1, "a1"),
        (2, "a2"),
    ]
    users = [i["data"]["turn_index"] for i in items if i["event"] == "user_message"]
    assert users == [1, 2, 3]  # turn 0 out of window; turn 3 = active stub


async def test_stamped_turn_index_overrides_ordinal(monkeypatch):
    # Mid-thread hole: turn 1 never checkpointed (cancelled during bringup).
    # Stamped metadata pairs turns 0 and 2 exactly; the hole replays as a
    # user_message stub instead of shifting every later turn by one.
    turns = [
        _turn(0, [AIMessage(content="a0", id="ai-0")], turn_index=0),
        _turn(1, [AIMessage(content="a2", id="ai-2")], turn_index=2),
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    responses = {
        0: _response(0),
        1: _response(1, status="cancelled"),
        2: _response(2),
    }
    items = await build_checkpoint_replay_items(
        THREAD, [_query(i) for i in range(3)], responses
    )
    chunks = [i["data"] for i in items if i["event"] == "message_chunk"]
    assert [(c["turn_index"], c["content"]) for c in chunks] == [
        (0, "a0"),
        (2, "a2"),
    ]
    turn1_items = [i for i in items if i["data"].get("turn_index") == 1]
    assert [i["event"] for i in turn1_items] == ["user_message"]


async def test_stamped_turn_index_unknown_unavailable(monkeypatch):
    history = ThreadHistory(
        thread_id=THREAD, turns=[_turn(0, [], turn_index=5)]
    )
    _mock_reader(monkeypatch, history)
    with pytest.raises(CheckpointReplayUnavailable, match="no persisted turn"):
        await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})


async def test_no_turns_unavailable(monkeypatch):
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD))
    with pytest.raises(CheckpointReplayUnavailable, match="no checkpoint turns"):
        await build_checkpoint_replay_items(THREAD, [_query(0)], {})


async def test_basic_turn_projection_and_enrichment(monkeypatch):
    history = ThreadHistory(
        thread_id=THREAD, turns=[_turn(0, [AIMessage(content="answer", id="ai-1")])]
    )
    _mock_reader(monkeypatch, history)
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0)}
    )
    assert [i["event"] for i in items] == ["user_message", "message_chunk"]
    assert items[0]["data"]["content"] == "hello"
    chunk = items[1]["data"]
    assert chunk["content"] == "answer"
    assert chunk["turn_index"] == 0
    assert chunk["response_id"] == "resp-0"
    assert chunk["thread_id"] == THREAD


async def test_stored_widget_replaces_projected(monkeypatch):
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "ShowWidget", "args": {}, "id": "tc-s"}],
        ),
        ToolMessage(
            content="shown",
            tool_call_id="tc-s",
            name="ShowWidget",
            id="tm-1",
            artifact={"html": "<div/>", "title": "W"},
        ),
    ]
    stored_widget = {
        "event": "artifact",
        "data": {
            "artifact_type": "html_widget",
            # Live widget ids are random and unrelated to the tool_call_id —
            # pairing with the projected widget is ordinal.
            "artifact_id": "widget_ab12cd34",
            "payload": {"html": "<div/>", "title": "W", "data": {"file.csv": "a,b"}},
        },
    }
    stored_cw = {
        "event": "context_window",
        "data": {"action": "summarize", "summary_text": "the summary"},
    }
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    _mock_reader(monkeypatch, history)

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, [stored_widget, stored_cw])}
    )
    widgets = [
        i for i in items
        if i["event"] == "artifact" and i["data"]["artifact_type"] == "html_widget"
    ]
    # The projected data-less widget upgrades to the stored payload in place:
    # it keeps its mid-turn position (before the tool result), not the end.
    assert len(widgets) == 1
    assert widgets[0]["data"]["artifact_id"] == "widget_ab12cd34"
    assert widgets[0]["data"]["payload"]["data"] == {"file.csv": "a,b"}
    events = [i["event"] for i in items]
    assert events.index("artifact") < events.index("tool_call_result")
    # Passthrough events ride along verbatim (plus enrichment).
    cw = next(i for i in items if i["event"] == "context_window")
    assert cw["data"]["summary_text"] == "the summary"
    assert cw["data"]["turn_index"] == 0


async def test_passthrough_events_keep_mid_turn_position(monkeypatch):
    turn_msgs = [
        AIMessage(
            content="thinking about it",
            id="ai-1",
            tool_calls=[{"name": "web_search", "args": {}, "id": "tc-1"}],
        ),
        ToolMessage(content="results", tool_call_id="tc-1", name="web_search", id="tm-1"),
        AIMessage(content="final answer", id="ai-2"),
    ]
    # Stored stream: chunks for ai-1 → tool result → context_window (offload
    # marker) → chunks for ai-2 → trailing credit_usage.
    stored = [
        {"event": "message_chunk", "data": {"id": "ai-1", "content_type": "text", "content": "thinking"}},
        {"event": "tool_call_result", "data": {"tool_call_id": "tc-1", "content": "results"}},
        {"event": "context_window", "data": {"action": "offload", "kind": "tool_args"}},
        {"event": "message_chunk", "data": {"id": "ai-2", "content_type": "text", "content": "final"}},
        {"event": "credit_usage", "data": {"total_credits": 1.0}},
    ]
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    _mock_reader(monkeypatch, history)

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    events = [i["event"] for i in items]
    # context_window lands between the tool result and the final text — its
    # original mid-turn position — not at the end of the turn.
    cw_idx = events.index("context_window")
    assert cw_idx > events.index("tool_call_result")
    final_idx = next(
        i for i, it in enumerate(items)
        if it["event"] == "message_chunk" and it["data"].get("id") == "ai-2"
    )
    assert cw_idx < final_idx
    # credit_usage stays terminal (after the final chunk, its last anchor).
    assert events.index("credit_usage") > final_idx


async def test_evicted_tool_result_restored_from_stored(monkeypatch):
    # Large results are evicted to a file before the ToolMessage is checkpointed,
    # so the checkpoint carries only the "too large, saved to …" pointer. The
    # full content the user saw survives in the stored event — restore it.
    full = "".join(f"line {i}\n" for i in range(500))
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "get_sec_filing", "args": {}, "id": "tc-1"}],
        ),
        ToolMessage(
            content=_EVICTION_POINTER, tool_call_id="tc-1", name="get_sec_filing", id="tm-1"
        ),
    ]
    stored = [
        {
            "event": "tool_call_result",
            "data": {"tool_call_id": "tc-1", "content": full, "content_type": "text"},
        }
    ]
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    _mock_reader(monkeypatch, history)

    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0, stored)})
    result = next(i for i in items if i["event"] == "tool_call_result")
    assert result["data"]["content"] == full  # restored, not the pointer


async def test_evicted_pointer_in_both_streams_is_noop(monkeypatch):
    # When the live stream also carries the pointer (eviction ran before SSE, the
    # current ordering), there is nothing fuller to restore — leave it be.
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "get_sec_filing", "args": {}, "id": "tc-1"}],
        ),
        ToolMessage(
            content=_EVICTION_POINTER, tool_call_id="tc-1", name="get_sec_filing", id="tm-1"
        ),
    ]
    stored = [
        {"event": "tool_call_result", "data": {"tool_call_id": "tc-1", "content": _EVICTION_POINTER}}
    ]
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    _mock_reader(monkeypatch, history)

    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0, stored)})
    result = next(i for i in items if i["event"] == "tool_call_result")
    assert result["data"]["content"] == _EVICTION_POINTER  # unchanged


async def test_stored_interrupt_and_error_pass_through_in_position(monkeypatch):
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "AskUserQuestion", "args": {}, "id": "tc-q"}],
        ),
        ToolMessage(
            content="answer: blue", tool_call_id="tc-q", name="AskUserQuestion", id="tm-1"
        ),
    ]
    # Stored stream as persisted for a resolved HITL turn: the interrupt fired
    # after the tool call, then the resumed run produced the result and failed.
    stored = [
        {
            "event": "tool_calls",
            "data": {"id": "run-1", "tool_calls": [{"id": "tc-q", "name": "AskUserQuestion"}]},
        },
        {
            "event": "interrupt",
            "data": {"interrupt_id": "int-q", "action_requests": [{"type": "ask_user_question"}]},
        },
        {"event": "tool_call_result", "data": {"tool_call_id": "tc-q", "content": "answer: blue"}},
        {"event": "error", "data": {"error": "provider exploded"}},
    ]
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    _mock_reader(monkeypatch, history)

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    events = [i["event"] for i in items]
    # The resolved interrupt keeps its original slot: after the tool call,
    # before its result — so the frontend attaches the HITL card, then
    # resolves it from the subsequent tool_call_result.
    assert events.index("interrupt") > events.index("tool_calls")
    assert events.index("interrupt") < events.index("tool_call_result")
    interrupt = next(i for i in items if i["event"] == "interrupt")
    assert interrupt["data"]["interrupt_id"] == "int-q"
    assert interrupt["data"]["turn_index"] == 0
    # The stored error marker rides along too (sse wire parity).
    error = next(i for i in items if i["event"] == "error")
    assert error["data"]["error"] == "provider exploded"


async def test_image_map_applied_from_ui_records(monkeypatch):
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[
            _turn(0, [AIMessage(content="![chart](work/chart.png)", id="ai-1")])
        ],
        ui=[
            {
                "type": "ui",
                "id": "ui-1",
                "name": "image_capture",
                "props": {"path_to_url": {"work/chart.png": "https://cdn/x/chart.png"}},
            }
        ],
    )
    _mock_reader(monkeypatch, history)
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    chunk = next(i for i in items if i["event"] == "message_chunk")
    assert chunk["data"]["content"] == "![chart](https://cdn/x/chart.png)"


async def test_unresolved_images_fall_back_to_stored(monkeypatch):
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[_turn(0, [AIMessage(content="![chart](work/chart.png)", id="ai-1")])],
    )
    _mock_reader(monkeypatch, history)
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        }
    ]
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    chunk = next(i for i in items if i["event"] == "message_chunk")
    assert chunk["data"]["content"] == "![chart](https://cdn/rewritten.png)"


async def test_subagent_transcript_projected_once_with_image_map(monkeypatch):
    task_artifact = {"task_id": "tsk1", "action": "init", "description": "d", "prompt": "p"}
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "Task", "args": {}, "id": "tc-t"}],
        ),
        ToolMessage(
            content="dispatched",
            tool_call_id="tc-t",
            name="Task",
            id="tm-1",
            additional_kwargs={"task_artifact": task_artifact},
        ),
    ]
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[_turn(0, turn_msgs), _turn(1, [AIMessage(content="done", id="ai-2")])],
        ui=[
            {
                "type": "ui",
                "id": "ui-1",
                "name": "image_capture",
                "props": {"path_to_url": {"work/sub.png": "https://cdn/sub.png"}},
            }
        ],
    )
    reader = _mock_reader(
        monkeypatch,
        history,
        task_messages=[
            AIMessage(
                content="![img](work/sub.png)",
                id="sub-ai-1",
                tool_calls=[
                    {
                        "name": "write_file",
                        "args": {"file_path": "notes.md", "content": "x"},
                        "id": "tc-sub-w",
                    }
                ],
            ),
            ToolMessage(
                content="ok", tool_call_id="tc-sub-w", name="write_file", id="sub-tm-1"
            ),
        ],
    )
    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1, content="next")],
        {0: _response(0), 1: _response(1)},
    )
    reader.aget_task_messages.assert_awaited_once_with(THREAD, "tsk1")
    sub_chunks = [
        i for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    # Subagent text also resolves through the image map (not only main items).
    assert len(sub_chunks) == 1
    assert sub_chunks[0]["data"]["content"] == "![img](https://cdn/sub.png)"
    # Derived artifacts are suppressed in the task lane — live streams never
    # emit them there and the frontend subagent handler has no artifact case.
    assert not [
        i for i in items
        if i["event"] == "artifact" and str(i["data"].get("agent", "")).startswith("task:")
    ]


async def test_terminal_interrupts_appended(monkeypatch):
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[_turn(0, [AIMessage(content="asking", id="ai-1")])],
        interrupts=[
            {"id": "int-1", "value": {"action_requests": [{"description": "go?"}]}}
        ],
    )
    _mock_reader(monkeypatch, history)
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    assert items[-1]["event"] == "interrupt"
    assert items[-1]["data"]["interrupt_id"] == "int-1"
    assert items[-1]["data"]["action_requests"] == [{"description": "go?"}]
    assert items[-1]["data"]["finish_reason"] == "interrupt"


async def test_system_query_tagged_and_synthetic_human_dropped(monkeypatch):
    # Report-back (flash dispatches PTC, the PTC completion is injected as a new
    # flash turn): the turn's boundary input is a <system>-wrapped HumanMessage
    # and its query row is type="system". Checkpoint replay reads that raw
    # HumanMessage from checkpoint state, so it must (a) tag the user_message
    # query_type="system" (the frontend hides the bubble) and (b) never surface
    # the <system> plumbing text as assistant content — the projector drops every
    # HumanMessage. Mirrors the sse path (test_build_sse_replay_items_shape).
    system_text = (
        "<system>\nThe analysis you dispatched has completed. "
        "Use agent_output to summarize.\n</system>"
    )
    turn_msgs = [
        HumanMessage(content=system_text, id="h-sys"),
        AIMessage(content="Here is your summary.", id="ai-1"),
    ]
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs, user=system_text)])
    _mock_reader(monkeypatch, history)

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0, content=system_text, qtype="system")], {0: _response(0)}
    )
    users = [i for i in items if i["event"] == "user_message"]
    assert len(users) == 1
    assert users[0]["data"]["query_type"] == "system"  # frontend hides the bubble
    chunks = [i for i in items if i["event"] == "message_chunk"]
    # The assistant summary survives; the <system> plumbing text never leaks.
    assert any(c["data"].get("content") == "Here is your summary." for c in chunks)
    assert all("<system>" not in (c["data"].get("content") or "") for c in chunks)


async def test_last_n_turns_windows_to_recent(monkeypatch):
    # Windowed replay materializes only the last N turns and pairs them with the
    # matching tail of query rows (aget_recent_history, not aget_thread_history).
    turns = [_turn(i, [AIMessage(content=f"a{i}", id=f"ai-{i}")]) for i in range(4)]
    reader = MagicMock()
    reader.aget_thread_history = AsyncMock(
        return_value=ThreadHistory(thread_id=THREAD, turns=turns)
    )
    reader.aget_recent_history = AsyncMock(
        return_value=ThreadHistory(thread_id=THREAD, turns=turns[-2:])
    )
    reader.aget_task_messages = AsyncMock(return_value=[])
    monkeypatch.setattr(
        "src.server.services.history.replay.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    queries = [_query(i) for i in range(4)]
    items = await build_checkpoint_replay_items(
        THREAD, queries, {i: _response(i) for i in range(4)}, last_n_turns=2
    )
    users = [i for i in items if i["event"] == "user_message"]
    assert [u["data"]["turn_index"] for u in users] == [2, 3]  # last two turns only
    reader.aget_recent_history.assert_awaited_once()
    reader.aget_thread_history.assert_not_awaited()


async def test_widget_data_ref_resolved_from_storage(monkeypatch):
    # Post-dual-write shape: no stored events; the checkpointed artifact
    # carries only data_ref, and replay inlines the data from object storage.
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "ShowWidget", "args": {}, "id": "tc-w"}],
        ),
        ToolMessage(
            content="shown",
            tool_call_id="tc-w",
            name="ShowWidget",
            id="tm-1",
            artifact={
                "html": "<div/>",
                "title": "W",
                "data_ref": {"key": "widgets/t/abc.json", "sha256": "abc", "size": 3},
            },
        ),
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    monkeypatch.setattr(
        "src.server.services.history.replay.get_bytes",
        lambda key: b'{"file.csv": "a,b"}' if key == "widgets/t/abc.json" else None,
    )
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    widgets = [
        i for i in items
        if i["event"] == "artifact" and i["data"]["artifact_type"] == "html_widget"
    ]
    assert len(widgets) == 1
    payload = widgets[0]["data"]["payload"]
    assert payload["data"] == {"file.csv": "a,b"}
    assert payload["data_ref"]["key"] == "widgets/t/abc.json"


async def test_widget_inline_data_needs_no_storage(monkeypatch):
    # Small payloads inline into the checkpointed artifact — replay is
    # complete without stored events or a storage read.
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "ShowWidget", "args": {}, "id": "tc-w"}],
        ),
        ToolMessage(
            content="shown",
            tool_call_id="tc-w",
            name="ShowWidget",
            id="tm-1",
            artifact={"html": "<div/>", "title": "W", "data": {"f.json": "[1]"}},
        ),
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))

    def _boom(key):
        raise AssertionError("storage must not be read for inline data")

    monkeypatch.setattr("src.server.services.history.replay.get_bytes", _boom)
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    widgets = [
        i for i in items
        if i["event"] == "artifact" and i["data"]["artifact_type"] == "html_widget"
    ]
    assert widgets[0]["data"]["payload"]["data"] == {"f.json": "[1]"}


async def test_widget_data_ref_unreadable_left_in_place(monkeypatch):
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "ShowWidget", "args": {}, "id": "tc-w"}],
        ),
        ToolMessage(
            content="shown",
            tool_call_id="tc-w",
            name="ShowWidget",
            id="tm-1",
            artifact={
                "html": "<div/>",
                "title": "W",
                "data_ref": {"key": "widgets/gone.json", "sha256": "x", "size": 1},
            },
        ),
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    monkeypatch.setattr(
        "src.server.services.history.replay.get_bytes", lambda key: None
    )
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    widgets = [
        i for i in items
        if i["event"] == "artifact" and i["data"]["artifact_type"] == "html_widget"
    ]
    payload = widgets[0]["data"]["payload"]
    assert "data" not in payload
    assert payload["html"] == "<div/>"


def test_build_sse_replay_items_shape():
    """The sse fallback: one user_message per query, then enriched stored events."""
    events = [
        {"event": "message_chunk", "data": {"content": "hi", "content_type": "text"}},
        {"event": "credit_usage", "data": {"total_credits": 2.0}},
        "not-a-dict",  # skipped
        {"event": "", "data": {}},  # invalid, skipped
    ]
    items = build_sse_replay_items(
        THREAD,
        [_query(0), _query(1, content="next", qtype="system")],
        {0: _response(0, events)},
    )
    assert [i["event"] for i in items] == [
        "user_message",
        "message_chunk",
        "credit_usage",
        "user_message",
    ]
    # System query is tagged so the frontend hides the user bubble.
    assert items[3]["data"]["query_type"] == "system"
    # Stored events are enriched with turn/response context.
    chunk = items[1]["data"]
    assert chunk["turn_index"] == 0
    assert chunk["response_id"] == "resp-0"
    assert chunk["thread_id"] == THREAD
    # The source event is not mutated (data is copied).
    assert "turn_index" not in events[0]["data"]


async def test_answered_interrupt_projected_at_turn_end(monkeypatch):
    """A turn's ending_interrupts (resume-boundary __interrupt__ writes) emit
    interrupt items after the turn content; a turn with stored events drops
    them in favor of the stored copies."""
    turn_msgs = [AIMessage(content="asking", id="a-0")]
    turn = _turn(0, turn_msgs)
    turn.ending_interrupts = [
        {"id": "int-1", "value": {"action_requests": [{"description": "pick one"}]}}
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))

    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    assert [i["event"] for i in items] == ["user_message", "message_chunk", "interrupt"]
    card = items[2]["data"]
    assert card["interrupt_id"] == "int-1"
    assert card["action_requests"] == [{"description": "pick one"}]
    assert card["finish_reason"] == "interrupt"

    # Transition rule: stored events win, the projected card is dropped.
    stored = [
        {"event": "message_chunk", "data": {"content": "asking", "content_type": "text", "id": "a-0", "agent": "main"}},
        {"event": "interrupt", "data": {"interrupt_id": "int-1", "action_requests": [{"description": "pick one"}], "extra": "stored"}},
    ]
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    cards = [i for i in items if i["event"] == "interrupt"]
    assert len(cards) == 1
    assert cards[0]["data"]["extra"] == "stored"


async def test_credit_usage_synthesized_from_usage_row(monkeypatch):
    """Post-cutover turns reconstruct the terminal credit_usage event from the
    conversation_usages row via the shared live aggregation."""
    turn_msgs = [AIMessage(content="done", id="a-0")]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    usage_row = {
        "conversation_response_id": "resp-0",
        "token_usage": {
            "by_model": {
                "m1": {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120},
                "m2": {"input_tokens": 50, "output_tokens": 5, "total_tokens": 55},
            }
        },
        "total_credits": 1.234,
        "created_at": "2026-07-07T00:00:00",
    }

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0)}, usages=[usage_row]
    )
    assert items[-1]["event"] == "credit_usage"
    data = items[-1]["data"]
    # Aggregated across models, no model names on the wire.
    assert data["tokens"] == {
        "input_tokens": 150,
        "output_tokens": 25,
        "total_tokens": 175,
    }
    assert data["total_credits"] == 1.23
    assert data["timestamp"] == "2026-07-07T00:00:00"
    assert "by_model" not in data

    # A turn with stored events keeps the stored credit_usage instead.
    stored = [{"event": "credit_usage", "data": {"total_credits": 9.99}}]
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}, usages=[usage_row]
    )
    credits = [i for i in items if i["event"] == "credit_usage"]
    assert len(credits) == 1
    assert credits[0]["data"]["total_credits"] == 9.99

    # Errored runs never emitted the event live — no synthesis.
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, status="error")}, usages=[usage_row]
    )
    assert not [i for i in items if i["event"] == "credit_usage"]


async def test_provenance_synthesized_from_rows_anchored(monkeypatch):
    """Provenance rows re-emit as provenance events anchored after the
    tool_call_result matching their tool_call_id; unanchorable rows tail."""
    turn_msgs = [
        AIMessage(
            content="",
            id="a-0",
            tool_calls=[{"name": "web_search", "args": {"q": "x"}, "id": "tc-1"}],
        ),
        ToolMessage(content="result", tool_call_id="tc-1", id="t-0"),
        AIMessage(content="summary", id="a-1"),
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    rows = [
        {
            "conversation_response_id": "resp-0",
            "provenance_record_id": "row-1",
            "tool_call_id": "tc-1",
            "source_type": "web_page",
            "identifier": "https://example.test/a",
            "title": "A",
            "detail": None,
            "provider": "search",
            "args_fingerprint": None,
            "args": {"q": "x"},
            "result_sha256": "abc",
            "result_size": 10,
            "result_snippet": "snip",
            "agent": "main",
            "source_timestamp": None,
            "created_at": None,
        },
        {
            "conversation_response_id": "resp-0",
            "provenance_record_id": "row-2",
            "tool_call_id": None,
            "source_type": "mcp_tool",
            "identifier": "get_quote",
            "agent": "main",
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0)}, provenance=rows
    )
    events = [i["event"] for i in items]
    result_idx = events.index("tool_call_result")
    assert events[result_idx + 1] == "provenance"
    anchored = items[result_idx + 1]["data"]
    assert anchored["record_id"] == "row-1"
    assert anchored["identifier"] == "https://example.test/a"
    assert anchored["result_sha256"] == "abc"
    # The unanchorable row tails the turn.
    assert items[-1]["event"] == "provenance"
    assert items[-1]["data"]["record_id"] == "row-2"

    # Stored events win during the transition.
    stored = [{"event": "provenance", "data": {"record_id": "live-1", "source_type": "web_page"}}]
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}, provenance=rows
    )
    prov = [i for i in items if i["event"] == "provenance"]
    assert [p["data"]["record_id"] for p in prov] == ["live-1"]


async def test_terminal_error_synthesized_on_both_paths(monkeypatch):
    """An errored response row reconstructs the terminal error event — on the
    checkpoint path and the sse path alike (stored events never contain it:
    live it is yielded after the persist snapshot)."""
    response = _response(0, status="error")
    response["errors"] = ["boom exploded"]
    response["metadata"] = {"error_type": "llm_provider_error", "error_class": "RuntimeError"}

    turn_msgs = [AIMessage(content="partial", id="a-0")]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: response})
    assert items[-1]["event"] == "error"
    data = items[-1]["data"]
    assert data["error"] == "boom exploded"
    assert data["type"] == "workflow_error"
    assert data["error_type"] == "llm_provider_error"
    assert data["error_class"] == "RuntimeError"
    assert data["turn_index"] == 0

    sse_items = build_sse_replay_items(THREAD, [_query(0)], {0: response})
    assert sse_items[-1]["event"] == "error"
    assert sse_items[-1]["data"]["error"] == "boom exploded"

    # Errored run that never checkpointed a boundary: stub + error item.
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs, turn_index=0)]),
    )
    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1, content="errored")],
        {0: _response(0), 1: response | {"conversation_response_id": "resp-1"}},
    )
    assert [i["event"] for i in items] == [
        "user_message",
        "message_chunk",
        "user_message",
        "error",
    ]
    assert items[-1]["data"]["response_id"] == "resp-1"

    # Legacy errored rows (errors column empty — pre-fix) stay silent.
    legacy = _response(0, status="error")
    assert build_sse_replay_items(THREAD, [_query(0)], {0: legacy})[-1]["event"] == "user_message"


async def test_model_fallback_projected_from_ui_records(monkeypatch):
    # A fallback notice rides the turn's ui channel (push_ui_message in the
    # resilience middleware); replay projects it at the turn head with the
    # live handler's field whitelist and error scrubbing. Unrelated ui
    # records (e.g. legacy image_capture maps) are ignored.
    records = [
        {
            "type": "ui",
            "id": "ui-fb-1",
            "name": "model_fallback",
            "props": {
                "from_model": "primary-model",
                "to_model": "fallback-a",
                "from_is_primary": True,
                "error": "boom https://user:sekret@api.example.com/v1",
                "status_code": 503,
                "attempts_on_from": 3,
            },
            "metadata": {},
        },
        {"type": "ui", "id": "ui-x", "name": "image_capture", "props": {}, "metadata": {}},
    ]
    turn = _turn(0, [AIMessage(content="ok", id="ai-1")], new_ui_records=records)
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    assert [i["event"] for i in items] == ["user_message", "model_fallback", "message_chunk"]
    data = items[1]["data"]
    assert data["agent"] == "main"
    assert data["from_model"] == "primary-model"
    assert data["to_model"] == "fallback-a"
    assert data["from_is_primary"] is True
    assert data["status_code"] == 503
    assert data["attempts_on_from"] == 3
    assert "sekret" not in data["error"]
    assert data["turn_index"] == 0


async def test_model_fallback_stored_events_preferred(monkeypatch):
    # Dual-write era: a turn with stored events replays the stored fallback
    # copy (exact mid-turn position) and drops the projected one.
    records = [
        {
            "type": "ui",
            "id": "ui-fb-1",
            "name": "model_fallback",
            "props": {"from_model": "p", "to_model": "f"},
            "metadata": {},
        }
    ]
    stored = [
        {"event": "message_chunk", "data": {"agent": "main", "id": "ai-1", "role": "assistant", "content": "ok", "content_type": "text"}},
        {"event": "model_fallback", "data": {"agent": "main", "from_model": "p", "to_model": "f", "position": "stored"}},
    ]
    turn = _turn(0, [AIMessage(content="ok", id="ai-1")], new_ui_records=records)
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, sse_events=stored)}
    )
    fallbacks = [i for i in items if i["event"] == "model_fallback"]
    assert len(fallbacks) == 1
    assert fallbacks[0]["data"].get("position") == "stored"
