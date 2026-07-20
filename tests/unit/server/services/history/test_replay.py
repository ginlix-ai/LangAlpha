"""build_checkpoint_replay_items: coverage guards, merging, image resolution."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.large_result_eviction import TOO_LARGE_TOOL_MSG
from src.server.services.history.reader import TaskHistory, ThreadHistory, TurnSlice
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


def _mock_reader(monkeypatch, history, task_messages=None, task_history=None):
    reader = MagicMock()
    reader.aget_thread_history = AsyncMock(return_value=history)
    reader.aget_task_history = AsyncMock(
        return_value=task_history or TaskHistory(messages=task_messages or [])
    )
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


async def test_missing_committed_tip_is_replay_unavailable(monkeypatch):
    from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

    reader = _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD))
    reader.aget_thread_history = AsyncMock(
        side_effect=CheckpointBranchTipNotFound(THREAD, "missing-tip")
    )

    with pytest.raises(CheckpointReplayUnavailable, match="missing-tip"):
        await build_checkpoint_replay_items(
            THREAD,
            [_query(0)],
            {0: _response(0)},
            branch_tip_checkpoint_id="missing-tip",
        )


async def test_missing_committed_tip_is_unavailable_on_cache_path(monkeypatch):
    from src.server.services.history import projection_cache
    from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

    reader = _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD))
    reader.aget_turn_anchors = AsyncMock(
        side_effect=CheckpointBranchTipNotFound(THREAD, "missing-tip")
    )
    monkeypatch.setattr(projection_cache, "cache_active", lambda: True)

    with pytest.raises(CheckpointReplayUnavailable, match="missing-tip"):
        await build_checkpoint_replay_items(
            THREAD,
            [_query(0)],
            {0: _response(0)},
            branch_tip_checkpoint_id="missing-tip",
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


async def test_user_message_run_id_stamped_only_when_terminal(monkeypatch):
    # Terminal turns' user_message carries the run id so a reloading client
    # can mark those runs rendered (report-back refresh dedup). The active
    # turn's stub must NOT carry it — the frontend attaches to the live run,
    # and a stamped id would suppress the attach that streams it.
    turns = [_turn(i, [AIMessage(content=f"a{i}", id=f"ai-{i}")]) for i in range(3)]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    responses = {i: _response(i) for i in range(3)}
    responses[3] = _response(3, status="streaming")
    items = await build_checkpoint_replay_items(
        THREAD, [_query(i) for i in range(4)], responses
    )
    stamps = {
        i["data"]["turn_index"]: i["data"].get("run_id")
        for i in items
        if i["event"] == "user_message"
    }
    assert stamps == {0: "resp-0", 1: "resp-1", 2: "resp-2", 3: None}


async def test_inflight_windowed_keeps_absolute_pairing(monkeypatch):
    # Regression for the live-proven mislabel: ?limit=N during a streaming
    # turn must not staple the previous turn's answer under the active turn's
    # index. Window covers boundaries 1-2 of a thread whose turn 3 is live.
    turns = [_turn(i, [AIMessage(content=f"a{i}", id=f"ai-{i}")]) for i in (1, 2)]
    reader = MagicMock()
    reader.aget_recent_history = AsyncMock(
        return_value=ThreadHistory(thread_id=THREAD, turns=turns)
    )
    reader.aget_task_history = AsyncMock(return_value=TaskHistory())
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
    image_record = {
        "type": "ui",
        "id": "ui-1",
        "name": "image_capture",
        "props": {"path_to_url": {"work/chart.png": "https://cdn/x/chart.png"}},
    }
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[
            _turn(
                0,
                [AIMessage(content="![chart](work/chart.png)", id="ai-1")],
                new_ui_records=[image_record],
            )
        ],
    )
    _mock_reader(monkeypatch, history)
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    chunk = next(i for i in items if i["event"] == "message_chunk")
    assert chunk["data"]["content"] == "![chart](https://cdn/x/chart.png)"


async def test_image_maps_are_scoped_to_their_turn(monkeypatch):
    """Reusing a sandbox path later must not rewrite the older turn."""
    old = {
        "type": "ui",
        "id": "ui-old",
        "name": "image_capture",
        "props": {"path_to_url": {"work/chart.png": "https://cdn/old.png"}},
    }
    new = {
        "type": "ui",
        "id": "ui-new",
        "name": "image_capture",
        "props": {"path_to_url": {"work/chart.png": "https://cdn/new.png"}},
    }
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[
            _turn(
                0,
                [AIMessage(content="![old](work/chart.png)", id="ai-old")],
                new_ui_records=[old],
            ),
            _turn(
                1,
                [AIMessage(content="![new](work/chart.png)", id="ai-new")],
                new_ui_records=[new],
            ),
        ],
    )
    _mock_reader(monkeypatch, history)

    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1, content="next")],
        {0: _response(0), 1: _response(1)},
    )

    chunks = [i["data"]["content"] for i in items if i["event"] == "message_chunk"]
    assert chunks == ["![old](https://cdn/old.png)", "![new](https://cdn/new.png)"]


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
    # The wholesale replay copies each stored event's nested data; enrichment
    # must not stamp turn/response context back into the pristine source row.
    assert "turn_index" not in stored[0]["data"]
    assert "response_id" not in stored[0]["data"]


async def test_wholesale_fallback_preserves_task_lane_and_watermark(monkeypatch):
    """The unresolved-image wholesale substitution replaces only the MAIN
    lane: the stored root archive has no task frames (the per-task channel
    owns them), so the projected task transcript must survive and the
    watermark stamped on the stored artifact must stay truthful."""
    from datetime import datetime, timezone

    started = datetime(2026, 1, 3, tzinfo=timezone.utc)
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
        "task_run_id": "run-1",
    }
    turn_msgs = [
        AIMessage(content="![chart](work/chart.png)", id="ai-1"),
        AIMessage(
            content="",
            id="ai-2",
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
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_messages=[AIMessage(content="task answer", id="sub-ai-1")],
    )
    reader.aget_task_run_stamps = AsyncMock(return_value=["run-1"])
    monkeypatch.setattr(
        "src.server.services.history.replay.sr_db.list_runs_for_thread",
        AsyncMock(
            return_value=[
                {
                    "task_run_id": "run-1",
                    "status": "completed",
                    "started_at": started,
                }
            ]
        ),
    )
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "artifact",
            "data": {
                "artifact_type": "task",
                "artifact_id": "task:tsk1",
                "agent": "main",
                "payload": {
                    "task_id": "tsk1",
                    "action": "init",
                    "task_run_id": "run-1",
                },
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    main_chunks = [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") != "task:tsk1"
    ]
    assert "![chart](https://cdn/rewritten.png)" in main_chunks
    # The projected task transcript survived the substitution.
    assert [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ] == ["task answer"]
    # And the stored artifact carries a truthful watermark.
    artifacts = [
        i
        for i in items
        if i["event"] == "artifact" and i["data"].get("artifact_type") == "task"
    ]
    assert len(artifacts) == 1
    assert (
        artifacts[0]["data"]["payload"]["projected_run_started_ms"]
        == started.timestamp() * 1000.0
    )


async def test_wholesale_fallback_dedups_legacy_interleaved_archive(monkeypatch):
    """Pre-mux archives interleaved task frames in the root stream. The
    projection is the transcript authority for claimed agents (as on the
    normal path), so the stored duplicate rows are dropped — single render,
    from the checkpoint."""
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
    }
    turn_msgs = [
        AIMessage(content="![chart](work/chart.png)", id="ai-1"),
        AIMessage(
            content="",
            id="ai-2",
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
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_messages=[AIMessage(content="projected copy", id="sub-ai-1")],
    )
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "content": "stored task copy",
                "content_type": "text",
                "agent": "task:tsk1",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    assert [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ] == ["projected copy"]


async def test_wholesale_fallback_tool_only_archive_renders_once(monkeypatch):
    """A collector-replaced archive for a run with no model text carries the
    opener/tool frames but zero message_chunk rows. The projection claims the
    agent, so the stored duplicates are dropped and the transcript renders
    once (no double opener, no duplicate tools); the stored error row has no
    projected counterpart and survives."""
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
    }
    turn_msgs = [
        AIMessage(content="![chart](work/chart.png)", id="ai-1"),
        AIMessage(
            content="",
            id="ai-2",
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
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(
                content="",
                id="sub-ai-1",
                tool_calls=[{"name": "bash", "args": {"cmd": "ls"}, "id": "tc-s"}],
            ),
            ToolMessage(
                content="files", tool_call_id="tc-s", name="bash", id="sub-tm-1"
            ),
        ],
    )
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "user_message",
            "data": {"agent": "task:tsk1", "role": "user", "content": "p"},
        },
        {
            "event": "tool_calls",
            "data": {
                "agent": "task:tsk1",
                "role": "assistant",
                "tool_calls": [{"name": "bash", "args": {"cmd": "ls"}, "id": "tc-s"}],
            },
        },
        {
            "event": "tool_call_result",
            "data": {
                "agent": "task:tsk1",
                "role": "assistant",
                "tool_call_id": "tc-s",
                "content": "files",
            },
        },
        {
            "event": "error",
            "data": {"agent": "task:tsk1", "error": "model call failed"},
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    task_rows = [
        i for i in items if (i["data"] or {}).get("agent") == "task:tsk1"
    ]
    # Exactly one opener and one tool round — the projected copy; the stored
    # error row has no projected twin and inserts after its stored anchor
    # (the tool result), keeping its original mid-transcript position.
    assert [i["event"] for i in task_rows] == [
        "user_message",
        "tool_calls",
        "tool_call_result",
        "error",
    ]
    # The transcript rows are the projected ones (checkpoint message ids),
    # not the stored copies.
    assert [i["data"].get("id") for i in task_rows[:3]] == [
        "sub-h-1",
        "sub-ai-1",
        "sub-tm-1",
    ]


async def test_wholesale_fallback_stop_snapshot_does_not_evict_projection(
    monkeypatch,
):
    """A user-stop drain appends a PARTIAL task snapshot to the root archive
    (drained rows up to a high-water mark + a synthetic ``stopped`` close),
    bypassing the collector — transcript-class rows without completeness.
    The projection claims the cancelled run from the checkpoint, which holds
    the full transcript: it must render, and the partial stored rows must
    not evict it."""
    from datetime import datetime, timezone

    started = datetime(2026, 1, 3, tzinfo=timezone.utc)
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
        "task_run_id": "run-1",
    }
    turn_msgs = [
        AIMessage(content="![chart](work/chart.png)", id="ai-1"),
        AIMessage(
            content="",
            id="ai-2",
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
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(content="partial answer plus the checkpointed tail", id="sub-ai-1"),
        ],
    )
    reader.aget_task_run_stamps = AsyncMock(return_value=["run-1"])
    monkeypatch.setattr(
        "src.server.services.history.replay.sr_db.list_runs_for_thread",
        AsyncMock(
            return_value=[
                {
                    "task_run_id": "run-1",
                    "status": "cancelled",
                    "started_at": started,
                }
            ]
        ),
    )
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "artifact",
            "data": {
                "artifact_type": "task",
                "artifact_id": "task:tsk1",
                "agent": "main",
                "payload": {
                    "task_id": "tsk1",
                    "action": "init",
                    "task_run_id": "run-1",
                },
            },
        },
        {
            "event": "user_message",
            "data": {"agent": "task:tsk1", "role": "user", "content": "p"},
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "role": "assistant",
                "content": "partial answer",
                "content_type": "text",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "task:tsk1:stopped",
                "role": "assistant",
                "content": "",
                "content_type": "text",
                "finish_reason": "stopped",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    # The full checkpointed transcript renders, once; the partial snapshot
    # rows (opener, truncated chunk, synthetic close) are gone.
    assert [
        i["data"].get("content")
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ] == ["partial answer plus the checkpointed tail"]
    assert [
        i["data"].get("id")
        for i in items
        if i["event"] == "user_message" and i["data"].get("agent") == "task:tsk1"
    ] == ["sub-h-1"]
    # The launch artifact keeps the ledger-exact watermark — truthful, the
    # transcript is in the payload.
    task_artifacts = [
        i
        for i in items
        if i["event"] == "artifact" and i["data"].get("artifact_type") == "task"
    ]
    assert len(task_artifacts) == 1
    assert (
        task_artifacts[0]["data"]["payload"]["projected_run_started_ms"]
        == started.timestamp() * 1000.0
    )


def _wholesale_task_turn():
    """A turn launching tsk1 whose main lane has an unresolved sandbox image
    (activates the wholesale branch)."""
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
    }
    return [
        AIMessage(content="![chart](work/chart.png)", id="ai-1"),
        AIMessage(
            content="",
            id="ai-2",
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


async def test_wholesale_fallback_restores_evicted_task_results(monkeypatch):
    """The task checkpoint holds only the eviction pointer for a large tool
    result; the stored copy holds the fuller content. The wholesale branch
    must apply the same restore as the normal path — projection authority
    must not discard stored content the checkpoint genuinely lacks."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, _wholesale_task_turn())]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(
                content="",
                id="sub-ai-1",
                tool_calls=[{"name": "bash", "args": {"cmd": "x"}, "id": "tc-1"}],
            ),
            ToolMessage(
                content=_EVICTION_POINTER,
                tool_call_id="tc-1",
                name="bash",
                id="sub-tm-1",
            ),
        ],
    )
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "tool_call_result",
            "data": {
                "agent": "task:tsk1",
                "role": "assistant",
                "tool_call_id": "tc-1",
                "content": "the full captured result",
                "content_type": "text",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    results = [
        i
        for i in items
        if i["event"] == "tool_call_result"
        and i["data"].get("agent") == "task:tsk1"
    ]
    assert len(results) == 1
    assert results[0]["data"]["content"] == "the full captured result"


async def test_wholesale_fallback_task_signals_render_once_in_position(
    monkeypatch,
):
    """Stored task-scoped signal rows (steering_delivered here) are the
    preferred copy — the projected twin is dropped and the stored row is
    anchored at its original mid-transcript position, exactly as on the
    normal path. Two copies would advance the client's run grouping twice."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, _wholesale_task_turn())]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(content="before steering", id="sub-ai-1"),
            HumanMessage(
                content="[Steering from User]\nfocus on X",
                id="sub-h-2",
                additional_kwargs={
                    "lc_source": "steering",
                    "steering_delivered": {"count": 1, "content": "focus on X"},
                },
            ),
            AIMessage(content="after steering", id="sub-ai-2"),
        ],
    )
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "live-1",
                "content": "before steering",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "steering_delivered",
            "data": {
                "agent": "task:tsk1",
                "count": 1,
                "content": "focus on X",
                "stored_marker": True,
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "live-2",
                "content": "after steering",
                "content_type": "text",
                "role": "assistant",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    task_rows = [
        i for i in items if (i["data"] or {}).get("agent") == "task:tsk1"
    ]
    steering = [i for i in task_rows if i["event"] == "steering_delivered"]
    # Exactly one — the stored copy, in position between the two chunks.
    assert len(steering) == 1
    assert steering[0]["data"].get("stored_marker") is True
    kinds = [
        (i["event"], i["data"].get("content"))
        for i in task_rows
        if i["event"] in ("message_chunk", "steering_delivered")
    ]
    assert kinds == [
        ("message_chunk", "before steering"),
        ("steering_delivered", "focus on X"),
        ("message_chunk", "after steering"),
    ]


def _ledgered_run(monkeypatch, reader, status):
    """Stamp-aligned single run 'run-1' with the given terminal status."""
    from datetime import datetime, timezone

    started = datetime(2026, 1, 3, tzinfo=timezone.utc)
    reader.aget_task_run_stamps = AsyncMock(return_value=["run-1"])
    monkeypatch.setattr(
        "src.server.services.history.replay.sr_db.list_runs_for_thread",
        AsyncMock(
            return_value=[
                {
                    "task_run_id": "run-1",
                    "status": status,
                    "started_at": started,
                }
            ]
        ),
    )
    return started


def _ledgered_task_turn():
    """_wholesale_task_turn with the launch stamped task_run_id=run-1."""
    msgs = _wholesale_task_turn()
    msgs[-1].additional_kwargs["task_artifact"]["task_run_id"] = "run-1"
    return msgs


_ERRORED_RUN_STORED_TASK_ROWS = [
    {
        "event": "user_message",
        "data": {"agent": "task:tsk1", "role": "user", "content": "p"},
    },
    {
        "event": "message_chunk",
        "data": {
            "agent": "task:tsk1",
            "id": "lc-1",
            "content": "The answer is",
            "content_type": "text",
            "role": "assistant",
        },
    },
    {
        "event": "message_chunk",
        "data": {
            "agent": "task:tsk1",
            "id": "lc-1",
            "content": " incomplete",
            "content_type": "text",
            "role": "assistant",
        },
    },
    {
        "event": "error",
        "data": {"agent": "task:tsk1", "error": "model call failed"},
    },
]


async def test_wholesale_fallback_errored_run_partial_text_survives(monkeypatch):
    """A model call that raises mid-stream leaves partial text in the capture
    that the checkpoint never committed (only the opener checkpointed). For
    the errored run's lane the merge resurrects those trailing stored chunks
    after the projected opener — text the user saw live must survive reload."""
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, _ledgered_task_turn())]),
        task_messages=[HumanMessage(content="p", id="sub-h-1")],
    )
    _ledgered_run(monkeypatch, reader, "error")
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        *_ERRORED_RUN_STORED_TASK_ROWS,
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    task_rows = [
        i for i in items if (i["data"] or {}).get("agent") == "task:tsk1"
    ]
    assert [(i["event"], i["data"].get("content")) for i in task_rows] == [
        ("user_message", "p"),
        ("message_chunk", "The answer is"),
        ("message_chunk", " incomplete"),
        ("error", None),
    ]
    # The single opener is the projected one.
    assert task_rows[0]["data"].get("id") == "sub-h-1"


async def test_normal_path_errored_run_partial_text_survives(monkeypatch):
    """Same resurrection on the normal (no unresolved image) path — the fix
    lives in the shared merge, not the wholesale branch."""
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, msgs)]),
        task_messages=[HumanMessage(content="p", id="sub-h-1")],
    )
    _ledgered_run(monkeypatch, reader, "error")
    stored = list(_ERRORED_RUN_STORED_TASK_ROWS)

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    chunks = [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    assert chunks == ["The answer is", " incomplete"]


async def test_completed_run_phantom_partials_stay_dropped(monkeypatch):
    """A completed run can leave phantom partials in the capture (a model
    attempt that failed mid-stream before an in-run retry re-streamed the
    full text). Resurrection is off for completed runs — replaying the
    phantom beside the checkpointed message would double-render."""
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, msgs)]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(content="the full final text", id="sub-ai-1"),
        ],
    )
    _ledgered_run(monkeypatch, reader, "completed")
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-phantom",
                "content": "the full",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-final",
                "content": "the full final text",
                "content_type": "text",
                "role": "assistant",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    chunks = [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    assert chunks == ["the full final text"]


async def test_errored_run_committed_copy_is_not_resurrected(monkeypatch):
    """A phantom partial in an errored run shifts stored lane ordinals: the
    committed message's stored copy lands beyond the projected count and
    looks trailing. Content matching marks it as the checkpointed message's
    duplicate — only genuinely capture-only output resurrects, once."""
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, msgs)]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(content="the full final text", id="sub-ai-1"),
        ],
    )
    _ledgered_run(monkeypatch, reader, "error")
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-phantom",
                "content": "the full fin",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-committed",
                "content": "the full final text",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-tail",
                "content": "and then it died",
                "content_type": "text",
                "role": "assistant",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    chunks = [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    # The committed copy renders once (projected); the capture-only tail
    # survives; the phantom stays consumed.
    assert chunks == ["the full final text", "and then it died"]


def _cache_probe(monkeypatch):
    """Absorb cache writes; return the list of cached tail checkpoint ids."""
    from src.server.services.history import replay as replay_module

    async def fake_statuses(thread_id, task_ids):
        return {}

    async def fake_live(thread_id, task_ids):
        return False

    cached: list[str] = []

    async def fake_store(thread_id, tail_checkpoint_id, items):
        cached.append(tail_checkpoint_id)

    async def fake_delete(thread_id, tail_checkpoint_ids):
        pass

    monkeypatch.setattr(replay_module, "resolve_task_statuses", fake_statuses)
    monkeypatch.setattr(
        replay_module.projection_cache, "task_streams_live", fake_live
    )
    monkeypatch.setattr(replay_module.projection_cache, "store_turn", fake_store)
    monkeypatch.setattr(replay_module.projection_cache, "delete_turns", fake_delete)
    return cached


async def test_lossy_lane_awaiting_collector_stays_uncacheable(monkeypatch):
    """The collector races the refresh-at-finalize and never invalidates the
    projection cache: an errored run's turn built before the captured rows
    reach Postgres must stay uncacheable (rebuild per read), else the
    opener-only build freezes the loss for the cache TTL. Once the lane has
    stored rows the atomic collector write has landed and the turn caches."""
    cached = _cache_probe(monkeypatch)
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    turn = _turn(0, msgs)
    turn.tail_checkpoint_id = "tail-0"
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[turn]),
        task_messages=[HumanMessage(content="p", id="sub-h-1")],
    )
    _ledgered_run(monkeypatch, reader, "error")

    # Archive not yet written: no task-lane stored rows -> uncacheable.
    await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    assert cached == []

    # Collector landed: lane rows present -> cacheable again.
    stored = list(_ERRORED_RUN_STORED_TASK_ROWS)
    await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    assert cached == ["tail-0"]


async def test_evicted_pointer_awaiting_collector_stays_uncacheable(monkeypatch):
    """Same race, completed run: the checkpoint holds only the eviction
    pointer and the fuller stored result exists only in the collector's
    pending archive — caching the pointer build would freeze it."""
    cached = _cache_probe(monkeypatch)
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    turn = _turn(0, msgs)
    turn.tail_checkpoint_id = "tail-0"
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[turn]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(
                content="",
                id="sub-ai-1",
                tool_calls=[{"name": "bash", "args": {"cmd": "x"}, "id": "tc-1"}],
            ),
            ToolMessage(
                content=_EVICTION_POINTER,
                tool_call_id="tc-1",
                name="bash",
                id="sub-tm-1",
            ),
        ],
    )
    _ledgered_run(monkeypatch, reader, "completed")

    await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    assert cached == []


async def test_task_custom_artifact_does_not_clear_archive_gate(monkeypatch):
    """Task custom artifacts (todo/ui) are written to the root archive LIVE,
    before any collector runs — an artifact row is lane presence, not
    archive evidence. Only transcript-class rows (which the atomic archive
    writers alone produce) clear the awaiting-archive debt."""
    cached = _cache_probe(monkeypatch)
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    turn = _turn(0, msgs)
    turn.tail_checkpoint_id = "tail-0"
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[turn]),
        task_messages=[HumanMessage(content="p", id="sub-h-1")],
    )
    _ledgered_run(monkeypatch, reader, "error")
    artifact_only = [
        {
            "event": "artifact",
            "data": {
                "artifact_type": "todo_list",
                "artifact_id": "todo-1",
                "agent": "task:tsk1",
                "payload": {"todos": []},
            },
        }
    ]

    await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, list(artifact_only))}
    )
    assert cached == []

    # Transcript rows present -> the archive landed -> cacheable.
    await build_checkpoint_replay_items(
        THREAD,
        [_query(0)],
        {0: _response(0, artifact_only + list(_ERRORED_RUN_STORED_TASK_ROWS))},
    )
    assert cached == ["tail-0"]


async def test_repeated_text_capture_only_message_resurrects(monkeypatch):
    """Identical content is not identity: a NEW capture-only message whose
    text repeats an earlier checkpointed message (agent loops emit repeated
    status texts) must still resurrect. Alignment consumes each projected
    message once, in order — only the first stored occurrence is the copy."""
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, msgs)]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(content="Still working", id="sub-ai-1"),
        ],
    )
    _ledgered_run(monkeypatch, reader, "error")
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-copy",
                "content": "Still working",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-new",
                "content": "Still working",
                "content_type": "text",
                "role": "assistant",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    task_chunks = [
        (i["data"].get("id"), i["data"]["content"])
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    # The projected message renders once; the distinct repeated message
    # resurrects under its own id.
    assert task_chunks == [
        ("sub-ai-1", "Still working"),
        ("lc-new", "Still working"),
    ]


async def test_rewritten_image_copy_is_not_resurrected(monkeypatch):
    """The checkpoint copy of a message carries the durable image URL while
    the archive copy keeps the sandbox path split across token fragments
    (the row-level archive rewrite can't see fragmented markdown). Matching
    normalizes image targets, so the shifted committed copy is still
    recognized and only genuinely lost output resurrects."""
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, msgs)]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(
                content="Here: ![chart](https://cdn/chart.png) done",
                id="sub-ai-1",
            ),
        ],
    )
    _ledgered_run(monkeypatch, reader, "error")
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-phantom",
                "content": "Here",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-copy",
                "content": "Here: ![chart](work/",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-copy",
                "content": "chart.png) done",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-tail",
                "content": "and then it died",
                "content_type": "text",
                "role": "assistant",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    chunks = [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    assert chunks == [
        "Here: ![chart](https://cdn/chart.png) done",
        "and then it died",
    ]


async def test_phantom_matching_later_text_does_not_displace_copies(monkeypatch):
    """A phantom whose text equals a LATER committed message must not consume
    that message's alignment slot: the pairing maximizes in-order matches,
    so both real copies align and nothing double-renders."""
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, msgs)]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(content="foo", id="sub-ai-1"),
            AIMessage(content="bar", id="sub-ai-2"),
        ],
    )
    _ledgered_run(monkeypatch, reader, "error")
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-phantom",
                "content": "bar",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-copy-a",
                "content": "foo",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-copy-b",
                "content": "bar",
                "content_type": "text",
                "role": "assistant",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    task_chunks = [
        (i["data"].get("id"), i["data"]["content"])
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    assert task_chunks == [("sub-ai-1", "foo"), ("sub-ai-2", "bar")]


async def test_distinct_image_targets_do_not_collide(monkeypatch):
    """Two image-only messages sharing alt text but pointing at DIFFERENT
    files must keep distinct signatures — target normalization keeps the
    basename (the rewrite preserves it), it does not strip the target.
    Stripping would collapse phantom and both copies into one signature,
    mispair the alignment, and resurrect the second copy as a duplicate."""
    msgs = _ledgered_task_turn()
    msgs[0] = AIMessage(content="plain main text", id="ai-1")
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, msgs)]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(content="![chart](https://cdn/x1/a.png)", id="sub-ai-1"),
            AIMessage(content="![chart](https://cdn/x2/b.png)", id="sub-ai-2"),
        ],
    )
    _ledgered_run(monkeypatch, reader, "error")
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-phantom",
                "content": "![chart](work/p.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-copy-a",
                "content": "![chart](work/a.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:tsk1",
                "id": "lc-copy-b",
                "content": "![chart](work/b.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    task_chunks = [
        (i["data"].get("id"), i["data"]["content"])
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    # Both projected messages render once; the phantom (within the projected
    # lane count) is suppressed and neither copy resurrects.
    assert task_chunks == [
        ("sub-ai-1", "![chart](https://cdn/x1/a.png)"),
        ("sub-ai-2", "![chart](https://cdn/x2/b.png)"),
    ]


async def test_wholesale_fallback_task_custom_artifact_is_not_ownership(monkeypatch):
    """A post-mux archive can hold a task-scoped CUSTOM artifact (todo/
    file-op/ui, agent task:<id>) while the task's message frames stay on the
    per-task channel. That row is not transcript evidence: the projected
    transcript must survive, the stored artifact must still render, and the
    launch artifact keeps its truthful watermark."""
    from datetime import datetime, timezone

    started = datetime(2026, 1, 3, tzinfo=timezone.utc)
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
        "task_run_id": "run-1",
    }
    turn_msgs = [
        AIMessage(content="![chart](work/chart.png)", id="ai-1"),
        AIMessage(
            content="",
            id="ai-2",
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
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_messages=[AIMessage(content="task answer", id="sub-ai-1")],
    )
    reader.aget_task_run_stamps = AsyncMock(return_value=["run-1"])
    monkeypatch.setattr(
        "src.server.services.history.replay.sr_db.list_runs_for_thread",
        AsyncMock(
            return_value=[
                {
                    "task_run_id": "run-1",
                    "status": "completed",
                    "started_at": started,
                }
            ]
        ),
    )
    stored = [
        {
            "event": "message_chunk",
            "data": {
                "content": "![chart](https://cdn/rewritten.png)",
                "content_type": "text",
                "role": "assistant",
            },
        },
        {
            "event": "artifact",
            "data": {
                "artifact_type": "task",
                "artifact_id": "task:tsk1",
                "agent": "main",
                "payload": {
                    "task_id": "tsk1",
                    "action": "init",
                    "task_run_id": "run-1",
                },
            },
        },
        {
            "event": "artifact",
            "data": {
                "artifact_type": "todo_list",
                "artifact_id": "todo-1",
                "agent": "task:tsk1",
                "payload": {"todos": []},
            },
        },
    ]

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0, stored)}
    )
    # The projected task transcript survived despite the stored custom artifact.
    assert [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ] == ["task answer"]
    # The stored custom artifact still renders alongside it.
    assert [
        i["data"]["artifact_id"]
        for i in items
        if i["event"] == "artifact" and i["data"].get("artifact_type") == "todo_list"
    ] == ["todo-1"]
    # And the launch artifact keeps the ledger-exact watermark.
    task_artifacts = [
        i
        for i in items
        if i["event"] == "artifact" and i["data"].get("artifact_type") == "task"
    ]
    assert len(task_artifacts) == 1
    assert (
        task_artifacts[0]["data"]["payload"]["projected_run_started_ms"]
        == started.timestamp() * 1000.0
    )


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
    image_record = {
        "type": "ui",
        "id": "ui-1",
        "name": "image_capture",
        "props": {"path_to_url": {"work/sub.png": "https://cdn/sub.png"}},
    }
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[
            _turn(0, turn_msgs, new_ui_records=[image_record]),
            _turn(1, [AIMessage(content="done", id="ai-2")]),
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
    reader.aget_task_history.assert_awaited_once_with(THREAD, "tsk1")
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


async def test_subagent_private_state_and_ui_are_checkpoint_projected(monkeypatch):
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
    }
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
    task_history = TaskHistory(
        messages=[AIMessage(content="task answer", id="sub-ai-1")],
        new_summarization_event={
            "summary_message": build_summary_message(
                "task summary", None, original_message_count=12
            ),
            "cutoff_index": 4,
        },
        newly_offloaded_args=2,
        newly_offloaded_reads=1,
        new_ui_records=[
            {
                "type": "ui",
                "id": "task-fallback",
                "name": "model_fallback",
                "props": {
                    "from_model": "primary",
                    "to_model": "fallback",
                    "error": "api_key=sk-abcdef0123456789",
                },
                "metadata": {},
            }
        ],
    )
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_history=task_history,
    )

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0)}
    )
    task_items = [i for i in items if i["data"].get("agent") == "task:tsk1"]

    assert [i["event"] for i in task_items] == [
        "context_window",
        "context_window",
        "context_window",
        "model_fallback",
        "message_chunk",
    ]
    assert task_items[0]["data"]["offloaded_args"] == 2
    assert task_items[1]["data"]["offloaded_reads"] == 1
    assert task_items[2]["data"]["summary_text"] == "task summary"
    assert "sk-abcdef" not in task_items[3]["data"]["error"]
    assert task_items[4]["data"]["content"] == "task answer"


async def test_claimed_run_stamps_projection_watermark(monkeypatch):
    """A settled ledgered run the build claims stamps its start onto the task
    artifact (``projected_run_started_ms``) — the client's authority for
    which runs the payload already contains."""
    from datetime import datetime, timezone

    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
        "task_run_id": "run-1",
    }
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
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_messages=[AIMessage(content="task answer", id="sub-ai-1")],
    )
    monkeypatch.setattr(
        "src.server.services.history.replay.sr_db.list_runs_for_thread",
        AsyncMock(
            return_value=[
                {
                    "task_run_id": "run-1",
                    "status": "completed",
                    "started_at": started,
                }
            ]
        ),
    )

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0)}
    )
    artifacts = [
        i
        for i in items
        if i["event"] == "artifact"
        and i["data"].get("artifact_type") == "task"
    ]
    assert len(artifacts) == 1
    assert (
        artifacts[0]["data"]["payload"]["projected_run_started_ms"]
        == started.timestamp() * 1000.0
    )
    # The claimed transcript really is in the payload.
    assert [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk"
        and i["data"].get("agent") == "task:tsk1"
    ] == ["task answer"]


async def test_in_progress_run_is_excluded_from_the_watermark(monkeypatch):
    """A run the build skips (in_progress — its live stream owns it) must not
    enter the watermark: stamping it would make the client drop the run's
    later drain even though its transcript was never projected."""
    from datetime import datetime, timezone

    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
        "task_run_id": "run-1",
    }
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
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_messages=[AIMessage(content="live text", id="sub-ai-1")],
    )
    reader.aget_task_run_stamps = AsyncMock(return_value=["run-1"])
    monkeypatch.setattr(
        "src.server.services.history.replay.sr_db.list_runs_for_thread",
        AsyncMock(
            return_value=[
                {
                    "task_run_id": "run-1",
                    "status": "in_progress",
                    "started_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
                }
            ]
        ),
    )

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0)}
    )
    artifacts = [
        i
        for i in items
        if i["event"] == "artifact"
        and i["data"].get("artifact_type") == "task"
    ]
    assert len(artifacts) == 1
    assert "projected_run_started_ms" not in artifacts[0]["data"]["payload"]
    # And the skipped run's transcript stayed out of the payload too.
    assert not [
        i
        for i in items
        if i["event"] == "message_chunk"
        and i["data"].get("agent") == "task:tsk1"
    ]


async def test_subagent_checkpoint_read_failure_makes_replay_unavailable(monkeypatch):
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
    }
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
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
    )
    reader.aget_task_history = AsyncMock(
        side_effect=RuntimeError("checkpoint pool unavailable")
    )

    with pytest.raises(
        CheckpointReplayUnavailable,
        match="subagent checkpoint state unavailable for task:tsk1",
    ):
        await build_checkpoint_replay_items(
            THREAD, [_query(0)], {0: _response(0)}
        )


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
    reader.aget_task_history = AsyncMock(return_value=TaskHistory())
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


async def test_credit_usage_ignores_later_subagent_usage_rows(monkeypatch):
    """Task billing rows share the parent response id but were not emitted as
    the main workflow's terminal credit_usage event."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[_turn(0, [AIMessage(content="done", id="a-0")])],
        ),
    )
    main = {
        "conversation_response_id": "resp-0",
        "msg_type": "ptc",
        "token_usage": {
            "by_model": {
                "main": {
                    "input_tokens": 100,
                    "output_tokens": 20,
                    "total_tokens": 120,
                }
            }
        },
        "total_credits": 2.5,
        "created_at": "2026-07-07T00:00:00",
    }
    task = {
        "conversation_response_id": "resp-0",
        "msg_type": "task",
        "token_usage": {
            "by_model": {
                "task": {
                    "input_tokens": 5,
                    "output_tokens": 1,
                    "total_tokens": 6,
                }
            }
        },
        "total_credits": 0.1,
        "created_at": "2026-07-07T00:01:00",
    }

    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0)],
        {0: _response(0)},
        usages=[main, task],
    )

    credit = next(i for i in items if i["event"] == "credit_usage")
    assert credit["data"]["tokens"]["total_tokens"] == 120
    assert credit["data"]["total_credits"] == 2.5


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


async def test_terminal_error_replay_sanitizes_legacy_rows(monkeypatch):
    """Defense in depth for rows written before persistence-side scrubbing."""
    response = _response(0, status="error")
    response["errors"] = [
        "request failed: https://user:hunter2@api.example.test "
        "api_key=sk-abcdef0123456789"
    ]
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[_turn(0, [AIMessage(content="partial", id="a-0")])],
        ),
    )

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: response}
    )

    error = next(i for i in items if i["event"] == "error")
    assert "hunter2" not in error["data"]["error"]
    assert "sk-abcdef" not in error["data"]["error"]
    assert "[REDACTED]" in error["data"]["error"]


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
    # The passthrough insert copies the stored event's data — enrichment must
    # not mutate the pristine source row.
    assert "turn_index" not in stored[1]["data"]


async def test_resumed_run_segments_attribute_to_their_launch_turns(monkeypatch):
    # A task namespace holds one segment per run; each launch claims the
    # segment whose boundary opener equals its artifact prompt. A launch
    # whose run never wrote a boundary (a failed/no-op resume) projects
    # nothing — positional pairing would hand it the next run's transcript.
    def launch(ordinal, action, prompt):
        artifact = {
            "task_id": "tsk1",
            "action": action,
            "description": "d",
            "prompt": prompt,
        }
        return [
            AIMessage(
                content="",
                id=f"ai-{ordinal}",
                tool_calls=[{"name": "Task", "args": {}, "id": f"tc-{ordinal}"}],
            ),
            ToolMessage(
                content="dispatched",
                tool_call_id=f"tc-{ordinal}",
                name="Task",
                id=f"tm-{ordinal}",
                additional_kwargs={"task_artifact": artifact},
            ),
        ]

    history = ThreadHistory(
        thread_id=THREAD,
        turns=[
            _turn(0, launch(0, "init", "run one")),
            _turn(1, launch(1, "resume", "run two")),  # no-op: wrote no segment
            _turn(2, launch(2, "resume", "run three")),
        ],
    )
    _mock_reader(
        monkeypatch,
        history,
        task_messages=[
            HumanMessage(content="run one", id="sub-h-1"),
            AIMessage(content="one done", id="sub-ai-1"),
            HumanMessage(content="run three", id="sub-h-3"),
            AIMessage(content="three done", id="sub-ai-3"),
        ],
    )
    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1, content="r2"), _query(2, content="r3")],
        {0: _response(0), 1: _response(1), 2: _response(2)},
    )
    openers = [
        i["data"]
        for i in items
        if i["event"] == "user_message" and i["data"].get("agent") == "task:tsk1"
    ]
    assert [(o["content"], o["turn_index"], o["response_id"]) for o in openers] == [
        ("run one", 0, "resp-0"),
        ("run three", 2, "resp-2"),
    ]
    chunks = {
        i["data"]["content"]: i["data"]["turn_index"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    }
    assert chunks == {"one done": 0, "three done": 2}


async def test_live_task_final_launch_defers_to_stream(monkeypatch):
    # Tail mode: the launching turn commits while the task still writes, so
    # the namespace already holds the in-flight run's boundary. Replay must
    # not project that segment — the live stream replays the same epoch from
    # seq 1, and both together render the instruction bubble twice. Earlier
    # settled runs still attribute; the live remainder is not salvaged as
    # trailing either.
    from src.server.services.history import replay as replay_module

    async def fake_statuses(thread_id, task_ids):
        return {tid: "running" for tid in task_ids}

    monkeypatch.setattr(replay_module, "resolve_task_statuses", fake_statuses)

    def launch(ordinal, action, prompt):
        artifact = {
            "task_id": "tsk1",
            "action": action,
            "description": "d",
            "prompt": prompt,
        }
        return [
            AIMessage(
                content="",
                id=f"ai-{ordinal}",
                tool_calls=[{"name": "Task", "args": {}, "id": f"tc-{ordinal}"}],
            ),
            ToolMessage(
                content="dispatched",
                tool_call_id=f"tc-{ordinal}",
                name="Task",
                id=f"tm-{ordinal}",
                additional_kwargs={"task_artifact": artifact},
            ),
        ]

    # Resume tail: run one settled, run two mid-flight with its boundary
    # already checkpointed.
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[
                _turn(0, launch(0, "init", "run one")),
                _turn(1, launch(1, "resume", "run two")),
            ],
        ),
        task_messages=[
            HumanMessage(content="run one", id="sub-h-1"),
            AIMessage(content="one done", id="sub-ai-1"),
            HumanMessage(content="run two", id="sub-h-2"),
            AIMessage(content="two partial", id="sub-ai-2"),
        ],
    )
    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1, content="r2")],
        {0: _response(0), 1: _response(1)},
    )
    task_items = [
        i["data"].get("content")
        for i in items
        if i["data"].get("agent") == "task:tsk1"
    ]
    assert task_items == ["run one", "one done"]

    # Init tail (a freshly spawned live task): the only launch is the final
    # one, so replay projects nothing from the namespace.
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD, turns=[_turn(0, launch(0, "init", "solo run"))]
        ),
        task_messages=[
            HumanMessage(content="solo run", id="sub-h-1"),
            AIMessage(content="solo partial", id="sub-ai-1"),
        ],
    )
    items = await build_checkpoint_replay_items(
        THREAD, [_query(0)], {0: _response(0)}
    )
    assert not [i for i in items if i["data"].get("agent") == "task:tsk1"]


async def test_trailing_salvage_keeps_its_turn_uncacheable(monkeypatch):
    # An orphan run beyond the last committed launch (its launching turn never
    # persisted) is salvaged under that launch's stamps. The salvage-stamped
    # turn must stay OUT of the projection cache: the all-cache-hit fast path
    # never runs the task lane, so a cached entry would replay the turn
    # without the salvage on the next refresh. Fixed regression.
    from src.server.services.history import replay as replay_module

    async def fake_statuses(thread_id, task_ids):
        return {}  # settled — no live writer owns the trailing segment

    async def fake_live(thread_id, task_ids):
        return False

    stored: list[str | None] = []
    deleted: list[str] = []

    async def fake_store(thread_id, tail_checkpoint_id, items):
        stored.append(tail_checkpoint_id)

    async def fake_delete(thread_id, tail_checkpoint_ids):
        deleted.extend(tail_checkpoint_ids)

    monkeypatch.setattr(replay_module, "resolve_task_statuses", fake_statuses)
    monkeypatch.setattr(
        replay_module.projection_cache, "task_streams_live", fake_live
    )
    monkeypatch.setattr(replay_module.projection_cache, "store_turn", fake_store)
    monkeypatch.setattr(replay_module.projection_cache, "delete_turns", fake_delete)

    def launch(ordinal, action, prompt):
        artifact = {
            "task_id": "tsk1",
            "action": action,
            "description": "d",
            "prompt": prompt,
        }
        return [
            AIMessage(
                content="",
                id=f"ai-{ordinal}",
                tool_calls=[{"name": "Task", "args": {}, "id": f"tc-{ordinal}"}],
            ),
            ToolMessage(
                content="dispatched",
                tool_call_id=f"tc-{ordinal}",
                name="Task",
                id=f"tm-{ordinal}",
                additional_kwargs={"task_artifact": artifact},
            ),
        ]

    turn0 = _turn(0, launch(0, "init", "run one"))
    turn0.tail_checkpoint_id = "tail-0"
    turn1 = _turn(1, [AIMessage(content="plain turn", id="ai-plain")])
    turn1.tail_checkpoint_id = "tail-1"
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[turn0, turn1]),
        task_messages=[
            HumanMessage(content="run one", id="sub-h-1"),
            AIMessage(content="one done", id="sub-ai-1"),
            HumanMessage(content="run two", id="sub-h-2"),
            AIMessage(content="orphan done", id="sub-ai-2"),
        ],
    )

    items = await build_checkpoint_replay_items(
        THREAD, [_query(0), _query(1)], {0: _response(0), 1: _response(1)}
    )

    salvaged = [
        i["data"]
        for i in items
        if i["event"] == "message_chunk"
        and i["data"].get("agent") == "task:tsk1"
        and i["data"].get("content") == "orphan done"
    ]
    assert salvaged and salvaged[0]["turn_index"] == 0  # last launch's stamps
    assert stored == ["tail-1"]  # the salvage-stamped turn is never stored
    assert deleted == ["tail-0"]  # and any pre-orphan entry is evicted
