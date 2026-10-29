"""Checkpoint replay: coverage guards, projection, image resolution."""

from __future__ import annotations

from unittest.mock import AsyncMock

import psycopg
import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.server.services.history.reader import TaskHistory
from src.server.services.history.replay import (
    CheckpointReplayUnavailable,
    build_replay_page,
    stub_page,
)
from tests.unit.server.services.history.replay_builders import (
    THREAD,
    TIP,
    ThreadHistory,
    _cache_probe,
    _mock_reader,
    _query,
    _reset_slice_store,
    _response,
    _slice_store,
    _turn,
    build_checkpoint_replay_items,
    thread_rows,
)


pytestmark = pytest.mark.asyncio


async def test_missing_committed_tip_is_replay_unavailable(monkeypatch):
    from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

    reader = _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD))
    reader.thread_history = AsyncMock(
        side_effect=CheckpointBranchTipNotFound(THREAD, "missing-tip")
    )

    with pytest.raises(CheckpointReplayUnavailable, match="missing-tip"):
        await build_checkpoint_replay_items(
            thread_rows([_query(0)], {0: _response(0)}),
            "missing-tip",
        )


async def test_missing_committed_tip_in_the_anchor_walk_is_unavailable(monkeypatch):
    from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

    reader = _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD))
    reader.aget_turn_anchors = AsyncMock(
        side_effect=CheckpointBranchTipNotFound(THREAD, "missing-tip")
    )

    with pytest.raises(CheckpointReplayUnavailable, match="missing-tip"):
        await build_checkpoint_replay_items(
            thread_rows([_query(0)], {0: _response(0)}),
            "missing-tip",
        )


async def test_steered_turn_projects_delivered_event(monkeypatch):
    # Modern steering shape: one turn whose slice carries the stamped steering
    # HumanMessage mid-run. The projector re-emits steering_delivered in
    # position.
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
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
    events = [i["event"] for i in items]
    assert events == ["user_message", "message_chunk", "steering_delivered", "message_chunk"]
    steer = items[2]["data"]
    assert steer["messages"] == delivered["messages"]
    assert steer["turn_index"] == 0
    # The raw "[Steering from User]" text never surfaces as content.
    assert all(
        "[Steering from User]" not in (i["data"].get("content") or "") for i in items
    )


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
        "src.server.services.history.replay.turn.resolve_token_threshold", lambda: 99000
    )
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
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
        thread_rows([_query(0), _query(1)], {0: _response(0), 1: _response(1)}),
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


def _manual_markers(summary_message):
    """A manual offload, then a manual compact, as the endpoints write them."""
    from src.server.services.history.projector import context_window_record

    offload = context_window_record(
        {"action": "offload", "signal": "complete", "offloaded_args": 2, "offloaded_reads": 0}
    )
    compact = context_window_record(
        {
            "action": "summarize",
            "signal": "complete",
            "original_message_count": 9,
            "new_message_count": 5,
            "summary_length": 23,
            "source": "model",
        },
        summary_message_id=summary_message.id,
    )
    return [offload, compact]


async def test_manual_compact_and_offload_close_the_turn_they_followed(monkeypatch):
    # The endpoints write their state into the span of the turn they followed,
    # beside a ui record each. The turn shows the records where the live
    # client showed them, after everything it streamed, and not the state
    # they changed as compaction at its head.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    summary_message = build_summary_message(
        "earlier turns condensed", None, original_message_count=9
    )
    turn = _turn(
        0,
        [AIMessage(content="answer", id="ai-0")],
        new_ui_records=_manual_markers(summary_message),
    )
    turn.new_summarization_event = {"summary_message": summary_message, "cutoff_index": 4}
    turn.newly_offloaded_args = 2
    turns = [turn, _turn(1, [AIMessage(content="next", id="ai-1")])]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0), _query(1)], {0: _response(0), 1: _response(1)}),
    )

    turn0 = [i for i in items if i["data"].get("turn_index") == 0]
    assert [i["event"] for i in turn0] == [
        "user_message",
        "message_chunk",
        "context_window",
        "context_window",
    ]
    offload, compact = turn0[2]["data"], turn0[3]["data"]
    assert offload["agent"] == compact["agent"] == "agent"
    assert offload["action"] == "offload"
    assert offload["offloaded_args"] == 2
    assert offload["offloaded_reads"] == 0
    assert "kind" not in offload
    assert compact["action"] == "summarize"
    assert compact["summary_text"] == "earlier turns condensed"
    assert compact["original_message_count"] == 9
    assert compact["new_message_count"] == 5
    assert compact["source"] == "model"
    assert not [
        i for i in items
        if i["event"] == "context_window" and i["data"].get("turn_index") == 1
    ]


async def test_manual_offload_leaves_the_turns_own_compaction_at_its_head(monkeypatch):
    # The turn compacted and offloaded on its own before the user offloaded
    # more after it: the turn's own signals stay at its head, net of the ids
    # the manual offload added.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    own_summary = build_summary_message("the turn's own", None, original_message_count=30)
    offload, _ = _manual_markers(own_summary)
    turn = _turn(0, [AIMessage(content="answer", id="ai-0")], new_ui_records=[offload])
    turn.new_summarization_event = {"summary_message": own_summary, "cutoff_index": 12}
    turn.newly_offloaded_args = 5
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    signals = [
        (i["data"]["action"], i["data"]["agent"], i["data"].get("offloaded_args"))
        for i in items
        if i["event"] == "context_window"
    ]
    assert signals == [
        ("offload", "main", 3),
        ("summarize", "main", None),
        ("offload", "agent", 2),
    ]
    assert items[-1]["event"] == "context_window"


async def test_a_compact_right_after_the_turn_keeps_the_summary_the_turn_wrote(
    monkeypatch,
):
    # The agent compacted during the turn and the user compacted right after
    # it, so the turn's span ends on the user's summary. The agent's still
    # shows at the turn's head, off the compact's record, and the compact
    # closes the turn as before.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message
    from src.server.services.history.projector import context_window_record

    own_summary = build_summary_message("the turn's own", None, original_message_count=30)
    manual_summary = build_summary_message("condensed", None, original_message_count=9)
    compact = context_window_record(
        {"action": "summarize", "signal": "complete"},
        summary_message_id=manual_summary.id,
        replaced=own_summary,
    )
    turn = _turn(0, [AIMessage(content="answer", id="ai-0")], new_ui_records=[compact])
    turn.new_summarization_event = {"summary_message": manual_summary, "cutoff_index": 4}
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    signals = [
        (i["data"]["agent"], i["data"]["summary_text"], i["data"].get("original_message_count"))
        for i in items
        if i["event"] == "context_window"
    ]
    assert signals == [("main", "the turn's own", 30), ("agent", "condensed", None)]
    assert [i["event"] for i in items][:3] == [
        "user_message",
        "context_window",
        "message_chunk",
    ]


async def test_a_second_compact_does_not_show_the_first_as_the_turns_own(monkeypatch):
    # Both compacts followed the same turn, so its span ends on the second's
    # summary; the first's text comes off the second's record.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message
    from src.server.services.history.projector import context_window_record

    first = build_summary_message("first", None, original_message_count=9)
    second = build_summary_message("second", None, original_message_count=6)
    records = [
        context_window_record(
            {"action": "summarize"},
            summary_message_id=first.id,
        ),
        context_window_record(
            {"action": "summarize"},
            summary_message_id=second.id,
            replaced=first,
        ),
    ]
    turn = _turn(0, [AIMessage(content="answer", id="ai-0")], new_ui_records=records)
    turn.new_summarization_event = {"summary_message": second, "cutoff_index": 2}
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    assert [
        (i["data"]["agent"], i["data"]["summary_text"])
        for i in items
        if i["event"] == "context_window"
    ] == [("agent", "first"), ("agent", "second")]


def _compacted(message, summary):
    """``message`` as the response to a call that compacted to ``summary``,
    stamped as the compaction middleware stamps it."""
    from ptc_agent.agent.middleware.compaction.utils import stamp_compaction_summary
    from src.server.services.history.projector import summarize_payload

    fields = summarize_payload(summary)
    del fields["action"], fields["signal"]
    stamp_compaction_summary(message, summary.id, fields)
    return message


def _signals(items, turn_index=0):
    """Each of the turn's rows, a summarize notice by its text."""
    return [
        i["data"]["summary_text"]
        if i["event"] == "context_window" and i["data"].get("action") == "summarize"
        else i["event"]
        for i in items
        if i["data"].get("turn_index") == turn_index
    ]


async def test_a_mid_turn_compaction_replays_between_the_calls_it_fell_between(
    monkeypatch,
):
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    summary = build_summary_message("condensed", None, original_message_count=30)
    turn = _turn(
        0,
        [
            AIMessage(
                content="",
                id="ai-1",
                tool_calls=[{"name": "Bash", "args": {"command": "ls"}, "id": "tc-1"}],
            ),
            ToolMessage(content="out", tool_call_id="tc-1", name="Bash", id="tm-1"),
            _compacted(AIMessage(content="answer", id="ai-2"), summary),
        ],
    )
    turn.new_summarization_event = {"summary_message": summary, "cutoff_index": 2}
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    assert _signals(items) == [
        "user_message",
        "tool_calls",
        "tool_call_result",
        "condensed",
        "message_chunk",
    ]


async def test_every_compaction_in_a_turn_replays_where_it_fired(monkeypatch):
    # The state diff sees only the last summary; each response keeps its own.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    first = build_summary_message("first", None, original_message_count=30)
    second = build_summary_message("second", None, original_message_count=12)
    turn = _turn(
        0,
        [
            AIMessage(content="one", id="ai-1"),
            _compacted(AIMessage(content="two", id="ai-2"), first),
            _compacted(AIMessage(content="three", id="ai-3"), second),
        ],
    )
    turn.new_summarization_event = {"summary_message": second, "cutoff_index": 2}
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    assert _signals(items) == [
        "user_message",
        "message_chunk",
        "first",
        "message_chunk",
        "second",
        "message_chunk",
    ]


async def test_a_compact_after_the_turn_keeps_its_inline_summary_once(monkeypatch):
    # The compact's record keeps the summary the agent wrote during the turn,
    # which its response already replays where it fired.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message
    from src.server.services.history.projector import context_window_record

    own_summary = build_summary_message("the turn's own", None, original_message_count=30)
    manual_summary = build_summary_message("condensed", None, original_message_count=9)
    compact = context_window_record(
        {"action": "summarize", "signal": "complete"},
        summary_message_id=manual_summary.id,
        replaced=own_summary,
    )
    turn = _turn(
        0,
        [
            AIMessage(content="one", id="ai-1"),
            _compacted(AIMessage(content="two", id="ai-2"), own_summary),
        ],
        new_ui_records=[compact],
    )
    turn.new_summarization_event = {"summary_message": manual_summary, "cutoff_index": 1}
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    assert _signals(items) == [
        "user_message",
        "message_chunk",
        "the turn's own",
        "message_chunk",
        "condensed",
    ]


async def test_a_subagents_first_run_keeps_its_compaction_notice(monkeypatch):
    # A notice at the head of a task's first run comes before any assistant
    # message the client could hang it on; a stamped one follows the run's
    # earlier responses, where it fired.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

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
    summary = build_summary_message("task summary", None, original_message_count=12)
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_history=TaskHistory(
            messages=[
                HumanMessage(content="p", id="sub-h-1"),
                AIMessage(content="looking", id="sub-ai-1"),
                _compacted(AIMessage(content="task answer", id="sub-ai-2"), summary),
            ],
            new_summarization_event={"summary_message": summary, "cutoff_index": 2},
        ),
    )

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    assert _signals(
        [i for i in items if i["data"].get("agent") == "task:tsk1"]
    ) == ["user_message", "message_chunk", "task summary", "message_chunk"]


@pytest.mark.parametrize("qtype", ["user", "steering"])
async def test_completed_turn_without_boundary_unavailable(monkeypatch, qtype):
    # A completed response always persists its boundary pointer first — a
    # completed row the checkpoints can't pair means they can't cover the
    # thread (e.g. a dangling row, ordinal drift on an unstamped thread, or a
    # legacy steering-backfilled turn, which has no source=input boundary).
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, [])])
    _mock_reader(monkeypatch, history)
    with pytest.raises(CheckpointReplayUnavailable, match="no checkpoint boundary"):
        await build_checkpoint_replay_items(
            thread_rows([_query(0), _query(1, qtype=qtype)], {1: _response(1)}),
        )


async def test_auto_stubs_only_the_completed_turn_without_boundary(monkeypatch):
    # Every other turn still paired, so the thread projects and only the turn
    # its checkpoints cannot cover replays as its stub.
    turns = [
        _turn(n, [AIMessage(content=f"a{i}", id=f"ai-{i}")], turn_index=i)
        for n, i in enumerate((0, 2))
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    page = await build_replay_page(
        thread_rows(
            [_query(i) for i in range(3)], {i: _response(i) for i in range(3)}
        ),
        TIP,
        turn_fallback=True,
    )

    by_turn: dict[int, list[str]] = {}
    for item in (line.item() for line in page.lines):
        by_turn.setdefault(item["data"]["turn_index"], []).append(item["event"])
    # It answered, and nothing here holds the answer.
    assert by_turn[1] == ["user_message", "error"]
    assert "message_chunk" in by_turn[0] and "message_chunk" in by_turn[2]


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
        thread_rows([_query(i) for i in range(4)], responses),
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
        thread_rows([_query(i) for i in range(4)], responses),
    )
    stamps = {
        i["data"]["turn_index"]: i["data"].get("run_id")
        for i in items
        if i["event"] == "user_message"
    }
    assert stamps == {0: "resp-0", 1: "resp-1", 2: "resp-2", 3: None}


async def test_report_back_names_its_run_before_it_ends(monkeypatch):
    # A reload mid report-back draws the row from history and again from the
    # live attach; the client keeps one by the run, which run_id withholds.
    # A running turn has only its in_progress slot, which replay reads by id.
    turns = [_turn(0, [AIMessage(content="a0", id="ai-0")])]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    queries = [_query(0), _query(1, qtype="system")]
    items = await build_checkpoint_replay_items(
        thread_rows(queries, {0: _response(0)}, open_runs={1: "run-1"}),
    )
    users = [i["data"] for i in items if i["event"] == "user_message"]
    assert [u.get("report_run_id") for u in users] == [None, "run-1"]
    assert users[1].get("run_id") is None

    # A thread that falls back to stubs names it the same way.
    stub = await stub_page(thread_rows(queries, {0: _response(0)}, open_runs={1: "run-1"}))
    users = [line.item()["data"] for line in stub.lines if line.event == "user_message"]
    assert [u.get("report_run_id") for u in users] == [None, "run-1"]


async def test_report_back_retried_names_the_run_going():
    # A retry's attempt is the run a live attach draws, not the one it replaces.
    queries = [_query(0, qtype="system")]
    stub = await stub_page(
        thread_rows(queries, {0: _response(0, status="error")}, open_runs={0: "run-2"})
    )
    (user,) = [line.item()["data"] for line in stub.lines if line.event == "user_message"]
    assert user["report_run_id"] == "run-2"


async def test_inflight_windowed_keeps_absolute_pairing(monkeypatch):
    # Regression for the live-proven mislabel: ?limit=N during a streaming
    # turn must not staple the previous turn's answer under the active turn's
    # index. Window covers boundaries 1-2 of a thread whose turn 3 is live.
    turns = [_turn(i, [AIMessage(content=f"a{i}", id=f"ai-{i}")]) for i in range(3)]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    responses = {i: _response(i) for i in range(3)}
    responses[3] = _response(3, status="streaming")
    items = await build_checkpoint_replay_items(
        thread_rows([_query(i) for i in range(4)], responses),
        last_n_turns=2,
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
        thread_rows([_query(i) for i in range(3)], responses),
    )
    chunks = [i["data"] for i in items if i["event"] == "message_chunk"]
    assert [(c["turn_index"], c["content"]) for c in chunks] == [
        (0, "a0"),
        (2, "a2"),
    ]
    turn1_items = [i for i in items if i["data"].get("turn_index") == 1]
    assert [i["event"] for i in turn1_items] == ["user_message"]


async def test_a_resume_after_a_dead_turn_pairs_to_the_next_turn(monkeypatch):
    # Turn 1 never checkpointed, and turn 3 resumes turn 2's interrupt: the
    # resume carries no stamp of its own and names the turn after turn 2,
    # where its position (2) would collide with turn 2.
    turns = [
        _turn(0, [AIMessage(content="a0", id="ai-0")], turn_index=0),
        _turn(1, [AIMessage(content="a2", id="ai-2")], turn_index=2),
        _turn(2, [AIMessage(content="a3", id="ai-3")]),
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    responses = {i: _response(i) for i in range(4)}
    responses[1] = _response(1, status="error")
    items = await build_checkpoint_replay_items(
        thread_rows([_query(i) for i in range(4)], responses),
    )
    chunks = [i["data"] for i in items if i["event"] == "message_chunk"]
    assert [(c["turn_index"], c["content"]) for c in chunks] == [
        (0, "a0"),
        (2, "a2"),
        (3, "a3"),
    ]
    users = [i["data"]["turn_index"] for i in items if i["event"] == "user_message"]
    assert users == [0, 1, 2, 3]


async def test_stamped_turn_index_unknown_unavailable(monkeypatch):
    history = ThreadHistory(
        thread_id=THREAD, turns=[_turn(0, [], turn_index=5)]
    )
    _mock_reader(monkeypatch, history)
    with pytest.raises(CheckpointReplayUnavailable, match="no persisted turn"):
        await build_checkpoint_replay_items(thread_rows([_query(0)], {0: _response(0)}))


async def test_no_turns_unavailable(monkeypatch):
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD))
    with pytest.raises(CheckpointReplayUnavailable, match="no checkpoint turns"):
        await build_checkpoint_replay_items(thread_rows([_query(0)], {}))


async def test_basic_turn_projection_and_enrichment(monkeypatch):
    history = ThreadHistory(
        thread_id=THREAD, turns=[_turn(0, [AIMessage(content="answer", id="ai-1")])]
    )
    _mock_reader(monkeypatch, history)
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
    assert [i["event"] for i in items] == ["user_message", "message_chunk"]
    assert items[0]["data"]["content"] == "hello"
    chunk = items[1]["data"]
    assert chunk["content"] == "answer"
    assert chunk["turn_index"] == 0
    assert chunk["response_id"] == "resp-0"
    assert chunk["thread_id"] == THREAD


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
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
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
        thread_rows([_query(0), _query(1, content="next")], {0: _response(0), 1: _response(1)}),
    )

    chunks = [i["data"]["content"] for i in items if i["event"] == "message_chunk"]
    assert chunks == ["![old](https://cdn/old.png)", "![new](https://cdn/new.png)"]


async def test_image_map_applied_from_the_rows_replay_facts(monkeypatch):
    """A turn's late-captured images are on its row, whatever its ending; a
    turn settled before that keeps resolving through its ui record."""
    legacy = {
        "type": "ui",
        "id": "ui-old",
        "name": "image_capture",
        "props": {"path_to_url": {"work/old.png": "https://cdn/old.png"}},
    }
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[
            _turn(
                0,
                [AIMessage(content="![old](work/old.png)", id="ai-old")],
                new_ui_records=[legacy],
            ),
            _turn(1, [AIMessage(content="![new](work/new.png)", id="ai-new")]),
        ],
    )
    _mock_reader(monkeypatch, history)
    failed = {
        **_response(1, status="error"),
        "replay_facts": {"images": {"work/new.png": "https://cdn/new.png"}},
    }

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0), _query(1, content="next")], {0: _response(0), 1: failed}),
    )

    chunks = [i["data"]["content"] for i in items if i["event"] == "message_chunk"]
    assert chunks == ["![old](https://cdn/old.png)", "![new](https://cdn/new.png)"]


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
        thread_rows([_query(0), _query(1, content="next")], {0: _response(0), 1: _response(1)}),
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
        thread_rows([_query(0)], {0: _response(0)}),
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
        "src.server.services.history.replay.run_lane.sr_db.list_runs_for_thread",
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
        thread_rows([_query(0)], {0: _response(0)}),
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
    reader.task_run_stamps = AsyncMock(return_value=["run-1"])
    monkeypatch.setattr(
        "src.server.services.history.replay.run_lane.sr_db.list_runs_for_thread",
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
        thread_rows([_query(0)], {0: _response(0)}),
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


@pytest.mark.parametrize(
    ("error", "raised", "match"),
    [
        (
            RuntimeError("checkpoint read failed"),
            CheckpointReplayUnavailable,
            "subagent checkpoint state unavailable for task:tsk1",
        ),
        # A database fault is not a turn checkpoints cannot cover: it ends the
        # read as itself, so the turn is not shown as a stub.
        (psycopg.OperationalError("server closed"), psycopg.OperationalError, "closed"),
    ],
)
async def test_a_subagent_read_failure_is_unavailable_unless_the_database_faulted(
    monkeypatch, error, raised, match
):
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
    reader.aget_task_history = AsyncMock(side_effect=error)

    with pytest.raises(raised, match=match):
        await build_checkpoint_replay_items(thread_rows([_query(0)], {0: _response(0)}))


async def test_terminal_interrupts_appended(monkeypatch):
    history = ThreadHistory(
        thread_id=THREAD,
        turns=[_turn(0, [AIMessage(content="asking", id="ai-1")])],
        interrupts=[
            {"id": "int-1", "value": {"action_requests": [{"description": "go?"}]}}
        ],
    )
    _mock_reader(monkeypatch, history)
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
    assert items[-1]["event"] == "interrupt"
    assert items[-1]["data"]["interrupt_id"] == "int-1"
    assert items[-1]["data"]["action_requests"] == [{"description": "go?"}]
    assert items[-1]["data"]["finish_reason"] == "interrupt"


async def test_the_newest_page_raises_its_turns_error_before_its_interrupts(
    monkeypatch,
):
    """The tip's interrupts are read beside the turns: when both reads fail
    the turns' error is the page's, and an interrupt read failing alone still
    fails the page."""
    history = ThreadHistory(
        thread_id=THREAD, turns=[_turn(0, [AIMessage(content="hi", id="ai-1")])]
    )
    reader = _mock_reader(monkeypatch, history)
    reader.aget_tip_interrupts = AsyncMock(side_effect=RuntimeError("interrupts"))
    turn_slices = reader.aget_turn_slices
    reader.aget_turn_slices = AsyncMock(
        side_effect=CheckpointReplayUnavailable("turn slices")
    )
    rows = thread_rows([_query(0)], {0: _response(0)})

    with pytest.raises(CheckpointReplayUnavailable, match="turn slices"):
        await build_replay_page(rows, TIP)

    reader.aget_turn_slices = turn_slices
    with pytest.raises(RuntimeError, match="interrupts"):
        await build_replay_page(rows, TIP)


async def test_system_query_tagged_and_synthetic_human_dropped(monkeypatch):
    # Report-back (the Chief of Staff dispatches PTC, the PTC completion is
    # injected as a new Chief of Staff turn): the turn's boundary input is a <system>-wrapped HumanMessage
    # and its query row is type="system". Checkpoint replay reads that raw
    # HumanMessage from checkpoint state, so it must (a) tag the user_message
    # query_type="system" (the frontend hides the bubble) and (b) never surface
    # the <system> plumbing text as assistant content — the projector drops every
    # HumanMessage.
    system_text = (
        "<system>\nThe analysis you dispatched has completed. "
        "Use agent_output to summarize.\n</system>"
    )
    turn_msgs = [
        HumanMessage(content=system_text, id="h-sys"),
        AIMessage(content="Here is your summary.", id="ai-1"),
    ]
    history = ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    _mock_reader(monkeypatch, history)

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0, content=system_text, qtype="system")], {0: _response(0)}),
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
    # matching tail of query rows: the light anchor walk spans the thread, the
    # state reads only the window.
    turns = [_turn(i, [AIMessage(content=f"a{i}", id=f"ai-{i}")]) for i in range(4)]
    reader = _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=turns))
    queries = [_query(i) for i in range(4)]
    items = await build_checkpoint_replay_items(
        thread_rows(queries, {i: _response(i) for i in range(4)}),
        last_n_turns=2,
    )
    users = [i for i in items if i["event"] == "user_message"]
    assert [u["data"]["turn_index"] for u in users] == [2, 3]  # last two turns only
    reader.aget_turn_slices.assert_awaited_once()
    read = reader.aget_turn_slices.await_args.args[1]
    assert [a.input_checkpoint_id for a in read] == ["cp-in-2", "cp-in-3"]


async def test_widget_data_ref_resolved_from_storage(monkeypatch):
    # The checkpointed artifact carries only data_ref, and replay inlines the
    # data from object storage.
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
        "src.server.services.history.replay.widgets.get_bytes",
        lambda key: b'{"file.csv": "a,b"}' if key == "widgets/t/abc.json" else None,
    )
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
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
    # complete without a storage read.
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

    monkeypatch.setattr("src.server.services.history.replay.widgets.get_bytes", _boom)
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
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
        "src.server.services.history.replay.widgets.get_bytes", lambda key: None
    )
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
    widgets = [
        i for i in items
        if i["event"] == "artifact" and i["data"]["artifact_type"] == "html_widget"
    ]
    payload = widgets[0]["data"]["payload"]
    assert "data" not in payload
    assert payload["html"] == "<div/>"


async def test_answered_interrupt_projected_at_turn_end(monkeypatch):
    """A turn's ending_interrupts (resume-boundary __interrupt__ writes) emit
    interrupt items after the turn content."""
    turn_msgs = [AIMessage(content="asking", id="a-0")]
    turn = _turn(0, turn_msgs)
    turn.anchor.ending_interrupts = [
        {"id": "int-1", "value": {"action_requests": [{"description": "pick one"}]}}
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
    assert [i["event"] for i in items] == ["user_message", "message_chunk", "interrupt"]
    card = items[2]["data"]
    assert card["interrupt_id"] == "int-1"
    assert card["action_requests"] == [{"description": "pick one"}]
    assert card["finish_reason"] == "interrupt"


async def test_credit_usage_synthesized_from_usage_row(monkeypatch):
    """A turn reconstructs the terminal credit_usage event from the
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
        thread_rows([_query(0)], {0: _response(0)}, usages=[usage_row]),
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

    # Errored runs never emitted the event live — no synthesis.
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0, status="error")}, usages=[usage_row]),
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
        thread_rows([_query(0)], {0: _response(0)}, usages=[main, task]),
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
        thread_rows([_query(0)], {0: _response(0)}, provenance=rows),
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


async def test_terminal_error_synthesized_on_both_paths(monkeypatch):
    """An errored response row whose run kept no error frame reconstructs the
    terminal error event, on the checkpoint path and on the stub a fallback
    serves alike."""
    response = _response(0, status="error")
    response["errors"] = ["boom exploded"]
    response["metadata"] = {"error_type": "llm_provider_error", "error_class": "RuntimeError"}

    turn_msgs = [AIMessage(content="partial", id="a-0")]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    items = await build_checkpoint_replay_items(thread_rows([_query(0)], {0: response}))
    assert items[-1]["event"] == "error"
    data = items[-1]["data"]
    assert data["error"] == "boom exploded"
    assert data["type"] == "workflow_error"
    assert data["error_type"] == "llm_provider_error"
    assert data["error_class"] == "RuntimeError"
    assert data["turn_index"] == 0

    stub = await stub_page(thread_rows([_query(0)], {0: response}))
    assert [line.item()["event"] for line in stub.lines] == ["user_message", "error"]
    assert stub.lines[-1].item()["data"]["error"] == "boom exploded"

    # Errored run that never checkpointed a boundary: stub + error item.
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs, turn_index=0)]),
    )
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0), _query(1, content="errored")], {0: _response(0), 1: response | {"conversation_response_id": "resp-1"}}),
    )
    assert [i["event"] for i in items] == [
        "user_message",
        "message_chunk",
        "user_message",
        "error",
    ]
    assert items[-1]["data"]["response_id"] == "resp-1"

    # An errored row with no error text (an exception with no message, or a
    # row from before errors were stored) still ends on the error event that
    # marks the turn failed, its text left for the client to word.
    legacy = _response(0, status="error")
    last = (await stub_page(thread_rows([_query(0)], {0: legacy}))).lines[-1].item()
    assert last["event"] == "error"
    assert last["data"]["error"] == ""


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

    items = await build_checkpoint_replay_items(thread_rows([_query(0)], {0: response}))

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
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
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


def _fallback_record(record_id, after=None):
    return {
        "type": "ui",
        "id": record_id,
        "name": "model_fallback",
        "props": {"from_model": "p", "to_model": "f"},
        "metadata": {"after_message_id": after} if after else {},
    }


def _failed_response(*fallbacks):
    return {
        **_response(0, status="error"),
        "errors": ["all models failed"],
        "replay_facts": {"model_fallbacks": list(fallbacks)},
    }


async def test_model_fallback_a_failed_step_never_saved_replays_from_its_row(
    monkeypatch,
):
    # The failing step's ui record never committed: the row's copy lands
    # after everything the turn saved, whatever message it named, and before
    # the terminal error.
    turn = _turn(0, [AIMessage(content="first", id="ai-1")])
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    items = await build_checkpoint_replay_items(
        thread_rows(
            [_query(0)], {0: _failed_response(_fallback_record("fb-1", "unsaved"))}
        ),
    )
    assert [i["event"] for i in items] == [
        "user_message",
        "message_chunk",
        "model_fallback",
        "error",
    ]
    assert items[2]["data"]["to_model"] == "f"


async def test_model_fallback_saved_and_on_the_row_replays_once(monkeypatch):
    saved = _fallback_record("fb-1")
    turn = _turn(0, [AIMessage(content="ok", id="ai-1")], new_ui_records=[saved])
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _failed_response(saved)}),
    )
    assert [i["event"] for i in items] == [
        "user_message",
        "model_fallback",
        "message_chunk",
        "error",
    ]


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
        thread_rows([_query(0), _query(1, content="r2"), _query(2, content="r3")], {0: _response(0), 1: _response(1), 2: _response(2)}),
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


async def test_namespace_signals_ride_the_first_run_only(monkeypatch):
    # Compaction and fallback signals are namespace-scoped, not per-run: a
    # task with two claimed segments still emits one set, ahead of both. This
    # bounds what the shared message-to-SSE conversion may cover — pulling the
    # signal projections into it would repeat them once per segment.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    def launch(ordinal, action, prompt):
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
                additional_kwargs={
                    "task_artifact": {
                        "task_id": "tsk1",
                        "action": action,
                        "description": "d",
                        "prompt": prompt,
                    }
                },
            ),
        ]

    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[
                _turn(0, launch(0, "init", "run one")),
                _turn(1, launch(1, "resume", "run two")),
            ],
        ),
        task_history=TaskHistory(
            messages=[
                HumanMessage(content="run one", id="sub-h-1"),
                AIMessage(content="one done", id="sub-ai-1"),
                HumanMessage(content="run two", id="sub-h-2"),
                AIMessage(content="two done", id="sub-ai-2"),
            ],
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
                    "props": {"from_model": "primary", "to_model": "fallback"},
                    "metadata": {},
                }
            ],
        ),
    )

    events = [
        i["event"]
        for i in await build_checkpoint_replay_items(
            thread_rows([_query(0), _query(1, content="r2")], {0: _response(0), 1: _response(1)}),
        )
        if i["data"].get("agent") == "task:tsk1"
    ]

    assert events.count("context_window") == 3
    assert events.count("model_fallback") == 1
    assert events.count("message_chunk") == 2
    assert events.index("model_fallback") < events.index("message_chunk")


async def test_live_task_final_launch_defers_to_stream(monkeypatch):
    # Tail mode: the launching turn commits while the task still writes, so
    # the namespace already holds the in-flight run's boundary. Replay must
    # not project that segment — the live stream replays the same epoch from
    # seq 1, and both together render the instruction bubble twice. Earlier
    # settled runs still attribute; the live remainder is not salvaged as
    # trailing either.
    from src.server.services.history.replay import run_lane as task_lane_module

    async def fake_details(thread_id, task_ids):
        return {tid: {"status": "running", "error": None} for tid in task_ids}

    monkeypatch.setattr(task_lane_module, "resolve_task_details", fake_details)

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
        thread_rows([_query(0), _query(1, content="r2")], {0: _response(0), 1: _response(1)}),
    )
    task_items = [
        i["data"].get("content")
        for i in items
        if i["data"].get("agent") == "task:tsk1"
    ]
    assert task_items == ["run one", "one done"]

    # Init tail (a freshly spawned live task): the only launch is the final
    # one, so replay projects nothing from the namespace.
    _reset_slice_store(monkeypatch)
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
        thread_rows([_query(0)], {0: _response(0)}),
    )
    assert not [i for i in items if i["data"].get("agent") == "task:tsk1"]


async def test_run_without_a_committed_launch_is_not_replayed(monkeypatch):
    # An orphan run beyond the last committed launch (its launching turn never
    # committed the launch) is not in the model's context, so replay leaves
    # it out and the turns cache as usual.
    from src.server.services.history import replay as replay_module
    from src.server.services.history.replay import run_lane as task_lane_module

    async def fake_details(thread_id, task_ids):
        return {}  # settled — no live writer owns the trailing segment

    async def fake_live(thread_id, task_ids):
        return dict.fromkeys(task_ids, "sealed")

    monkeypatch.setattr(task_lane_module, "resolve_task_details", fake_details)
    monkeypatch.setattr(
        replay_module.run_lane.task_streams, "task_stream_states", fake_live
    )
    # A pre-ledger task: the ledger reads fine and holds no run for it.
    monkeypatch.setattr(
        task_lane_module.sr_db, "list_runs_for_thread", AsyncMock(return_value=[])
    )
    stored = _slice_store(monkeypatch).lined

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
    turn0.anchor.tail_checkpoint_id = "tail-0"
    turn1 = _turn(1, [AIMessage(content="plain turn", id="ai-plain")])
    turn1.anchor.tail_checkpoint_id = "tail-1"
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
        thread_rows([_query(0), _query(1)], {0: _response(0), 1: _response(1)}),
    )

    task_chunks = [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ]
    assert task_chunks == ["one done"]
    assert stored == ["tail-0", "tail-1"]


async def test_workflow_launch_replays_ns_ui_snapshot(monkeypatch):
    """A RunWorkflow launch (action="workflow") has no transcript in its task
    namespace — only the driver's terminal ui snapshot. Replay must emit that
    record's frames as workflow_lifecycle events, and must not try to claim
    transcript segments for the run task."""
    frames = [
        {
            "agent": "task:wf1",
            "run_id": "wf1",
            "phase": "run_started",
            "name": "briefs",
        },
        {
            "agent": "task:wf1",
            "run_id": "wf1",
            "phase": "run_completed",
            "status": "completed",
        },
    ]
    task_artifact = {
        "task_id": "wf1",
        "action": "workflow",
        "description": "d",
        "task_run_id": "run-w1",
    }
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "RunWorkflow", "args": {}, "id": "tc-w"}],
        ),
        ToolMessage(
            content="dispatched",
            tool_call_id="tc-w",
            name="RunWorkflow",
            id="tm-1",
            additional_kwargs={"task_artifact": task_artifact},
        ),
    ]
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_history=TaskHistory(
            messages=[],
            new_ui_records=[
                {
                    "type": "ui",
                    "id": "workflow-run-run-w1",
                    "name": "workflow_run",
                    "props": {"task_id": "wf1", "frames": frames},
                }
            ],
        ),
    )
    monkeypatch.setattr(
        "src.server.services.history.replay.run_lane.sr_db.list_runs_for_thread",
        AsyncMock(return_value=[]),
    )

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    lifecycle = [i for i in items if i["event"] == "workflow_lifecycle"]
    assert [i["data"]["phase"] for i in lifecycle] == [
        "run_started",
        "run_completed",
    ]
    assert all(i["data"]["agent"] == "task:wf1" for i in lifecycle)
    # Enriched like every projected item, so the client buckets it correctly.
    assert all(i["data"]["thread_id"] == THREAD for i in lifecycle)


async def test_workflow_launch_terminal_status_follows_ledger(monkeypatch):
    """The snapshot is stamped before the terminal CAS, so a raced cancel can
    leave the ledger 'cancelled' under a 'completed' snapshot — replay must
    surface the ledger's verdict, with the row's failure text."""
    frames = [
        {"agent": "task:wf1", "run_id": "wf1", "phase": "run_started"},
        {
            "agent": "task:wf1",
            "run_id": "wf1",
            "phase": "run_completed",
            "status": "completed",
            "error": None,
        },
    ]
    task_artifact = {
        "task_id": "wf1",
        "action": "workflow",
        "description": "d",
        "task_run_id": "run-w1",
    }
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "RunWorkflow", "args": {}, "id": "tc-w"}],
        ),
        ToolMessage(
            content="dispatched",
            tool_call_id="tc-w",
            name="RunWorkflow",
            id="tm-1",
            additional_kwargs={"task_artifact": task_artifact},
        ),
    ]
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_history=TaskHistory(
            messages=[],
            new_ui_records=[
                {
                    "type": "ui",
                    "id": "workflow-run-run-w1",
                    "name": "workflow_run",
                    "props": {"task_id": "wf1", "frames": frames},
                }
            ],
        ),
    )
    monkeypatch.setattr(
        "src.server.services.history.replay.run_lane.sr_db.list_runs_for_thread",
        AsyncMock(
            return_value=[
                {
                    "task_run_id": "run-w1",
                    "status": "cancelled",
                    "failure": None,
                    "started_at": None,
                }
            ]
        ),
    )

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    terminal = [
        i
        for i in items
        if i["event"] == "workflow_lifecycle"
        and i["data"]["phase"] == "run_completed"
    ]
    assert len(terminal) == 1
    assert terminal[0]["data"]["status"] == "cancelled"


async def test_workflow_turn_uncacheable_when_ledger_read_fails(monkeypatch):
    """Ledger-read failure skips workflow status reconciliation; caching that
    build would freeze the snapshot's own (possibly stale) status for the
    cache TTL. The turn must rebuild per read until the ledger is readable."""
    cached = _cache_probe(monkeypatch)
    frames = [
        {"agent": "task:wf1", "run_id": "wf1", "phase": "run_started"},
        {
            "agent": "task:wf1",
            "run_id": "wf1",
            "phase": "run_completed",
            "status": "completed",
            "error": None,
        },
    ]
    task_artifact = {
        "task_id": "wf1",
        "action": "workflow",
        "description": "d",
        "task_run_id": "run-w1",
    }
    turn_msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "RunWorkflow", "args": {}, "id": "tc-w"}],
        ),
        ToolMessage(
            content="dispatched",
            tool_call_id="tc-w",
            name="RunWorkflow",
            id="tm-1",
            additional_kwargs={"task_artifact": task_artifact},
        ),
    ]
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]),
        task_history=TaskHistory(
            messages=[],
            new_ui_records=[
                {
                    "type": "ui",
                    "id": "workflow-run-run-w1",
                    "name": "workflow_run",
                    "props": {"task_id": "wf1", "frames": frames},
                }
            ],
        ),
    )
    monkeypatch.setattr(
        "src.server.services.history.replay.run_lane.sr_db.list_runs_for_thread",
        AsyncMock(side_effect=RuntimeError("ledger down")),
    )

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )

    # Frames still replay from the snapshot; the build just must not cache.
    terminal = [
        i
        for i in items
        if i["event"] == "workflow_lifecycle"
        and i["data"]["phase"] == "run_completed"
    ]
    assert len(terminal) == 1
    assert cached == []


async def test_stream_probe_precedes_task_namespace_reads(monkeypatch):
    """The cache gate trusts prepare()'s PRE-read seal verdict: sealing is
    monotonic, so "sealed before the read" proves the read saw final state.
    A post-read probe would let a seal landing in between freeze pre-terminal
    state for the cache TTL. Pin the ordering (fixed regression)."""
    from src.server.services.history.replay import run_lane as task_lane_module

    order: list[str] = []
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "run one",
    }
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "Task", "args": {}, "id": "tc-1"}],
        ),
        ToolMessage(
            content="dispatched",
            tool_call_id="tc-1",
            name="Task",
            id="tm-1",
            additional_kwargs={"task_artifact": task_artifact},
        ),
    ]
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, msgs)]),
        task_messages=[HumanMessage(content="run one", id="sub-h-1")],
    )
    inner = reader.aget_task_history

    async def read_spy(*args, **kwargs):
        order.append("read")
        return await inner(*args, **kwargs)

    reader.aget_task_history = read_spy

    async def probe_spy(thread_id, task_ids):
        order.append("probe")
        assert set(task_ids) == {"tsk1"}
        return dict.fromkeys(task_ids, "sealed")

    monkeypatch.setattr(
        task_lane_module.task_streams, "task_stream_states", probe_spy
    )

    await build_checkpoint_replay_items(thread_rows([_query(0)], {0: _response(0)}))

    assert order[0] == "probe"
    assert "read" in order
