"""A stopped or failed turn replays what it committed, then its stop close or error.

Output that streamed from a step the checkpoint never committed is not in
the model's context, so replay leaves it out."""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from src.server.services.history.replay import (
    build_checkpoint_replay_items,
    build_sse_replay_items,
)
from tests.unit.server.services.history.replay_builders import (
    thread_rows,
    THREAD,
    ThreadHistory,
    _cache_probe,
    _mock_reader,
    _query,
    _response,
    _slice_store,
    _turn,
)

pytestmark = pytest.mark.asyncio


_MAIN_AGENT = "model:t1"


def _main_chunk(message_id, content_type, content, **extra):
    return {
        "event": "message_chunk",
        "data": {
            "agent": _MAIN_AGENT,
            "id": message_id,
            "role": "assistant",
            "content_type": content_type,
            "content": content,
            **extra,
        },
    }


def _stop_close(message_id):
    """The close finalize_stopped_events writes for the message open at stop."""
    return {
        "event": "message_chunk",
        "data": {
            "agent": _MAIN_AGENT,
            "id": message_id,
            "role": "assistant",
            "finish_reason": "stopped",
        },
    }


def _user_stopped(turn_index, sse_events=None):
    """A turn's response row as the finalize of a user's Stop leaves it."""
    return _response(turn_index, sse_events, status="cancelled") | {
        "metadata": {"cancelled_by_user": True}
    }


def _main_step(message_id, text, tool_call_id, result):
    return [
        _main_chunk(message_id, "text", text),
        {
            "event": "tool_calls",
            "data": {
                "agent": _MAIN_AGENT,
                "id": message_id,
                "role": "assistant",
                "tool_calls": [{"name": "bash", "args": {}, "id": tool_call_id}],
            },
        },
        {
            "event": "tool_call_result",
            "data": {
                "agent": "tools:t1",
                "role": "assistant",
                "tool_call_id": tool_call_id,
                "content": result,
            },
        },
    ]


def _partial_answer(message_id="lc-1"):
    """Rows of a model call cut off mid-answer, as the live stream wrote them."""
    return [
        _main_chunk(message_id, "reasoning_signal", "start"),
        _main_chunk(message_id, "reasoning", "Reading the filing"),
        _main_chunk(message_id, "reasoning_signal", "complete", elapsed_ms=900),
        _main_chunk(message_id, "text", "The answer"),
        _main_chunk(message_id, "text", " is"),
    ]


def _replayed(items, turn_index=0):
    """One turn's rows after its user_message, reduced to what renders."""
    rows = []
    for item in items:
        data = item["data"]
        if data.get("turn_index") != turn_index or item["event"] == "user_message":
            continue
        if item["event"] == "message_chunk":
            rows.append((data.get("id"), data.get("content") or data.get("finish_reason")))
        elif item["event"] == "tool_calls":
            rows.append(("tool_calls", data["tool_calls"][0]["id"]))
        elif item["event"] == "tool_call_result":
            rows.append(("tool_call_result", data["tool_call_id"]))
        else:
            rows.append((item["event"], data.get("error")))
    return rows


def _turn_close(turn_index):
    """The close for a stopped turn with no committed message to name."""
    return {
        "thread_id": THREAD,
        "role": "assistant",
        "finish_reason": "stopped",
        "turn_index": turn_index,
        "response_id": f"resp-{turn_index}",
    }


async def test_stopped_flash_turn_drops_its_uncommitted_partial(monkeypatch):
    """A stop inside the only model call commits nothing past the input, so
    the checkpoint slice is the HumanMessage alone. The partial and its
    archived close name a message the checkpoint never held: the turn
    replays its question and a close on the turn."""
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, [])]))
    stored = [*_partial_answer(), _stop_close("lc-1")]

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0, stored)}),
    )

    assert [i["event"] for i in items] == ["user_message", "message_chunk"]
    assert items[1]["data"] == _turn_close(0)


async def test_stopped_ptc_turn_replays_committed_steps_once(monkeypatch):
    """Steps that finished before the stop are committed and project from the
    checkpoint; their stored copies only anchor. The in-flight answer exists
    only in the archive and is dropped; the close lands on the last
    committed message."""
    turn_msgs = [
        AIMessage(
            content="Pulling the filings.",
            id="ai-1",
            tool_calls=[{"name": "bash", "args": {}, "id": "tc-1"}],
        ),
        ToolMessage(content="filings", tool_call_id="tc-1", name="bash", id="tm-1"),
        AIMessage(
            content="Now the margins.",
            id="ai-2",
            tool_calls=[{"name": "bash", "args": {}, "id": "tc-2"}],
        ),
        ToolMessage(content="margins", tool_call_id="tc-2", name="bash", id="tm-2"),
    ]
    _mock_reader(
        monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    )
    stored = [
        *_main_step("lc-1", "Pulling the filings.", "tc-1", "filings"),
        *_main_step("lc-2", "Now the margins.", "tc-2", "margins"),
        _main_chunk("lc-3", "text", "The revenue"),
        _main_chunk("lc-3", "text", " grew"),
        _stop_close("lc-3"),
    ]

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0, stored)}),
    )

    assert _replayed(items) == [
        ("ai-1", "Pulling the filings."),
        ("tool_calls", "tc-1"),
        ("tool_call_result", "tc-1"),
        ("ai-2", "Now the margins."),
        ("tool_calls", "tc-2"),
        ("tool_call_result", "tc-2"),
        ("ai-2", "stopped"),
    ]


async def test_stopped_turn_replays_a_parallel_round_once(monkeypatch):
    """The checkpoint holds a parallel round as one tool_calls row, the live
    archive as one row per call. Every call's row anchors on the projected
    round, so none of them replays a second time."""
    turn_msgs = [
        AIMessage(
            content="Pulling both filings.",
            id="ai-1",
            tool_calls=[
                {"name": "bash", "args": {}, "id": "tc-1"},
                {"name": "bash", "args": {}, "id": "tc-2"},
            ],
        ),
        ToolMessage(content="10-K", tool_call_id="tc-1", name="bash", id="tm-1"),
        ToolMessage(content="10-Q", tool_call_id="tc-2", name="bash", id="tm-2"),
    ]
    _mock_reader(
        monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    )
    text, calls_1, result_1 = _main_step("lc-1", "Pulling both filings.", "tc-1", "10-K")
    _, calls_2, result_2 = _main_step("lc-1", "", "tc-2", "10-Q")
    stored = [text, calls_1, calls_2, result_1, result_2, _main_chunk("lc-2", "text", "Revenue")]

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0, stored, status="cancelled")}),
    )

    assert [
        [tc["id"] for tc in i["data"]["tool_calls"]]
        for i in items
        if i["event"] == "tool_calls"
    ] == [["tc-1", "tc-2"]]
    assert _replayed(items) == [
        ("ai-1", "Pulling both filings."),
        ("tool_calls", "tc-1"),
        ("tool_call_result", "tc-1"),
        ("tool_call_result", "tc-2"),
    ]


async def test_stopped_tool_round_drops_the_uncommitted_result(monkeypatch):
    """A stop while parallel tools run commits the call but none of the
    results. One that finished before the stop streamed live, but the model
    never sees it, so replay leaves it out."""
    turn_msgs = [
        AIMessage(
            content="Checking both.",
            id="ai-1",
            tool_calls=[
                {"name": "bash", "args": {}, "id": "tc-1"},
                {"name": "bash", "args": {}, "id": "tc-2"},
            ],
        ),
    ]
    _mock_reader(
        monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)])
    )
    stored = _main_step("lc-1", "Checking both.", "tc-1", "first done")
    stored[1]["data"]["tool_calls"].append({"name": "bash", "args": {}, "id": "tc-2"})

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0, stored)}),
    )

    assert _replayed(items) == [
        ("ai-1", "Checking both."),
        ("tool_calls", "tc-1"),
        ("ai-1", "stopped"),
    ]


def _stopped_during_search(stored_close):
    """A Flash turn stopped while its web search ran: the model's message,
    text and tool call, committed; the search never returned. A provider that
    streamed no finish leaves the message open, so the finalize closes it."""
    stored = [
        _main_chunk("lc-1", "text", "I'll grab current yields."),
        {
            "event": "tool_calls",
            "data": {
                "agent": _MAIN_AGENT,
                "id": "lc-1",
                "role": "assistant",
                "tool_calls": [{"name": "web_search", "args": {}, "id": "tc-1"}],
            },
        },
    ]
    return stored + [_stop_close("lc-1")] if stored_close else stored


def _search_turn():
    return _turn(
        0,
        [
            AIMessage(
                content="I'll grab current yields.",
                id="ai-1",
                tool_calls=[{"name": "web_search", "args": {}, "id": "tc-1"}],
            )
        ],
    )


@pytest.mark.parametrize(
    "stored_close", [False, True], ids=["no-archived-close", "archived-close"]
)
async def test_stop_between_messages_closes_the_committed_message(
    monkeypatch, stored_close
):
    """Stopped is recorded on the turn, not on whichever message was open. A
    stop during a tool leaves the committed message without a close: none was
    written, or the one written names the live id and anchors nothing."""
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_search_turn()]))

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0, _stopped_during_search(stored_close))}),
    )

    assert _replayed(items) == [
        ("ai-1", "I'll grab current yields."),
        ("tool_calls", "tc-1"),
        ("ai-1", "stopped"),
    ]
    assert items[-1]["data"] == {
        "thread_id": THREAD,
        "agent": "main",
        "id": "ai-1",
        "role": "assistant",
        "finish_reason": "stopped",
        "turn_index": 0,
        "response_id": "resp-0",
    }


@pytest.mark.parametrize(
    "metadata",
    [{"cancelled_by_user": False}, {}],
    ids=["system-cancel", "unflagged"],
)
async def test_cancel_the_user_did_not_ask_for_gets_no_stop_close(
    monkeypatch, metadata
):
    """A shutdown cancel is not a Stop: the turn replays without the chip."""
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_search_turn()]))
    response = _response(0, _stopped_during_search(False), status="cancelled")

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: response | {"metadata": metadata}}),
    )

    assert _replayed(items) == [
        ("ai-1", "I'll grab current yields."),
        ("tool_calls", "tc-1"),
    ]


async def test_stored_replay_closes_a_user_stopped_turn_once():
    """The sse source reads the same row: it adds the close the archive lacks
    and leaves an archived one alone."""
    between = build_sse_replay_items(
        THREAD, [_query(0)], {0: _user_stopped(0, _stopped_during_search(False))}
    )
    mid_text = build_sse_replay_items(
        THREAD,
        [_query(0)],
        {0: _user_stopped(0, [*_partial_answer(), _stop_close("lc-1")])},
    )

    assert _replayed(between)[-1] == ("lc-1", "stopped")
    assert between[-1]["data"]["agent"] == _MAIN_AGENT
    assert [r for r in _replayed(mid_text) if r[1] == "stopped"] == [
        ("lc-1", "stopped")
    ]


@pytest.mark.parametrize("status", ["completed", "interrupted"])
async def test_settled_main_lane_keeps_its_phantom_partial_dropped(
    monkeypatch, status
):
    """A turn that ended on a committed boundary replays the checkpoint alone.
    Its archive can still hold a partial from a model attempt an in-run retry
    replaced; replaying it would double-render the answer."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[_turn(0, [AIMessage(content="The answer is 42.", id="ai-1")])],
        ),
    )
    stored = [
        _main_chunk("lc-phantom", "text", "The ans"),
        _main_chunk("lc-1", "text", "The answer is 42."),
    ]

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0, stored, status=status)}),
    )

    assert _replayed(items) == [("ai-1", "The answer is 42.")]


async def test_a_dropped_partial_leaves_later_stored_rows_in_place(monkeypatch):
    """Rows that streamed after a partial no checkpoint kept still replay
    where they streamed, not at the end of the turn."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[
                _turn(
                    0,
                    [
                        AIMessage(
                            content="Searching",
                            id="ai-1",
                            tool_calls=[{"name": "bash", "args": {}, "id": "call-1"}],
                        ),
                        ToolMessage(content="out", tool_call_id="call-1"),
                    ],
                )
            ],
        ),
    )
    step = _main_step("lc-1", "Searching", "call-1", "out")
    stored = [
        *step[:2],
        _main_chunk("lc-phantom", "text", "Sear"),
        {"event": "context_window", "data": {"agent": _MAIN_AGENT, "action": "noop"}},
        step[2],
    ]

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0, stored)}),
    )

    assert _replayed(items) == [
        ("ai-1", "Searching"),
        ("tool_calls", "call-1"),
        ("context_window", None),
        ("tool_call_result", "call-1"),
    ]


async def test_errored_turn_replays_the_error_without_its_partial(monkeypatch):
    """A model call that raises commits nothing. What streamed before it
    raised is dropped; the terminal error replays. No stop close exists
    here."""
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, [])]))
    response = _response(0, _partial_answer(), status="error")
    response["errors"] = ["provider exploded"]

    items = await build_checkpoint_replay_items(thread_rows([_query(0)], {0: response}))

    assert _replayed(items) == [("error", "provider exploded")]


async def test_stopped_turn_without_a_boundary_replays_as_a_stub(monkeypatch):
    """A stopped turn whose finalize could not advance the commit pointer has
    no slice and replays as a stub: its question and a close on the turn."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[_turn(0, [AIMessage(content="a0", id="ai-0")], turn_index=0)],
        ),
    )
    stored = [*_partial_answer(), _stop_close("lc-1")]

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0), _query(1)], {0: _response(0), 1: _user_stopped(1, stored)}),
    )

    turn1 = [i for i in items if i["data"].get("turn_index") == 1]
    assert [i["event"] for i in turn1] == ["user_message", "message_chunk"]
    assert turn1[1]["data"] == _turn_close(1)


async def test_stop_before_the_first_assistant_event_closes_the_turn(monkeypatch):
    """A stop during bring-up archives no assistant event, so there is no
    message to close. The turn still replays stopped on the checkpoint and
    stored paths: its close names no message and lands on the turn."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[_turn(0, [AIMessage(content="a0", id="ai-0")], turn_index=0)],
        ),
    )
    queries = [_query(0), _query(1)]
    responses = {0: _response(0), 1: _user_stopped(1)}

    checkpoint = await build_checkpoint_replay_items(thread_rows(queries, responses))
    stored = build_sse_replay_items(THREAD, queries, responses)

    for items in (checkpoint, stored):
        turn1 = [i for i in items if i["data"].get("turn_index") == 1]
        assert [i["event"] for i in turn1] == ["user_message", "message_chunk"]
        assert turn1[1]["data"] == _turn_close(1)


async def test_stopped_turn_caches_with_its_close(monkeypatch):
    """A stopped turn owes no later write, so it caches as it replays."""
    stored_tails = _cache_probe(monkeypatch)
    turn = _turn(0, [])
    turn.anchor.tail_checkpoint_id = "tail-0"
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    stored = [*_partial_answer(), _stop_close("lc-1")]

    await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0, stored)}),
    )

    from src.server.services.history.replay import lines

    assert stored_tails == ["tail-0"]
    row = _slice_store(monkeypatch).turns["resp-0"]
    segment = [line.item() for line in lines.decode(row["lines"])]
    assert _replayed(segment) == [(None, "stopped")]
