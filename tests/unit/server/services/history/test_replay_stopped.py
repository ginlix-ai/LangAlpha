"""A stopped or failed turn replays what it committed, then its stop close or error."""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from src.server.database.replay_facts import LEGACY_KEY
from src.server.services.history.replay import legacy, stub_page
from tests.unit.server.services.history.replay_builders import (
    THREAD,
    ThreadHistory,
    _cache_probe,
    _mock_reader,
    _query,
    _response,
    _slice_store,
    _turn,
    build_checkpoint_replay_items,
    thread_rows,
)

pytestmark = pytest.mark.asyncio


def _user_stopped(turn_index):
    """A turn's response row as the finalize of a user's Stop leaves it."""
    return _response(turn_index, status="cancelled") | {
        "metadata": {"cancelled_by_user": True}
    }


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


async def test_stopped_turn_with_nothing_committed_closes_the_turn(monkeypatch):
    """A stop inside the only model call commits nothing past the input, so
    the checkpoint slice is the HumanMessage alone: the turn replays its
    question and a close on the turn, naming no message."""
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, [])]))

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0)}),
    )

    assert [i["event"] for i in items] == ["user_message", "message_chunk"]
    assert items[1]["data"] == _turn_close(0)


async def test_stopped_turn_closes_its_last_committed_step(monkeypatch):
    """Steps that finished before the stop are committed and project from the
    checkpoint. The answer in flight never committed, so the close lands on
    the last committed message, after its results."""
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

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0)}),
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


def _search_turn():
    """A turn stopped while its web search ran: the model's message,
    text and tool call, committed; the search never returned."""
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


async def test_stop_between_messages_closes_the_committed_message(monkeypatch):
    """Stopped is recorded on the turn, not on whichever message was open. A
    stop during a tool leaves the committed message without a close, so the
    replay adds one."""
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_search_turn()]))

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0)}),
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
    response = _response(0, status="cancelled")

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: response | {"metadata": metadata}}),
    )

    assert _replayed(items) == [
        ("ai-1", "I'll grab current yields."),
        ("tool_calls", "tc-1"),
    ]


async def test_errored_turn_replays_its_error(monkeypatch):
    """A model call that raises commits nothing, so the turn replays the
    terminal error its response row records. No stop close exists here."""
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, [])]))
    response = _response(0, status="error")
    response["errors"] = ["provider exploded"]

    items = await build_checkpoint_replay_items(thread_rows([_query(0)], {0: response}))

    assert _replayed(items) == [("error", "provider exploded")]


_PROCESSING = "An error occurred during processing"


def _failed(errors, metadata, *, kept=None, archived=()):
    """A failed turn's response row as finalize leaves it: the error frame
    its run kept, or, for a legacy turn, the frames its archive held."""
    facts = {}
    if kept is not None:
        facts["error_frame"] = kept
    if archived:
        signals = [{"event": "error", "data": frame} for frame in archived]
        facts[LEGACY_KEY] = {"v": legacy.FACTS_VERSION, "merge": {"signals": signals}}
    return _response(0, status="error") | {
        "errors": errors,
        "metadata": metadata,
        "replay_facts": facts or None,
    }


def _kept_by(source, frame):
    """``_failed``'s arguments for a frame the run kept or a legacy turn's
    archive held."""
    return {"kept": frame} if source == "kept" else {"archived": [frame]}


async def _replayed_errors(monkeypatch, response):
    """The error items one failed turn replays, projected from its checkpoint
    and as the stub a turn checkpoints cannot cover gets, without the
    envelope replay adds."""
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, [])]))
    projected = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: response})
    )
    stub_rows = thread_rows([_query(0)], {0: response})
    stub = [line.item() for line in (await stub_page(stub_rows)).lines]
    envelope = ("thread_id", "turn_index", "response_id")
    return [
        [
            {k: v for k, v in item["data"].items() if k not in envelope}
            for item in items
            if item["event"] == "error"
        ]
        for items in (projected, stub)
    ]


@pytest.mark.parametrize("source", ["kept", "archived"])
async def test_a_provider_failure_replays_its_live_frame_once(monkeypatch, source):
    """The frame the run sent carries what the user saw (the model, the
    models tried, the hints); the row adds the status finalize read off the
    trace. One item, so neither the web (which keeps the last) nor an older
    client (the first) loses the card."""
    frame = {
        "error": "Error code: 429 - rate limited",
        "message": _PROCESSING,
        "error_kind": "upstream",
        "provider_module": "openai",
        "hints": ["provider_status", "try_another_model"],
        "model": "glm-5.3",
        "attempted_models": [
            {"model": "glm-5.3", "error": "Error code: 429", "status_code": 429, "attempts": 2}
        ],
    }
    response = _failed(
        ["Error code: 429 - rate limited"],
        {"error_kind": "upstream", "error_status_code": 429},
        **_kept_by(source, frame),
    )

    for errors in await _replayed_errors(monkeypatch, response):
        assert errors == [frame | {"status_code": 429}]


@pytest.mark.parametrize("source", ["kept", "archived"])
@pytest.mark.parametrize(
    "metadata", [{"error_kind": "internal"}, {}], ids=["stamped", "before-kinds"]
)
async def test_our_own_fault_replays_its_frame_without_the_diagnostic(
    monkeypatch, metadata, source
):
    """A fault of this service's own replays once, by its kind and with no
    text, as the row's own item would: the text is a diagnostic naming the
    run and its stream. A row from before kinds were stamped leaves the
    frame's own kind to say so. The row's facts keep the text."""
    diagnostic = "transport_lost: run r1 stream s1"
    frame = {"error": diagnostic, "message": _PROCESSING, "error_kind": "internal"}
    response = _failed([diagnostic], metadata, **_kept_by(source, frame))

    for errors in await _replayed_errors(monkeypatch, response):
        assert errors == [{"error": "", "message": _PROCESSING, "error_kind": "internal"}]
    assert frame["error"] == diagnostic


@pytest.mark.parametrize("source", ["kept", "archived"])
async def test_the_time_limit_replays_its_own_words(monkeypatch, source):
    """The time limit's frame is worded for the user; the row holds the
    exception's text, which must not replace it."""
    frame = {
        "error": "Turn timed out after 722 seconds",
        "type": "timeout_error",
        "elapsed_seconds": 722,
        "timeout_seconds": 720,
    }
    response = _failed(
        ["Workflow exceeded timeout of 720 seconds"], {}, **_kept_by(source, frame)
    )

    for errors in await _replayed_errors(monkeypatch, response):
        assert errors == [frame]


async def test_a_failure_that_sent_no_frame_replays_the_rows(monkeypatch):
    """A run whose stream quota ran out sent no frame, so the row's item is
    the turn's only error. A subagent's failure frame a legacy turn archived
    is its card's: it neither stands in for the turn's nor takes the row's
    kind, and a stub, which shows no card, leaves it out."""
    subagent = {"agent": "task:tsk1", "error": "model call failed"}
    response = _failed(
        ["transport_lost: stream quota exceeded for run r1"],
        {"error_kind": "internal"},
        archived=[dict(subagent)],
    )
    row = {"error": "", "type": "workflow_error", "error_kind": "internal"}

    projected, stub = await _replayed_errors(monkeypatch, response)
    assert projected == [subagent, row]
    assert stub == [row]


async def test_stopped_turn_without_a_boundary_closes_the_turn(monkeypatch):
    """A stopped turn whose finalize could not advance the commit pointer (or
    a stop during bring-up) has no slice and replays as a stub, and so does
    every turn of a thread checkpoints cannot cover. Either way there is no
    message to close: the close names none and lands on the turn."""
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
    stubs = [line.item() for line in (await stub_page(thread_rows(queries, responses))).lines]

    for items in (checkpoint, stubs):
        turn1 = [i for i in items if i["data"].get("turn_index") == 1]
        assert [i["event"] for i in turn1] == ["user_message", "message_chunk"]
        assert turn1[1]["data"] == _turn_close(1)


async def test_stopped_turn_caches_with_its_close(monkeypatch):
    """A stopped turn owes no later write, so it caches as it replays."""
    stored_tails = _cache_probe(monkeypatch)
    turn = _turn(0, [])
    turn.anchor.tail_checkpoint_id = "tail-0"
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))

    await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _user_stopped(0)}),
    )

    from src.server.services.history.replay import lines

    assert stored_tails == ["tail-0"]
    row = _slice_store(monkeypatch).turns["resp-0"]
    segment = [line.item() for line in lines.decode(row["lines"])]
    assert _replayed(segment) == [(None, "stopped")]
