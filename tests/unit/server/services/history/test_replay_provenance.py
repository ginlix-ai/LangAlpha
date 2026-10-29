"""Where table-sourced provenance lands in a turn's replay.

Live, a provenance record joins whatever bubble the turn is writing, and a
delivered steer opens a new one, so replay must put each row on the same
side of every steer that it reached live.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from tests.unit.server.services.history.replay_builders import (
    THREAD,
    ThreadHistory,
    _mock_reader,
    _query,
    _response,
    _turn,
    build_checkpoint_replay_items,
    thread_rows,
)

pytestmark = pytest.mark.asyncio

_STEER_AT = 1_760_000_000.0


def _at(offset: float) -> datetime:
    return datetime.fromtimestamp(_STEER_AT + offset, tz=timezone.utc)


def _row(record_id: str, **fields):
    return {
        "conversation_response_id": "resp-0",
        "provenance_record_id": record_id,
        "tool_call_id": None,
        "source_type": "market_data",
        "identifier": "AAA",
        "detail": "market_watch",
        "result_sha256": f"sha-{record_id}",
        "agent": "main",
        "source_timestamp": None,
        "created_at": None,
        **fields,
    }


def _steer() -> HumanMessage:
    return HumanMessage(
        content="[Steering from User]\nalso cover bonds",
        id="h-steer",
        additional_kwargs={
            "lc_source": "steering",
            "steering_delivered": {
                "count": 1,
                "messages": [{"content": "also cover bonds", "timestamp": _STEER_AT}],
                "timestamp": _STEER_AT,
            },
        },
    )


async def _replay(monkeypatch, turn_msgs, rows):
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, turn_msgs)]))
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}, provenance=rows),
    )
    return items


def _labels(items):
    return [
        item["data"]["record_id"] if item["event"] == "provenance" else item["event"]
        for item in items
    ]


async def test_market_watch_rows_stay_on_their_side_of_a_steer(monkeypatch):
    # Regression: every row with no tool call tailed the turn, which moved a
    # stamp the first bubble showed live into the last one on reload.
    turn_msgs = [
        AIMessage(content="starting", id="ai-1"),
        _steer(),
        AIMessage(content="done, with bonds", id="ai-2"),
    ]
    rows = [
        _row("before", source_timestamp=_at(-5)),
        _row("after", source_timestamp=_at(5)),
        _row("untimed"),
    ]

    items = await _replay(monkeypatch, turn_msgs, rows)

    assert _labels(items) == [
        "user_message",
        "message_chunk",
        "before",
        "steering_delivered",
        "message_chunk",
        "after",
        "untimed",
    ]


async def test_market_watch_rows_tail_a_turn_without_steers(monkeypatch):
    turn_msgs = [AIMessage(content="answer", id="ai-1")]
    rows = [_row("early", source_timestamp=_at(-5)), _row("late", source_timestamp=_at(5))]

    items = await _replay(monkeypatch, turn_msgs, rows)

    assert _labels(items) == ["user_message", "message_chunk", "early", "late"]


async def test_same_result_under_two_calls_anchors_to_each(monkeypatch):
    # Rows the writer now keeps per call each follow their own call's result,
    # so both Sources entries hold the source on reload as they did live.
    turn_msgs = [
        AIMessage(
            content="",
            id="a-0",
            tool_calls=[{"name": "web_search", "args": {"q": "x"}, "id": "tc-1"}],
        ),
        ToolMessage(content="result", tool_call_id="tc-1", id="t-0"),
        AIMessage(
            content="",
            id="a-1",
            tool_calls=[{"name": "web_search", "args": {"q": "x"}, "id": "tc-2"}],
        ),
        ToolMessage(content="result", tool_call_id="tc-2", id="t-1"),
    ]
    shared = {
        "source_type": "web_page",
        "identifier": "https://example.test/a",
        "detail": None,
        "result_sha256": "same",
    }
    rows = [
        _row("first", tool_call_id="tc-1", **shared),
        _row("second", tool_call_id="tc-2", **shared),
    ]

    items = await _replay(monkeypatch, turn_msgs, rows)

    labels = _labels(items)
    results = [i for i, item in enumerate(items) if item["event"] == "tool_call_result"]
    assert [labels[i + 1] for i in results] == ["first", "second"]


async def test_subagent_rows_stay_on_their_side_of_a_steer(monkeypatch):
    # Regression: a subagent's lane replays after the whole main lane, so its
    # rows, anchored to its own tool results, all landed in the last bubble.
    turn_msgs = [
        AIMessage(content="starting", id="ai-1"),
        _steer(),
        AIMessage(content="done, with bonds", id="ai-2"),
    ]
    rows = [
        _row("sub-before", tool_call_id="sub-tc-1", agent="task:t1", source_timestamp=_at(-5)),
        _row("sub-after", tool_call_id="sub-tc-2", agent="task:t1", source_timestamp=_at(5)),
    ]

    items = await _replay(monkeypatch, turn_msgs, rows)

    assert _labels(items) == [
        "user_message",
        "message_chunk",
        "sub-before",
        "steering_delivered",
        "message_chunk",
        "sub-after",
    ]


async def test_a_subagent_row_goes_where_it_arrived_not_where_it_was_fetched(
    monkeypatch,
):
    # Code in the sandbox reports what it fetched only when it returns: a
    # fetch before the steer whose call returned after it showed live in the
    # bubble the steer opened.
    turn_msgs = [
        AIMessage(content="starting", id="ai-1"),
        _steer(),
        AIMessage(content="done, with bonds", id="ai-2"),
    ]
    rows = [
        _row(
            "fetched-before",
            tool_call_id="sub-tc-1",
            agent="task:t1",
            source_timestamp=_at(-5),
            emitted_at=_at(3),
        ),
        _row(
            "older-row",
            tool_call_id="sub-tc-2",
            agent="task:t1",
            source_timestamp=_at(-4),
        ),
    ]

    items = await _replay(monkeypatch, turn_msgs, rows)

    assert _labels(items) == [
        "user_message",
        "message_chunk",
        "older-row",
        "steering_delivered",
        "message_chunk",
        "fetched-before",
    ]
