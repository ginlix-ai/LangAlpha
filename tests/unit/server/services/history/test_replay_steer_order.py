"""Replay draws a turn's steers where the live view did.

A steer sent before its turn started is read ahead of the turn's input (the
middleware places it there), while the input itself comes from the query
table, not the checkpoint. Without the hoist, replay put the table-sourced
input first and the steer after it, reordering what the user saw live.
"""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage, HumanMessage

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


def _steer(content: str, id_: str) -> HumanMessage:
    delivered = {
        "count": 1,
        "messages": [{"content": content, "user_id": "u-1", "timestamp": 1.0}],
        "timestamp": 2.0,
    }
    return HumanMessage(
        content=f"[Steering from User]\n{content}",
        id=id_,
        additional_kwargs={"lc_source": "steering", "steering_delivered": delivered},
    )


async def _replay(monkeypatch, messages, query="what about AAPL?"):
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, messages)]))
    items = await build_checkpoint_replay_items(
        thread_rows([_query(0, content=query)], {0: _response(0)}),
    )
    return [
        (i["event"], i["data"]["messages"][0]["content"])
        if i["event"] == "steering_delivered"
        else (i["event"], None)
        for i in items
    ]


async def test_a_steer_read_ahead_of_the_input_replays_above_it(monkeypatch):
    events = await _replay(
        monkeypatch,
        [
            _steer("also cover bonds", "h-steer"),
            HumanMessage(content="what about AAPL?", id="h-in"),
            AIMessage(content="AAPL, and bonds", id="ai-1"),
        ],
    )

    assert events == [
        ("steering_delivered", "also cover bonds"),
        ("user_message", None),
        ("message_chunk", None),
    ]


async def test_a_mid_turn_steer_stays_after_the_input(monkeypatch):
    events = await _replay(
        monkeypatch,
        [
            _steer("older one", "h-early"),
            HumanMessage(content="what about AAPL?", id="h-in"),
            AIMessage(content="starting", id="ai-1"),
            _steer("also cover bonds", "h-late"),
            AIMessage(content="done, with bonds", id="ai-2"),
        ],
    )

    assert events == [
        ("steering_delivered", "older one"),
        ("user_message", None),
        ("message_chunk", None),
        ("steering_delivered", "also cover bonds"),
        ("message_chunk", None),
    ]


async def test_a_turn_whose_input_is_the_steer_hoists_nothing(monkeypatch):
    """The executor's turn for a missed steer carries it as a plain input;
    a steer read later in that turn is still mid-turn."""
    events = await _replay(
        monkeypatch,
        [
            HumanMessage(content="also cover bonds", id="h-in"),
            AIMessage(content="bonds", id="ai-1"),
            _steer("and gold", "h-late"),
            AIMessage(content="and gold", id="ai-2"),
        ],
        query="also cover bonds",
    )

    assert events == [
        ("user_message", None),
        ("message_chunk", None),
        ("steering_delivered", "and gold"),
        ("message_chunk", None),
    ]
