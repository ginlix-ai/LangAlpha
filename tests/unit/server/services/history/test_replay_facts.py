"""Replay reads what a run's checkpoint does not keep from the run's facts.

Reasoning durations, captured subagent images and returned steering are
measured or learned around the graph, so they reach replay through the run
rows' ``replay_facts`` rather than the stored events. A row settled before
those existed keeps replaying from its stored events, and a turn holding both
must replay the same either way. Run facts land after a run settles, so a
turn's stored lines are keyed on them.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.server.services.history.replay import (
    CheckpointReplayUnavailable,
    build_checkpoint_replay_items,
    build_replay_page,
    read_replay_page,
)
from src.server.services.history.replay import cold
from src.server.services.history.replay import facts as replay_facts
from src.server.services.history.replay import legacy
from src.server.services.history.replay.stored_merge import derive_merge
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


def _merged(projected, stored, **kwargs):
    return legacy.apply(projected, derive_merge(projected, stored, **kwargs))


def _signal(message_id, content, agent="main", **extra):
    return {
        "event": "message_chunk",
        "data": {
            "thread_id": THREAD,
            "agent": agent,
            "id": message_id,
            "role": "assistant",
            "content": content,
            "content_type": "reasoning_signal",
            **extra,
        },
    }


def _text(message_id, content, agent="main"):
    return {
        "event": "message_chunk",
        "data": {
            "agent": agent,
            "id": message_id,
            "role": "assistant",
            "content": content,
            "content_type": "text",
        },
    }


def _returned(input_id, agent="task:tsk1", content="also AMD", reason="run_ended"):
    return {
        "event": "steering_returned",
        "data": {
            "agent": agent,
            "content": content,
            "input_id": input_id,
            "reason": reason,
            "thread_id": THREAD,
        },
    }


# ------------------------------------------------------------ reasoning


def test_durations_sum_onto_each_main_message_close():
    """The projector joins a message's thinking blocks into one row, so its
    one close stands for every block the stream timed."""
    items = [
        _signal("m1", "start"),
        _signal("m1", "complete"),
        _signal("m2", "start"),
        _signal("m2", "complete"),
        _signal("t1", "complete", agent="task:k"),
    ]
    replay_facts.apply_reasoning_durations(
        items, {"m1": [3000, 12000, 8000], "m2": [900], "t1": [5]}
    )
    assert items[1]["data"]["elapsed_ms"] == 23000
    assert items[3]["data"]["elapsed_ms"] == 900
    assert "elapsed_ms" not in items[0]["data"]
    # Task lanes were never timed live; a stray key does not invent one.
    assert "elapsed_ms" not in items[4]["data"]


def test_durations_ignore_untimed_and_malformed_spans():
    items = [_signal("m1", "complete"), _signal("m2", "complete")]
    replay_facts.apply_reasoning_durations(items, {"m1": [True, "9"], "m2": "900"})
    assert "elapsed_ms" not in items[0]["data"]
    assert "elapsed_ms" not in items[1]["data"]


def test_known_durations_are_not_carried_again_from_stored_events():
    """With the row's facts applied, the stored copy of the same closes is
    not summed on top."""
    projected = [_signal("ai-1", "start"), _signal("ai-1", "complete")]
    replay_facts.apply_reasoning_durations(projected, {"ai-1": [1200, 800]})
    stored = [
        _signal("lc-1", "start"),
        _signal("lc-1", "complete", elapsed_ms=1200),
        _signal("lc-1", "start"),
        _signal("lc-1", "complete", elapsed_ms=800),
    ]
    merged = _merged(projected, stored, main_durations_known=True)
    assert merged[1]["data"]["elapsed_ms"] == 2000


def test_stored_durations_still_carry_for_a_row_without_facts():
    projected = [_signal("ai-1", "start"), _signal("ai-1", "complete")]
    stored = [_signal("lc-1", "start"), _signal("lc-1", "complete", elapsed_ms=4200)]
    merged = _merged(projected, stored)
    assert merged[1]["data"]["elapsed_ms"] == 4200


# ------------------------------------------------------------ run facts


def _run_items():
    return [
        _text("a1", "first", agent="task:tsk1"),
        _text("a2", "second", agent="task:tsk1"),
    ]


def _placed(items):
    return [
        (i["event"], i["data"].get("input_id") or i["data"].get("content"))
        for i in items
    ]


def test_returns_at_run_end_follow_everything_the_run_wrote():
    out = replay_facts.apply_run_facts(
        _run_items(),
        {
            "steering_returned": [
                {"content": "x", "input_id": "in-1", "reason": "run_ended"}
            ]
        },
        agent="task:tsk1",
        thread_id=THREAD,
        message_ids=["h1", "a1", "a2"],
    )
    assert _placed(out) == [
        ("message_chunk", "first"),
        ("message_chunk", "second"),
        ("steering_returned", "in-1"),
    ]
    assert out[-1]["data"] == {
        "agent": "task:tsk1",
        "content": "x",
        "input_id": "in-1",
        "reason": "run_ended",
        "thread_id": THREAD,
    }


def test_returns_before_a_model_call_follow_the_message_the_run_had_reached():
    returned = [
        {"input_id": "r-head", "reason": "run_mismatch", "after": None},
        {"input_id": "r-a1", "reason": "run_mismatch", "after": "a1"},
        # The opener projects no item of its own: the return leads the run.
        {"input_id": "r-h1", "reason": "run_mismatch", "after": "h1"},
        # A message this run's transcript does not hold: end of the run.
        {"input_id": "r-gone", "reason": "run_mismatch", "after": "elsewhere"},
        {"input_id": "r-a1", "reason": "run_mismatch", "after": "a1"},  # repeated write
    ]
    out = replay_facts.apply_run_facts(
        _run_items(),
        {"steering_returned": returned},
        agent="task:tsk1",
        thread_id=THREAD,
        message_ids=["h1", "a1", "a2"],
    )
    assert _placed(out) == [
        ("steering_returned", "r-head"),
        ("steering_returned", "r-h1"),
        ("message_chunk", "first"),
        ("steering_returned", "r-a1"),
        ("message_chunk", "second"),
        ("steering_returned", "r-gone"),
    ]


def test_run_images_resolve_the_runs_text():
    items = [
        _text("a1", "![c](work/sub.png) and ![d](work/other.png)", agent="task:tsk1")
    ]
    replay_facts.apply_run_facts(
        items,
        {"images": {"work/sub.png": "https://cdn/sub.png"}},
        agent="task:tsk1",
        thread_id=THREAD,
        message_ids=["a1"],
    )
    assert items[0]["data"]["content"] == (
        "![c](https://cdn/sub.png) and ![d](work/other.png)"
    )


def test_a_placed_return_is_not_merged_again_from_stored_events():
    projected = [_text("a1", "first", agent="task:tsk1"), _returned("in-1")]
    stored = [
        _text("lc-a1", "first", agent="task:tsk1"),
        _returned("in-1"),
        _returned("in-2"),
    ]
    merged = _merged(projected, stored)
    # A run settled before facts keeps its stored returns; one with facts
    # shows each of its returns once.
    assert sorted(
        i["data"]["input_id"] for i in merged if i["event"] == "steering_returned"
    ) == ["in-1", "in-2"]


# ------------------------------------------------------------ through replay


def _task_turn():
    task_artifact = {
        "task_id": "tsk1",
        "action": "init",
        "description": "d",
        "prompt": "p",
        "task_run_id": "run-1",
    }
    return [
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


def _ledgered(monkeypatch, reader):
    from datetime import datetime, timezone

    reader.task_run_stamps = AsyncMock(return_value=["run-1"])
    monkeypatch.setattr(
        "src.server.services.history.replay.run_lane.sr_db.list_runs_for_thread",
        AsyncMock(
            return_value=[
                {
                    "task_run_id": "run-1",
                    "status": "completed",
                    "started_at": datetime(2026, 1, 3, tzinfo=timezone.utc),
                }
            ]
        ),
    )


def _task_reader(monkeypatch):
    turn = _turn(0, _task_turn())
    turn.anchor.tail_checkpoint_id = "tail-0"
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[turn]),
        task_messages=[
            HumanMessage(content="p", id="sub-h-1"),
            AIMessage(content="see ![c](work/sub.png)", id="sub-ai-1"),
        ],
    )
    _ledgered(monkeypatch, reader)
    return reader


_RUN_FACTS = {
    "turn_index": 0,
    "task_run_id": "run-1",
    "replay_facts": {
        "v": 1,
        "images": {"work/sub.png": "https://cdn/sub.png"},
        "steering_returned": [
            {"content": "also AMD", "input_id": "in-1", "reason": "run_ended"}
        ],
    },
}


def _task_lane(items):
    return [
        (i["event"], i["data"].get("content"))
        for i in items
        if str(i["data"].get("agent", "")).startswith("task:")
        and i["event"] in ("message_chunk", "steering_returned")
    ]


@pytest.mark.asyncio
async def test_run_facts_reach_the_projected_run(monkeypatch):
    _cache_probe(monkeypatch)
    _task_reader(monkeypatch)
    stored_copy = _response(0, [_returned("in-1")])

    items = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: stored_copy}, run_facts=[_RUN_FACTS]),
    )

    assert _task_lane(items) == [
        ("message_chunk", "see ![c](https://cdn/sub.png)"),
        ("steering_returned", "also AMD"),
    ]


@pytest.mark.asyncio
async def test_run_facts_landing_later_reproject_a_stored_turn(monkeypatch):
    """A run's facts are written after it settles, by when its turn's lines
    may already be stored."""
    stored_tails = _cache_probe(monkeypatch)
    _task_reader(monkeypatch)

    before = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}),
    )
    assert _task_lane(before) == [("message_chunk", "see ![c](work/sub.png)")]
    assert stored_tails == ["tail-0"]

    after = await build_checkpoint_replay_items(
        thread_rows([_query(0)], {0: _response(0)}, run_facts=[_RUN_FACTS]),
    )
    assert _task_lane(after) == [
        ("message_chunk", "see ![c](https://cdn/sub.png)"),
        ("steering_returned", "also AMD"),
    ]
    assert stored_tails == ["tail-0", "tail-0"]


@pytest.mark.asyncio
async def test_a_turn_with_facts_and_stored_events_replays_the_same(monkeypatch):
    """The dual-write guarantee: the facts reproduce the stored durations."""
    reasoning = AIMessage(
        content=[
            {"type": "thinking", "thinking": "weighing it"},
            {"type": "text", "text": "the answer"},
        ],
        id="msg_1",
    )
    stored = [
        _signal("lc-1", "start"),
        _signal("lc-1", "complete", elapsed_ms=1500),
        _signal("lc-1", "start"),
        _signal("lc-1", "complete", elapsed_ms=500),
        _text("lc-1", "the answer"),
    ]

    def closes(items):
        return [
            i["data"].get("elapsed_ms")
            for i in items
            if i["event"] == "message_chunk"
            and i["data"].get("content_type") == "reasoning_signal"
            and i["data"].get("content") == "complete"
        ]

    async def replay_with(response):
        _mock_reader(
            monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, [reasoning])])
        )
        return await build_checkpoint_replay_items(
            thread_rows([_query(0)], {0: response}),
        )

    legacy = await replay_with(_response(0, stored))
    with_facts = _response(0, stored)
    with_facts["replay_facts"] = {"v": 1, "reasoning_ms": {"msg_1": [1500, 500]}}
    dual = await replay_with(with_facts)
    facts_only = _response(0, [])
    facts_only["replay_facts"] = with_facts["replay_facts"]
    alone = await replay_with(facts_only)

    assert closes(legacy) == closes(dual) == closes(alone) == [2000]
    assert legacy == dual


# ------------------------------------------------------------ per-turn fallback


def _two_turns(monkeypatch):
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[
                _turn(0, [AIMessage(content="zero", id="ai-0")]),
                _turn(1, [AIMessage(content="one", id="ai-1")]),
            ],
        ),
    )
    real = cold.project_turn

    def flaky(inputs, turn_index, turn, response, lane, derive=None):
        if turn_index == 1:
            raise ValueError("a turn this build cannot project")
        return real(inputs, turn_index, turn, response, lane, derive)

    monkeypatch.setattr(cold, "project_turn", flaky)
    return (
        [_query(0), _query(1, content="next")],
        {0: _response(0), 1: _response(1, [_text("lc-1", "one, as stored")])},
    )


def _texts(lines):
    return [
        (line.item()["data"].get("turn_index"), line.item()["data"]["content"])
        for line in lines
        if line.event == "message_chunk"
    ]


@pytest.mark.asyncio
async def test_a_turn_that_cannot_project_replays_alone_from_storage(monkeypatch):
    queries, responses = _two_turns(monkeypatch)
    page = await build_replay_page(thread_rows(queries, responses), turn_fallback=True)
    assert _texts(page.lines) == [(0, "zero"), (1, "one, as stored")]
    # The stored-events copy is never stored as the turn's lines: the next
    # read projects it again.
    assert "resp-1" not in _slice_store(monkeypatch).turns or not (
        _slice_store(monkeypatch).turns["resp-1"].get("lines")
    )


@pytest.mark.asyncio
async def test_without_turn_fallback_the_failure_reaches_the_caller(monkeypatch):
    queries, responses = _two_turns(monkeypatch)
    with pytest.raises(ValueError):
        await build_replay_page(thread_rows(queries, responses))


# ------------------------------------------------------------ read_replay_page


@pytest.mark.asyncio
async def test_auto_falls_back_to_storage_when_turns_cannot_pair(monkeypatch):
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[]))
    page, source = await read_replay_page(
        thread_rows([_query(0)], {0: _response(0, [_text("lc-0", "stored")])}),
    )
    assert source == "sse"
    assert _texts(page.lines) == [(0, "stored")]


@pytest.mark.asyncio
async def test_checkpoint_source_never_falls_back(monkeypatch):
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[]))
    with pytest.raises(CheckpointReplayUnavailable):
        await read_replay_page(
            thread_rows([_query(0)], {0: _response(0)}),
            source="checkpoint",
        )


@pytest.mark.asyncio
async def test_a_thread_without_a_commit_pointer_replays_from_storage(monkeypatch):
    reader = _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[]))
    page, source = await read_replay_page(
        thread_rows(
            [_query(0), _query(1), _query(2)],
            {
                0: _response(0, [_text("lc-0", "zero")]),
                1: _response(1, [_text("lc-1", "one")]),
                2: _response(2, [_text("lc-2", "two")]),
            },
            tip=None,
        ),
        limit=2,
    )
    assert source == "sse"
    reader.thread_history.assert_not_awaited()
    assert _texts(page.lines) == [(1, "one"), (2, "two")]
    assert (page.first_turn_index, page.has_more) == (1, True)
