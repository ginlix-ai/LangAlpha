"""A turn replay cannot project replays as its stub: its user message, the
stream its row archived when it kept one, and the close its row records. A
turn that answered must still read as one replay lost, not as an answer of
nothing, and only the turns that failed may lose it.
"""

from __future__ import annotations

import logging

import pytest
from langchain_core.messages import AIMessage

from src.server.database.replay_facts import ARCHIVE_KEY
from src.server.services.history.replay import build_replay_page, read_replay_page
from src.server.services.history.replay import cold
from src.server.services.history.replay.stubs import stub_lines
from tests.unit.server.services.history.replay_builders import (
    THREAD,
    TIP,
    ThreadHistory,
    _mock_reader,
    _query,
    _response,
    _turn,
    thread_rows,
)

pytestmark = pytest.mark.asyncio


def _archived(turn_index, events, status="completed"):
    """A row settled while streams were archived, which kept its own."""
    return {
        **_response(turn_index, status=status),
        "replay_facts": {ARCHIVE_KEY: events},
    }


async def _stub(response, open_run=None):
    lines, lost = await stub_lines(
        THREAD,
        [0],
        {0: [_query(0)]},
        {0: response},
        {0: open_run} if open_run else None,
    )
    return [line.item() for line in lines[0]], lost


def _events(lines):
    return [(line.item()["data"].get("turn_index"), line.event) for line in lines]


def _unavailable(items):
    return [
        i
        for i in items
        if i["event"] == "error" and i["data"].get("type") == "replay_unavailable"
    ]


async def test_a_settled_turn_with_nothing_archived_replays_as_unavailable():
    items, lost = await _stub(_response(0))

    assert [i["event"] for i in items] == ["user_message", "error"]
    error = items[1]["data"]
    assert error["type"] == "replay_unavailable"
    # The client words an internal fault as this service's own.
    assert error["error_kind"] == "internal"
    assert (error["turn_index"], error["response_id"]) == (0, "resp-0")
    assert lost == [0]


async def test_a_failed_turn_keeps_its_own_error_alone():
    # One failure per turn: the row's error is the one the user saw.
    items, lost = await _stub(_response(0, status="error"))

    errors = [i for i in items if i["event"] == "error"]
    assert len(errors) == 1
    assert errors[0]["data"]["type"] == "workflow_error"
    assert lost == []


@pytest.mark.parametrize(
    "stop",
    [
        {"status": "cancelled", "metadata": {"cancelled_by_user": True}},
        {"status": "cancelled"},
    ],
    ids=["user stop", "system cancel"],
)
async def test_a_turn_ended_before_its_answer_is_not_unavailable(stop):
    # Stopped before its first checkpoint, it may have streamed nothing to
    # lose, and its row closes it as ended.
    items, lost = await _stub({**_response(0), **stop})
    assert not _unavailable(items)
    assert lost == []


async def test_a_turn_paused_on_an_interrupt_is_unavailable():
    # Its interrupt came from a tool call the model made: it answered.
    items, lost = await _stub(_response(0, status="interrupted"))
    assert len(_unavailable(items)) == 1
    assert lost == [0]


async def test_a_turn_still_going_is_not_unavailable():
    # Its stub attaches to the live run; a retry's earlier row is not its end.
    items, lost = await _stub(_response(0), open_run="run-2")
    assert not _unavailable(items)
    assert lost == []


async def test_an_archived_turn_replays_its_stream():
    archive = [
        {"event": "message_chunk", "data": {"content": "archived", "content_type": "text"}},
        {"event": "message_chunk"},
        "not an event",
    ]
    response = _archived(0, archive)

    items, lost = await _stub(response)

    assert [i["event"] for i in items] == ["user_message", "message_chunk"]
    chunk = items[1]["data"]
    assert chunk["content"] == "archived"
    assert (chunk["turn_index"], chunk["response_id"]) == (0, "resp-0")
    assert lost == []
    # The row is read again on the next replay, so it is never stamped.
    assert "turn_index" not in response["replay_facts"][ARCHIVE_KEY][0]["data"]


async def test_an_archive_written_empty_is_a_whole_replay():
    # A turn stopped before it streamed archived an empty list: nothing is lost.
    items, lost = await _stub(_archived(0, []))
    assert [i["event"] for i in items] == ["user_message"]
    assert lost == []


async def test_an_archived_failure_closes_on_its_own_frame():
    # The frame the user saw, with the row's kind over its own: no second one.
    archive = [
        {"event": "message_chunk", "data": {"content": "partial", "content_type": "text"}},
        {"event": "error", "data": {"error": "provider fell over", "error_kind": "upstream"}},
    ]
    response = {
        **_archived(0, archive, status="error"),
        "errors": ["diagnostic"],
        "metadata": {"error_kind": "internal"},
    }

    items, _ = await _stub(response)

    assert [i["event"] for i in items] == ["user_message", "message_chunk", "error"]
    error = items[-1]["data"]
    assert error["error_kind"] == "internal"
    assert error["error"] == ""


async def test_a_turn_still_going_ignores_an_earlier_attempts_archive():
    archive = [
        {"event": "message_chunk", "data": {"content": "attempt 1", "content_type": "text"}},
    ]
    items, _ = await _stub(_archived(0, archive), open_run="run-2")
    assert [i["event"] for i in items] == ["user_message"]


def _two_turns_second_fails(monkeypatch):
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

    def flaky(inputs, turn_index, turn, response, lane):
        if turn_index == 1:
            raise ValueError("a turn this build cannot project")
        return real(inputs, turn_index, turn, response, lane)

    monkeypatch.setattr(cold, "project_turn", flaky)
    return [_query(0), _query(1, content="next")], {0: _response(0), 1: _response(1)}


async def test_a_turn_that_cannot_project_costs_only_itself(monkeypatch, caplog):
    queries, responses = _two_turns_second_fails(monkeypatch)

    with caplog.at_level(logging.WARNING):
        page = await build_replay_page(
            thread_rows(queries, responses), TIP, turn_fallback=True
        )

    assert _events(page.lines) == [
        (0, "user_message"),
        (0, "message_chunk"),
        (1, "user_message"),
        (1, "error"),
    ]
    assert page.lines[-1].item()["data"]["type"] == "replay_unavailable"
    (record,) = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert "without their answer" in record.getMessage()
    assert isinstance(record.exc_info[1], ValueError)


async def test_a_page_that_cannot_project_logs_the_turns_it_loses(
    monkeypatch, caplog
):
    # No checkpoint turns to pair: the whole page replays as stubs.
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[]))
    archived = _archived(
        0,
        [{"event": "message_chunk", "data": {"content": "archived", "content_type": "text"}}],
    )

    with caplog.at_level(logging.INFO):
        page, source = await read_replay_page(
            thread_rows([_query(0), _query(1)], {0: archived, 1: _response(1)})
        )

    assert source == "stub"
    assert _events(page.lines) == [
        (0, "user_message"),
        (0, "message_chunk"),
        (1, "user_message"),
        (1, "error"),
    ]
    (record,) = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert "[1]" in record.getMessage()
    # The failure that sent the read to stubs rides along.
    assert record.exc_info is not None and record.exc_info[1] is not None


async def test_a_turn_that_cannot_project_while_it_runs_again_is_not_unavailable(
    monkeypatch,
):
    # Its settled attempt fails to project while a newer one streams: the
    # turn's stub attaches to that run, which has nothing lost yet.
    queries, responses = _two_turns_second_fails(monkeypatch)

    page = await build_replay_page(
        thread_rows(queries, responses, open_runs={1: "run-2"}),
        TIP,
        turn_fallback=True,
    )

    assert not _unavailable([line.item() for line in page.lines])
