"""How to answer comments is a row stated once while in view.

A send of comments records what it needs beside its message, on a turn's
input as on a steer's delivery (``artifact_comments``); the row before
the call that reads it says only what no row in view says yet, and a summary
that takes the row from view has the next send say it again.
"""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.comment_guidance import (
    BASE,
    CHART,
    CHART_DRAWING,
    COMPUTER,
    PICTURES,
    READING,
    CommentGuidanceMiddleware,
)
from ptc_agent.agent.middleware.compaction.summary_request import _summarizable
from ptc_agent.agent.middleware.runtime_context import (
    NON_CHANGE_ROW_KINDS,
    RUNTIME_UPDATE_KEY,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import COMMENT_GUIDANCE_ROW_KIND


def _send(reading: str = "chat", *parts: str, text: str = "<file-comments>…</file-comments>") -> HumanMessage:
    return HumanMessage(content=text, additional_kwargs={"artifact_comments": _shown(reading, *parts)})


def _steer(*shown: dict | None) -> HumanMessage:
    entries = [
        {"content": "<file-comments>…</file-comments>", **({"artifact_comments": s} if s else {})}
        for s in shown
    ]
    return HumanMessage(
        content="steer",
        additional_kwargs={"lc_source": "steering", "steering_delivered": {"messages": entries}},
    )


def _shown(reading: str, *parts: str) -> dict:
    return {"note": "", "reading": reading, "comments": [], "guidance": list(parts)}


async def _row(*messages, drawing: bool = True, cutoff_index: int | None = None) -> HumanMessage | None:
    state: dict = {"messages": list(messages)}
    if cutoff_index is not None:
        state["_summarization_event"] = {"cutoff_index": cutoff_index}
    written = await CommentGuidanceMiddleware(drawing=drawing).abefore_model(state)
    if written is None:
        return None
    (row,) = written["messages"]
    return row


def _text(row: HumanMessage) -> str:
    return runtime_update_from_message(row).text


@pytest.mark.asyncio
async def test_a_first_send_states_every_part_it_needs_in_order():
    row = await _row(_send("file", "chart", "pictures", "computer"))
    assert row.content == "\n".join(
        [BASE, READING["file"], COMPUTER, PICTURES, CHART + CHART_DRAWING]
    )
    meta = row.additional_kwargs[RUNTIME_UPDATE_KEY]
    assert meta["kind"] == COMMENT_GUIDANCE_ROW_KIND
    assert meta["provenance"] == {
        "source": "harness",
        "reading": "file",
        "chart": "draw",
        "parts": ["base", "reading", "computer", "pictures", "chart"],
    }
    assert row.id is None


@pytest.mark.asyncio
async def test_the_row_text_is_byte_stable_for_equal_sends():
    first = await _row(_send("chat", "pictures", "chart"))
    again = await _row(_send("chat", "chart", "pictures"))
    assert first.content == again.content


@pytest.mark.asyncio
async def test_a_plain_send_states_the_base_and_reading_only():
    row = await _row(_send("chat"))
    assert _text(row) == f"{BASE}\n{READING['chat']}"


@pytest.mark.asyncio
async def test_a_part_in_view_is_not_said_again():
    first = await _row(_send("chat", "pictures"))
    history = [_send("chat", "pictures"), first, AIMessage(content="done")]
    assert await _row(*history, _send("chat", "pictures")) is None
    # A part not said yet is said alone.
    row = await _row(*history, _send("chat", "pictures", "computer"))
    assert _text(row) == COMPUTER


@pytest.mark.asyncio
async def test_a_call_after_the_row_writes_nothing():
    """The row was written for this send already: a retry or the next call of
    the turn finds it in view."""
    send = _send("file", "chart")
    first = await _row(send)
    assert await _row(send, first) is None
    tool_turn = [
        send,
        first,
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "Read", "args": {}}]),
        ToolMessage(content="ok", tool_call_id="c1"),
    ]
    assert await _row(*tool_turn) is None


@pytest.mark.asyncio
async def test_a_summary_that_takes_the_row_has_the_next_send_restate_it():
    first = await _row(_send("chat"))
    history = [_send("chat"), first, AIMessage(content="done")]
    # The summary keeps only the answer: the row is out of view.
    row = await _row(*history, _send("chat"), cutoff_index=2)
    assert _text(row) == f"{BASE}\n{READING['chat']}"


@pytest.mark.asyncio
async def test_a_changed_reading_restates_only_the_reading():
    first = await _row(_send("chat", "chart"))
    history = [_send("chat", "chart"), first, AIMessage(content="done")]
    row = await _row(*history, _send("file", "chart"))
    assert _text(row) == READING["file"]
    assert runtime_update_from_message(row).provenance == {
        "source": "harness",
        "reading": "file",
        "parts": ["reading"],
    }
    # Back to chat: the last reading in view is file, so chat is said again.
    later = [*history, _send("file", "chart"), row, AIMessage(content="ok")]
    assert _text(await _row(*later, _send("chat"))) == READING["chat"]


@pytest.mark.asyncio
async def test_the_chart_part_follows_the_skill():
    plain = await _row(_send("chat", "chart"), drawing=False)
    assert plain.content.endswith(CHART)
    assert CHART_DRAWING not in plain.content
    history = [_send("chat", "chart"), plain, AIMessage(content="done")]
    # Off again: nothing new.
    assert await _row(*history, _send("chat", "chart"), drawing=False) is None
    # Switched on: the chart part is a different state, so it is said again.
    drawn = await _row(*history, _send("chat", "chart"), drawing=True)
    assert _text(drawn) == CHART + CHART_DRAWING


@pytest.mark.asyncio
async def test_a_steer_s_comments_get_the_row_mid_turn():
    turn = [
        HumanMessage(content="hi"),
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "Read", "args": {}}]),
        ToolMessage(content="ok", tool_call_id="c1"),
        _steer(None, _shown("file", "pictures")),
    ]
    row = await _row(*turn)
    assert _text(row) == "\n".join([BASE, READING["file"], PICTURES])


@pytest.mark.asyncio
async def test_only_sends_of_comments_not_yet_answered_need_a_row():
    assert await _row(HumanMessage(content="hi")) is None
    # One an answer followed is past.
    assert await _row(_send("file"), AIMessage(content="done")) is None
    # A send that records no parts needs the base and the reading.
    bare = _shown("chat")
    del bare["guidance"]
    assert _text(await _row(_steer(bare))) == f"{BASE}\n{READING['chat']}"


def test_the_row_is_no_change_and_no_summary_carries_it():
    assert COMMENT_GUIDANCE_ROW_KIND in NON_CHANGE_ROW_KINDS


@pytest.mark.asyncio
async def test_the_summarizer_is_not_handed_the_row():
    row = await _row(_send("chat"))
    assert _summarizable([row]) == []


def test_claude_code_gets_the_whole_guidance_with_every_send():
    row = CommentGuidanceMiddleware(drawing=False).inline(_send("chat", "chart"))
    assert row.content == "\n".join([BASE, READING["chat"], CHART])
    assert row.id is not None
    assert CommentGuidanceMiddleware(drawing=True).inline(HumanMessage(content="hi")) is None


def _resume() -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"id": "r1", "name": "Task", "args": {"action": "resume"}}],
        additional_kwargs={"lc_source": "steering"},
    )


@pytest.mark.asyncio
async def test_a_call_the_harness_made_is_no_answer():
    """A steering resume jumps past the model call that would read the steer
    it came with, and a plan close-out's stand-in reads nothing either: the
    send stays unread, so the next call still gets the row."""
    from ptc_agent.agent.middleware.plan_close_out import CLOSE_OUT_KEY

    resumed = [_steer(_shown("file", "pictures")), _resume(), ToolMessage(content="resumed", tool_call_id="r1")]
    assert _text(await _row(*resumed)) == "\n".join([BASE, READING["file"], PICTURES])
    closed = AIMessage(content="", additional_kwargs={CLOSE_OUT_KEY: True})
    assert _text(await _row(_send("chat"), closed)) == f"{BASE}\n{READING['chat']}"


@pytest.mark.asyncio
async def test_a_summary_inside_the_turn_has_its_sends_guidance_said_again():
    """The comments may still owe answers once a summary takes the send and
    its row, and no summary carries a row of this kind."""
    send = _send("file", "computer")
    first = await _row(send)
    working = [
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "Read", "args": {}}]),
        ToolMessage(content="ok", tool_call_id="c1"),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "Read", "args": {}}]),
        ToolMessage(content="ok", tool_call_id="c2"),
    ]
    turn = [send, first, *working]
    row = await _row(*turn, cutoff_index=3)
    assert _text(row) == "\n".join([BASE, READING["file"], COMPUTER])
    # Said again once: the restated row is in view on the next call.
    assert await _row(*turn, row, cutoff_index=3) is None
    # A summary that only took an earlier turn's send says nothing new.
    later = [send, first, AIMessage(content="done"), HumanMessage(content="next"), *working]
    assert await _row(*later, cutoff_index=2) is None
