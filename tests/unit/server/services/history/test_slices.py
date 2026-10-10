"""Which messages a turn added: the diff between a turn's start and end state."""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.server.services.history.slices import _new_messages


def _state(*messages):
    return {"messages": list(messages)}


def _human(content, id=None):
    message = HumanMessage(content=content)
    message.id = id
    return message


def test_an_idless_message_is_new_only_once():
    """A message saved without an id loads id-less from some checkpoints and
    with an id from others; either way it is the same message, not new in
    every turn that follows it."""
    legacy = [_human("ask"), _human("continue")]
    asked = _human("continue", id="h-new")

    assert _new_messages(_state(*legacy), _state(*legacy, asked)) == [asked]
    # The same messages with ids at the end, and without them at the start.
    ided = [_human("ask", id="a"), _human("continue", id="c")]
    assert _new_messages(_state(*legacy), _state(*ided, asked)) == [asked]
    assert _new_messages(_state(*ided), _state(*legacy, asked)) == [asked]


def test_a_repeat_after_compaction_is_new():
    """Compaction drops the first "continue"; the user's next one is new."""
    first = _human("continue", id="c1")
    again = _human("continue", id="c2")

    assert _new_messages(_state(first), _state(again)) == [again]


def test_a_repeat_of_a_compacted_legacy_message_is_new():
    """Compaction removed the id-less legacy messages, and the user's next
    input repeats one of them under a fresh id: it is the turn's input, not
    the old message reloaded, because carried messages precede the input."""
    legacy = [_human("ask"), _human("continue")]
    summary = HumanMessage(
        content="summary of the work so far",
        id="s",
        additional_kwargs={"lc_source": "summarization"},
    )
    again = _human("continue", id="c2")
    answer = AIMessage(content="carrying on", id="ai-1")

    assert _new_messages(_state(*legacy), _state(summary, again, answer)) == [
        summary,
        again,
        answer,
    ]


def test_a_resume_has_no_input_to_cut_at():
    """A HITL resume adds no message of its own, so its carried legacy
    messages still match by content wherever they sit."""
    legacy = [_human("ask"), AIMessage(content="which one?")]
    ided = [_human("ask", id="a"), AIMessage(content="which one?", id="q")]
    answer = AIMessage(content="done", id="ai-2")

    assert _new_messages(
        _state(*legacy), _state(*ided, answer), has_input=False
    ) == [answer]


def test_a_resume_that_trimmed_the_window_keeps_its_new_messages():
    """An orchestrator re-entry passes ``before_agent``, so the window can
    trim inside a resume span, where no input bounds the content match. The
    id-less legacy messages it dropped are not in the end state, so a new
    message that repeats one of them is new; a kept one the trim stamped a
    fresh id on is still the same message."""
    call = {"name": "bash", "args": {"c": "ls"}, "id": "c1"}
    dropped = [
        _human("continue"),
        AIMessage(content="", tool_calls=[call]),
        ToolMessage(content="ok", tool_call_id="c1"),
    ]
    for message in dropped:
        message.id = None
    kept = [_human("run two"), AIMessage(content="answer two", id="a2")]
    stamped = [_human("run two", id="h2-stamped"), kept[1]]
    again = {"name": "bash", "args": {"c": "pwd"}, "id": "c3"}
    new = [
        AIMessage(content="", id="a3", tool_calls=[again]),
        ToolMessage(content="ok", tool_call_id="c3", id="t3"),
        _human("continue", id="h-notice"),
        AIMessage(content="final", id="a4"),
    ]

    got = _new_messages(
        _state(*dropped, *kept), _state(*stamped, *new), has_input=False
    )
    assert [m.id for m in got] == [m.id for m in new]
