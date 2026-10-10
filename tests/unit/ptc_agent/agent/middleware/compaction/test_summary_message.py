"""Round-trip tests for ``build_summary_message`` / ``parse_summary_message``.

``parse_summary_message`` recovers the raw summary text a checkpoint stores so
the projector can re-emit the ``context_window`` event on replay. It slices by
the stamped ``summary_length`` rather than string-splitting on the note, so a
summary that itself contains the note text survives the round-trip.
"""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.compaction.types import CONTEXT_SUMMARY_PREFIX
from ptc_agent.agent.middleware.compaction.utils import (
    _LEGACY_FILE_NOTE,
    build_summary_message,
    parse_summary_message,
)
from ptc_agent.agent.transcript import TranscriptTarget, Window
from ptc_agent.agent.transcript.classify import LEGACY_SUMMARY_PREFIX
from ptc_agent.agent.transcript.pointer import (
    SummarySpan,
    TranscriptTurns,
    summary_span,
    transcript_note,
)
from ptc_agent.agent.transcript.render import message_turns

TRANSCRIPT = TranscriptTarget("abcd1234-0000-0000-0000-000000000000")


def test_round_trip_with_transcript():
    summary = "We analyzed three tickers and charted the spread."
    span = SummarySpan(4, 9, (1, 3))
    msg = build_summary_message(summary, TRANSCRIPT, span=span)
    assert parse_summary_message(msg) == summary
    assert msg.content.endswith(transcript_note(TRANSCRIPT, span))


def test_round_trip_without_transcript():
    summary = "Quick factual answer, no sandbox to hold a transcript."
    msg = build_summary_message(summary, None)
    assert parse_summary_message(msg) == summary
    assert msg.content == f"{CONTEXT_SUMMARY_PREFIX}{summary}"


def test_summary_containing_the_note_survives():
    summary = f"Earlier the run said: {transcript_note(TRANSCRIPT)} and moved on."
    msg = build_summary_message(summary, TRANSCRIPT)
    assert parse_summary_message(msg) == summary


def test_legacy_message_without_length_stamp_falls_back():
    # Pre-stamp checkpoints have no summarize_complete metadata → rsplit path.
    summary = "Legacy summary text."
    content = f"{LEGACY_SUMMARY_PREFIX}{summary}{_LEGACY_FILE_NOTE}work/history.md`."
    legacy = HumanMessage(content=content)  # no additional_kwargs stamp
    assert parse_summary_message(legacy) == summary


def test_stamped_summary_with_the_old_opening_still_parses():
    summary = "Written before the opening said which side wins."
    msg = build_summary_message(summary, TRANSCRIPT, span=SummarySpan(1, 2))
    msg.content = msg.content.replace(CONTEXT_SUMMARY_PREFIX, LEGACY_SUMMARY_PREFIX, 1)
    assert parse_summary_message(msg) == summary


def test_task_note_names_the_task_runs():
    task = TranscriptTarget.for_agent(
        "abcd1234-0000-0000-0000-000000000000", "task:t/1|model:x"
    )
    assert task.directory == ".agents/transcripts/abcd1234/tasks/t_1"
    note = transcript_note(task, SummarySpan(3, 5, (1, 2)))
    assert f"`{task.directory}/`" in note and "`run-NNNN.jsonl` per run" in note
    assert "covers runs 3 to 5 (`run-0003.jsonl` to `run-0005.jsonl`)" in note
    assert note.endswith(
        "Runs 1 to 2 did not fit in this summary in full; their files hold what it leaves out."
    )
    assert transcript_note(task, SummarySpan(1, 1, (1, 1))).endswith(
        "Run 1 did not fit in this summary in full; its file holds what it leaves out."
    )
    assert "covers run 1 (`run-0001.jsonl`)" in transcript_note(task, SummarySpan(1, 1))


def _turns(n):
    out = []
    for i in range(1, n + 1):
        out += [HumanMessage(content=f"q{i}", id=f"h{i}"), AIMessage(content="a", id=f"a{i}")]
    return out


def test_span_names_the_turns_the_summary_covers():
    raw = _turns(5)
    turns = message_turns(raw, base=0)
    assert summary_span(turns, raw[:8], raw[:8]) == SummarySpan(1, 4)
    assert summary_span(turns, raw[:8], raw[4:8]) == SummarySpan(3, 4, (1, 2))
    # An earlier summary heads the summarized list and is not in raw. Kept,
    # it stands in for every turn before the stretch, so a trimmed middle is
    # a gap inside the span; dropped, the turns it stood in for go with it.
    prior = build_summary_message("old summary", TRANSCRIPT)
    assert summary_span(turns, [prior, *raw[6:8]], [prior, *raw[6:8]]) == SummarySpan(1, 4)
    assert summary_span(turns, [prior, *raw[2:8]], [prior, *raw[6:8]]) == SummarySpan(1, 4, (2, 3))
    assert summary_span(turns, [prior, *raw[2:8]], raw[4:8]) == SummarySpan(3, 4, (1, 2))


def test_span_flags_a_message_cut_partway():
    # One huge message dominates: trimming keeps its tail under the same id.
    raw = _turns(3)
    turns = message_turns(raw, base=0)
    tail = raw[2].model_copy(update={"content": "q2 (tail)"})
    assert summary_span(turns, raw[:4], [tail, raw[3]]).gap == (1, 2)
    head_cut = raw[0].model_copy(update={"content": "1 (tail)"})
    assert summary_span(turns, raw[:4], [head_cut, *raw[1:4]]).gap == (1, 1)


def test_a_second_summary_keeps_the_turns_the_first_left_out():
    # The first summary was trimmed to turns 2 to 3 and left turn 1 out. Kept
    # whole by the next compaction, it reaches the summarizer without its
    # note, so the new pointer has to say so again.
    from ptc_agent.agent.middleware.compaction.compact import build_summary_event
    from ptc_agent.agent.middleware.compaction.summarize import Summary

    raw = _turns(4)
    first = build_summary_message("first", TRANSCRIPT, span=SummarySpan(2, 3, (1, 1)))
    stretch = [first, raw[6]]
    event = build_summary_event(
        Summary("second", "model", stretch),
        TranscriptTurns.of(TRANSCRIPT, raw, window=Window()),
        raw_messages=raw,
        to_summarize=stretch,
        preserved=raw[7:],
        original_count=len(raw),
    )
    message = event["summary_message"]
    assert "covers turns 2 to 4" in message.content
    assert "Turn 1 did not fit in this summary in full" in message.content


def test_event_hands_back_skills_whose_body_was_summarized():
    from ptc_agent.agent.middleware.compaction.compact import build_summary_event
    from ptc_agent.agent.middleware.compaction.summarize import Summary

    raw = _turns(3)
    raw[0] = HumanMessage(
        content='go\n<loaded-skill name="dcf-model" mid="h1">steps</loaded-skill>', id="h1"
    )
    for skill_files, how in (
        (True, "Read its `.agents/skills/<name>/SKILL.md` again."),
        (False, "call LoadSkill with its name."),
    ):
        event = build_summary_event(
            Summary("the summary", "model", raw[:4]),
            TranscriptTurns.of(TRANSCRIPT, raw, window=Window()),
            raw_messages=raw,
            to_summarize=raw[:4],
            preserved=raw[4:],
            original_count=len(raw),
            skill_files=skill_files,
        )
        message = event["summary_message"]
        assert parse_summary_message(message) == "the summary"
        assert "covers turns 1 to 2" in message.content
        assert message.content.endswith(how)
        assert "`dcf-model`" in message.content
