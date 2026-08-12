"""Round-trip tests for ``build_summary_message`` / ``parse_summary_message``.

``parse_summary_message`` recovers the raw summary text a checkpoint stores so
the projector can re-emit the ``context_window`` event on replay. It slices by
the stamped ``summary_length`` rather than string-splitting on the file note, so
a summary that itself contains the note text survives the round-trip.
"""

from __future__ import annotations

from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.compaction.types import CONTEXT_SUMMARY_PREFIX
from ptc_agent.agent.middleware.compaction.utils import (
    _SUMMARY_FILE_NOTE,
    build_summary_message,
    parse_summary_message,
)

_NOTE_PHRASE = _SUMMARY_FILE_NOTE.split("{", 1)[0]


def test_round_trip_with_file_path():
    summary = "We analyzed three tickers and charted the spread."
    msg = build_summary_message(summary, file_path="work/history.md")
    assert parse_summary_message(msg) == summary


def test_round_trip_without_file_path():
    summary = "Quick factual answer, no history offloaded."
    msg = build_summary_message(summary, file_path=None)
    assert parse_summary_message(msg) == summary


def test_summary_containing_note_phrase_no_file_path():
    # The LLM summary organically contains the file-note text and no note was
    # appended (file_path is None). rsplit would truncate at the phrase; slicing
    # by the stamped length recovers it verbatim.
    summary = f"Earlier the run said: {_NOTE_PHRASE}old/run.md` — noted for context."
    msg = build_summary_message(summary, file_path=None)
    assert parse_summary_message(msg) == summary


def test_summary_containing_note_phrase_with_file_path():
    summary = f"Discussed {_NOTE_PHRASE}prior.md` at length."
    msg = build_summary_message(summary, file_path="work/history.md")
    assert parse_summary_message(msg) == summary


def test_legacy_message_without_length_stamp_falls_back():
    # Pre-stamp checkpoints have no summarize_complete metadata → rsplit path.
    summary = "Legacy summary text."
    content = f"{CONTEXT_SUMMARY_PREFIX}{summary}" + _SUMMARY_FILE_NOTE.format(
        file_path="work/history.md"
    )
    legacy = HumanMessage(content=content)  # no additional_kwargs stamp
    assert parse_summary_message(legacy) == summary
