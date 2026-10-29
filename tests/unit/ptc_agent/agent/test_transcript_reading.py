"""What Read shows of a transcript file: each long field cut, the line still JSON."""

from __future__ import annotations

import json

import pytest

from ptc_agent.agent.transcript import reading
from ptc_agent.agent.transcript.reading import FIELD_CHARS, READ_CHARS, cut_event, cut_window, is_segment

TURN = "/home/workspace/Home/.agents/transcripts/abcd1234/turn-0001.jsonl"
RUN = "/home/workspace/Home/.agents/transcripts/abcd1234/tasks/task-7/run-0001.jsonl"


class TestIsSegment:
    @pytest.mark.parametrize("path", [TURN, RUN])
    def test_a_turn_or_run_file_is_a_segment(self, path):
        assert is_segment(path)

    @pytest.mark.parametrize(
        "path",
        [
            "/home/workspace/Home/.agents/transcripts/abcd1234/manifest.json",
            "/home/workspace/Home/.agents/transcripts/abcd1234/tasks/task-7/meta.json",
            "/home/workspace/.agents/threads.jsonl",
            "/home/workspace/Home/notes/turn-0001.jsonl",
            "/home/workspace/Home/.agents/transcripts/not-hex/turn-0001.jsonl",
        ],
    )
    def test_anything_else_is_not(self, path):
        assert not is_segment(path)


class TestCutEvent:
    def test_a_short_line_is_unchanged(self):
        line = json.dumps({"role": "user", "text": "hi"})

        assert cut_event(line, 3) == line

    def test_a_long_string_field_is_cut_with_a_note_naming_its_offset(self):
        line = json.dumps({"role": "tool", "text": "x" * 5_000})

        cut = json.loads(cut_event(line, 7))

        assert cut["role"] == "tool"
        assert cut["text"].startswith("x" * FIELD_CHARS + "…[cut: 5,000 chars")
        assert "Read(offset=7, limit=1)" in cut["text"]
        assert len(cut["text"]) < FIELD_CHARS + 200

    def test_nested_fields_are_cut_and_short_ones_stay_whole(self):
        event = {
            "parts": [{"text": "y" * 3_000}, {"text": "short"}],
            "meta": {"body": "z" * 3_000, "id": "abc"},
        }

        cut = json.loads(cut_event(json.dumps(event), 0))

        assert "[cut:" in cut["parts"][0]["text"]
        assert cut["parts"][1] == {"text": "short"}
        assert "[cut:" in cut["meta"]["body"]
        assert cut["meta"]["id"] == "abc"

    def test_a_long_line_with_no_long_field_is_returned_as_stored(self):
        line = json.dumps({"items": ["a" * 100] * 30}, indent=None, separators=(",", ":"))
        assert len(line) > FIELD_CHARS

        assert cut_event(line, 0) == line

    def test_a_long_line_that_is_not_json_is_cut_plainly(self):
        line = "q" * 4_000

        cut = cut_event(line, 2)

        assert cut.startswith("q" * FIELD_CHARS + "…[cut: 4,000 chars")
        assert "Read(offset=2, limit=1)" in cut


class TestCutWindow:
    @pytest.mark.asyncio
    async def test_lines_past_what_read_shows_are_never_parsed(self, monkeypatch):
        parsed = []
        monkeypatch.setattr(reading, "cut_event", lambda line, n: parsed.append(n) or line)
        line = "x" * (READ_CHARS // 4)

        cut = await cut_window([line] * 2000, 10)

        # The fifth line takes the window past the cap, where Read clips it.
        assert parsed == [10, 11, 12, 13, 14]
        assert cut == [line] * 5
