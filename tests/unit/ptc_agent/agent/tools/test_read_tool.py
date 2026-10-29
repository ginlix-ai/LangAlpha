"""Tests for the Read tool's size guard.

The Read tool returned full files when invoked with default args, blowing past
the context window when an agent read a multi-megabyte file. These tests pin
down the line-cap (always honored), the character-cap (catches long-line
files that defeat the line-cap), and the well-known passthrough paths
(URLs, images, missing files).
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.backends.read_window import DEFAULT_READ_LINES, MAX_READ_CHARS
from ptc_agent.agent.tools.file_ops import create_filesystem_tools
from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError


def _make_backend(*, content: str | None, supports_validate: bool = True) -> Any:
    """Build a minimal backend stub the Read tool will accept.

    Only the surface the Read tool uses is mocked: `normalize_path`,
    `validate_path`, `filesystem_config`, and `aread_range`.
    """
    backend = SimpleNamespace()
    backend.normalize_path = lambda p: p
    backend.validate_path = lambda p: supports_validate
    backend.filesystem_config = SimpleNamespace(enable_path_validation=False)
    backend.aread_range = AsyncMock(return_value=content)
    return backend


def _get_read_tool(backend: Any):
    read, _write, _edit = create_filesystem_tools(backend)
    return read


class TestReadToolDefaultLimits:
    """Default args must honor the line cap — this is the regression for the bug
    where `Read(file_path)` (no offset/limit) bypassed `aread_range` and returned
    the entire file via `aread_text`."""

    @pytest.mark.asyncio
    async def test_default_args_route_through_aread_range(self):
        backend = _make_backend(content="alpha\nbeta\ngamma\n")
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/x.txt"})

        backend.aread_range.assert_awaited_once_with(
            "/tmp/x.txt", 0, DEFAULT_READ_LINES
        )
        # Line-number prefix uses width 6 + tab; assert the first line shows up.
        assert "alpha" in result
        assert "     1" in result

    @pytest.mark.asyncio
    async def test_explicit_offset_and_limit_pass_through(self):
        backend = _make_backend(content="line11\nline12\n")
        read = _get_read_tool(backend)

        await read.ainvoke({"file_path": "/tmp/x.txt", "offset": 10, "limit": 2})

        backend.aread_range.assert_awaited_once_with("/tmp/x.txt", 10, 2)


class TestReadToolLineCap:
    """The backend returns at most ``limit`` lines, so a full window can't tell
    the whole file from its first page; the agent is told how to go on."""

    @pytest.mark.asyncio
    async def test_a_full_window_says_where_to_continue(self):
        backend = _make_backend(content="a\nb\nc\n")
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/x.txt", "offset": 10, "limit": 3})

        assert result.endswith("Read(file_path='/tmp/x.txt', offset=13) for more.]")

    @pytest.mark.asyncio
    async def test_a_short_window_is_the_end_of_the_file(self):
        backend = _make_backend(content="a\nb\n")
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/x.txt", "limit": 3})

        assert "for more" not in result

    @pytest.mark.asyncio
    async def test_an_offset_past_the_end_says_so(self):
        backend = _make_backend(content="")
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/x.txt", "offset": 500})

        assert "no lines from offset 500" in result


class TestReadToolCharCap:
    """The char cap fires after line trimming and after cat-n formatting; the
    line cap alone cannot save us from a file whose lines are megabytes long."""

    @pytest.mark.asyncio
    async def test_long_single_line_takes_single_line_overflow_path(self):
        # One line that on its own exceeds the char budget. Recovery hint must
        # name this as the single-line overflow case, not the multi-line one.
        huge_line = "x" * (MAX_READ_CHARS * 2)
        backend = _make_backend(content=huge_line)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/huge.md"})

        assert len(result) <= MAX_READ_CHARS
        assert "Read truncated" in result
        assert "exceeds the" in result and "context budget" in result
        assert "/tmp/huge.md" in result

    @pytest.mark.asyncio
    async def test_long_path_does_not_overflow_max_read_chars(self):
        # The single-line overflow marker embeds file_path three times
        # (narrative + head -c hint + sed -n hint). At 250 chars (close to
        # the POSIX 255-char filesystem cap), the marker alone is ~960
        # chars. If marker_budget is too small the result blows past
        # MAX_READ_CHARS, the cap stops being a cap.
        long_path = "/home/daytona/work/" + ("subdir/" * 32) + "result.md"
        assert len(long_path) >= 250, f"path needs to actually be long, got {len(long_path)}"
        huge_line = "x" * (MAX_READ_CHARS * 2)
        backend = _make_backend(content=huge_line)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": long_path})

        assert len(result) <= MAX_READ_CHARS, (
            f"result length {len(result)} exceeds budget {MAX_READ_CHARS} "
            f"for path of {len(long_path)} chars"
        )
        assert long_path in result

    @pytest.mark.asyncio
    async def test_single_line_overflow_reports_first_line_size_only(self):
        # The single-line overflow size hint feeds the agent's bash slice
        # plan. If we report `len(formatted)` it counts trailing lines that
        # fit after the huge first line and inflates the slice target. The
        # reported size should be the offending line itself.
        first_line = "x" * (MAX_READ_CHARS * 2)
        trailing = "y" * 1000  # a small second line that happens to fit
        backend = _make_backend(content=first_line + "\n" + trailing)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/mixed.md"})

        # Extract the reported "~N characters" figure.
        match = re.search(r"is ~(\d+) characters", result)
        assert match, f"single-line marker missing: {result[-400:]}"
        reported = int(match.group(1))
        # Allow a few chars of wiggle (off-by-one with cat-n prefix) but
        # must NOT include the trailing 1000-char line.
        assert reported <= len(first_line) + 10, (
            f"reported={reported} overstates first-line size {len(first_line)}"
        )
        assert reported < len(first_line) + 1000, (
            "reported size includes trailing lines — should not"
        )

    @pytest.mark.asyncio
    async def test_multi_line_truncation_reports_accurate_next_offset(self):
        # 500 lines × 1000 chars each + cat-n prefix = ~500KB formatted. The
        # char cap will fire and clip to a newline boundary. The recovery hint
        # must point at the FIRST line the agent did not see, never past it,
        # otherwise pagination silently skips content.
        lines = [("x" * 1000) for _ in range(500)]
        backend = _make_backend(content="\n".join(lines))
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/big.md"})

        assert len(result) <= MAX_READ_CHARS
        assert "Read truncated" in result
        # Extract the "You saw lines 1..K" range and the next offset N.
        range_match = re.search(r"You saw lines (\d+)\.\.(\d+)", result)
        next_match = re.search(r"offset=(\d+)", result)
        assert range_match and next_match, f"recovery markers missing: {result[-400:]}"
        first_seen = int(range_match.group(1))
        last_seen = int(range_match.group(2))
        next_offset = int(next_match.group(1))

        assert first_seen == 1
        assert last_seen < 500, "must report fewer lines than the full chunk on truncation"
        # next_offset is 0-indexed; reading from it must land on the FIRST
        # unseen line, not skip any.
        assert next_offset == last_seen, (
            f"next_offset={next_offset} would skip lines {last_seen}..{next_offset - 1}"
        )

    @pytest.mark.asyncio
    async def test_non_ascii_path_uses_plain_quotes_not_repr(self):
        # `!r` renders non-ASCII paths with unicode escapes (\uXXXX), which
        # the model may copy back verbatim and get "file not found". Plain
        # single-quoting keeps the path roundtrippable.
        path = "/tmp/测试.md"
        huge = "x" * (MAX_READ_CHARS * 2)
        backend = _make_backend(content=huge)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": path})

        assert "测试" in result
        assert "\\u6d4b" not in result

    @pytest.mark.asyncio
    async def test_small_file_returns_unmodified(self):
        backend = _make_backend(content="hello\nworld\n")
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/x.txt"})

        assert "Read truncated" not in result
        assert "hello" in result and "world" in result


class TestReadToolBinaryDetection:
    """Pre-fix, binary files either returned `File not found` (when the
    backend's text decode failed) or got cat-n'ed and produced garbled
    output for the model. Now a null-byte sniff returns a clear error with
    bash escape hints, matching what file(1) and git do."""

    @pytest.mark.asyncio
    async def test_binary_content_returns_clear_error(self):
        # ELF magic header + null padding, classic shape for a .o / .so / a.out.
        binary_content = "ELF\x02\x01\x01" + ("\x00" * 100) + "more"
        backend = _make_backend(content=binary_content)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/program.o"})

        assert result.startswith("ERROR")
        assert "binary" in result.lower()
        assert "/tmp/program.o" in result
        assert "xxd" in result or "file" in result

    @pytest.mark.asyncio
    async def test_text_with_no_null_bytes_passes_through(self):
        # Sanity check: a normal text file with control chars (tab, newline)
        # but no null bytes must NOT trip the binary detector.
        text = "line1\n\tindented\nline3\twith\ttabs"
        backend = _make_backend(content=text)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/text.txt"})

        assert not result.startswith("ERROR")
        assert "line1" in result and "line3" in result


class TestReadToolPassthroughs:
    """URLs, images, and missing files must still route to their existing
    handlers — the size guard must not change these branches."""

    @pytest.mark.asyncio
    async def test_url_returns_acknowledgment(self):
        backend = _make_backend(content=None)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "https://example.com/a.pdf"})

        backend.aread_range.assert_not_awaited()
        assert "Loading document from URL" in result

    @pytest.mark.asyncio
    async def test_image_extension_returns_acknowledgment(self):
        backend = _make_backend(content=None)
        backend.filesystem_config = SimpleNamespace(enable_path_validation=False)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/pic.png"})

        backend.aread_range.assert_not_awaited()
        assert "Loading image" in result

    @pytest.mark.asyncio
    async def test_missing_file_returns_error(self):
        backend = _make_backend(content=None)
        read = _get_read_tool(backend)

        result = await read.ainvoke({"file_path": "/tmp/missing.txt"})

        assert result.startswith("ERROR: File not found")


class TestReadToolTranscripts:
    """A transcript line is one event, so a window cuts each long field and a
    single-line Read is how the agent gets the event whole."""

    SEGMENT = "/home/workspace/Home/.agents/transcripts/abcd1234/turn-0001.jsonl"
    LONG = "w" * 5_000

    def _line(self) -> str:
        import json

        return json.dumps({"role": "tool", "text": self.LONG})

    @pytest.mark.asyncio
    async def test_a_window_cuts_a_long_field_and_names_the_offset(self):
        read = _get_read_tool(_make_backend(content=self._line() + "\n"))

        result = await read.ainvoke({"file_path": self.SEGMENT, "offset": 4})

        assert self.LONG not in result
        assert "[cut: 5,000 chars" in result
        assert "Read(offset=4, limit=1)" in result

    @pytest.mark.asyncio
    async def test_one_line_comes_whole(self):
        read = _get_read_tool(_make_backend(content=self._line() + "\n"))

        result = await read.ainvoke({"file_path": self.SEGMENT, "offset": 4, "limit": 1})

        assert self.LONG in result
        assert "[cut:" not in result

    @pytest.mark.asyncio
    async def test_the_same_content_elsewhere_is_not_cut(self):
        read = _get_read_tool(_make_backend(content=self._line() + "\n"))

        result = await read.ainvoke({"file_path": "/home/workspace/Home/notes.jsonl"})

        assert self.LONG in result


class TestReadSandboxOutcome:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("raised", "outcome", "error_class"),
        [
            pytest.param(SandboxGoneError("gone"), "error", "SandboxGoneError", id="gone"),
            pytest.param(
                SandboxTransientError("unreachable"),
                "error",
                "SandboxTransientError",
                id="unreachable",
            ),
            # What else the catch-all sees is the request refused, such as
            # a read-only path.
            pytest.param(PermissionError("read-only"), "ok", None, id="refused"),
        ],
    )
    async def test_only_the_sandbox_failing_marks_the_run(
        self, traced_tool, raised, outcome, error_class
    ):
        backend = _make_backend(content=None)
        backend.aread_range = AsyncMock(side_effect=raised)

        content, metadata = await traced_tool(
            _get_read_tool(backend), {"file_path": "notes.md"}
        )

        assert content.startswith("ERROR")
        assert metadata["tool_outcome"] == outcome
        assert metadata.get("error_class") == error_class

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("file_path", "channel", "provider"),
        [
            ("notes.md", "docker", "docker"),
            ("../.agents/threads.jsonl", "fuse", "docker"),
            ("/memory/notes.md", "server", None),
        ],
        ids=["sandbox", "file-mount", "store-mount"],
    )
    async def test_a_call_names_its_channel_and_the_provider_of_a_sandbox_it_reaches(
        self, traced_tool, file_path, channel, provider
    ):
        channels = {"notes.md": "docker", "../.agents/threads.jsonl": "fuse"}
        backend = _make_backend(content="hello")
        backend.provider = "docker"
        backend.channel_for = lambda p: channels.get(p, "server")

        _content, metadata = await traced_tool(_get_read_tool(backend), {"file_path": file_path})

        assert metadata.get("tool_channel") == channel
        assert metadata.get("tool_provider") == provider
