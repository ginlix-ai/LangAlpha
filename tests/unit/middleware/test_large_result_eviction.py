"""Tests for the LargeResultEvictionMiddleware.

Verifies content preview creation, extension detection, size-based eviction,
and excluded-tool bypass logic.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from deepagents.backends.protocol import WriteResult
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.large_result_eviction import (
    NUM_CHARS_PER_TOKEN,
    TOOLS_EXCLUDED_FROM_EVICTION,
    LargeResultEvictionMiddleware,
    _create_content_preview,
    _detect_extension,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_backend(write_error: str | None = None) -> MagicMock:
    """Create a mock backend whose awrite returns the REAL upstream WriteResult.

    Deliberately not a MagicMock result: one serves any attribute, so a stub
    kept answering a field deepagents had already deleted and every test here
    passed against a shape production could not produce.
    """
    backend = MagicMock()
    awrite_result = (
        WriteResult(error=write_error)
        if write_error
        else WriteResult(path="/evicted/file.md")
    )
    backend.awrite = AsyncMock(return_value=awrite_result)
    return backend


def _make_tool_request(tool_name: str = "CustomTool") -> MagicMock:
    """Create a mock ToolCallRequest."""
    request = MagicMock()
    request.tool_call = {"name": tool_name}
    request.runtime = MagicMock()
    return request


# ---------------------------------------------------------------------------
# Tests for pure functions
# ---------------------------------------------------------------------------


class TestCreateContentPreview:
    """Tests for _create_content_preview."""

    def test_short_content_returns_all(self):
        content = "line1\nline2\nline3"
        preview = _create_content_preview(content, head_lines=5, tail_lines=5)
        assert "line1" in preview
        assert "line2" in preview
        assert "line3" in preview
        assert "truncated" not in preview

    def test_long_content_shows_head_and_tail(self):
        lines = [f"line{i}" for i in range(100)]
        content = "\n".join(lines)
        preview = _create_content_preview(content, head_lines=3, tail_lines=3)
        assert "line0" in preview
        assert "line1" in preview
        assert "line2" in preview
        assert "line97" in preview
        assert "line99" in preview
        assert "truncated" in preview

    def test_exact_boundary(self):
        """Content with exactly head+tail lines should not truncate."""
        lines = [f"line{i}" for i in range(10)]
        content = "\n".join(lines)
        preview = _create_content_preview(content, head_lines=5, tail_lines=5)
        assert "truncated" not in preview


class TestDetectExtension:
    """Tests for _detect_extension."""

    def test_json_object(self):
        assert _detect_extension('{"key": "value"}') == ".json"

    def test_json_array(self):
        assert _detect_extension('[1, 2, 3]') == ".json"

    def test_json_with_whitespace(self):
        assert _detect_extension("  \n  {\"k\": 1}") == ".json"

    def test_markdown_content(self):
        assert _detect_extension("# Title\nSome text") == ".md"

    def test_plain_text(self):
        assert _detect_extension("Just plain text") == ".md"


# ---------------------------------------------------------------------------
# Tests for middleware eviction logic
# ---------------------------------------------------------------------------


class TestEvictionDecision:
    """Tests for _aprocess_large_message size threshold."""

    @pytest.mark.asyncio
    async def test_small_message_not_evicted(self):
        backend = _make_backend()
        mw = LargeResultEvictionMiddleware(backend=backend, tool_token_limit_before_evict=100)
        msg = ToolMessage(content="small output", tool_call_id="call_1", name="Tool")
        processed = await mw._aprocess_large_message(msg)
        assert processed.content == "small output"
        backend.awrite.assert_not_called()

    @pytest.mark.asyncio
    async def test_large_message_is_evicted(self):
        backend = _make_backend()
        limit = 10  # 10 tokens = 40 chars
        mw = LargeResultEvictionMiddleware(backend=backend, tool_token_limit_before_evict=limit)
        # Create content larger than 40 chars
        large_content = "x" * (NUM_CHARS_PER_TOKEN * limit + 100)
        msg = ToolMessage(content=large_content, tool_call_id="call_1", name="Tool")
        processed = await mw._aprocess_large_message(msg)
        assert "saved in the filesystem" in processed.content
        backend.awrite.assert_called_once()
        # Eviction paths overwrite prior writes by design.
        assert backend.awrite.call_args.kwargs.get("overwrite") is True

    @pytest.mark.asyncio
    async def test_zero_limit_disables_eviction(self):
        backend = _make_backend()
        mw = LargeResultEvictionMiddleware(backend=backend, tool_token_limit_before_evict=0)
        msg = ToolMessage(content="anything", tool_call_id="call_1", name="Tool")
        processed = await mw._aprocess_large_message(msg)
        assert processed.content == "anything"
        backend.awrite.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_failure_returns_original(self):
        backend = _make_backend(write_error="disk full")
        limit = 10
        mw = LargeResultEvictionMiddleware(backend=backend, tool_token_limit_before_evict=limit)
        large_content = "x" * (NUM_CHARS_PER_TOKEN * limit + 100)
        msg = ToolMessage(content=large_content, tool_call_id="call_1", name="Tool")
        processed = await mw._aprocess_large_message(msg)
        # Original content is returned since write failed
        assert processed.content == large_content


class TestAwrapToolCall:
    """Tests for async awrap_tool_call."""

    @pytest.mark.asyncio
    async def test_excluded_tool_bypasses_eviction_async(self):
        backend = _make_backend()
        mw = LargeResultEvictionMiddleware(backend=backend, tool_token_limit_before_evict=10)
        request = _make_tool_request("Read")
        msg = ToolMessage(content="x" * 10000, tool_call_id="call_1", name="Read")
        handler = AsyncMock(return_value=msg)
        result = await mw.awrap_tool_call(request, handler)
        assert result.content == "x" * 10000

    @pytest.mark.asyncio
    async def test_async_eviction(self):
        backend = _make_backend()
        limit = 10
        mw = LargeResultEvictionMiddleware(backend=backend, tool_token_limit_before_evict=limit)
        request = _make_tool_request("CustomTool")
        large_content = "x" * (NUM_CHARS_PER_TOKEN * limit + 100)
        msg = ToolMessage(content=large_content, tool_call_id="call_1", name="CustomTool")
        handler = AsyncMock(return_value=msg)
        result = await mw.awrap_tool_call(request, handler)
        assert isinstance(result, ToolMessage)
        assert "saved in the filesystem" in result.content

    @pytest.mark.asyncio
    async def test_glob_result_is_evicted_when_large(self):
        """Glob is no longer on the exclusion list, so a pathological result must be
        evicted to the filesystem instead of reaching the model (Glob self-caps for
        the common case; eviction is the backstop)."""
        assert "Glob" not in TOOLS_EXCLUDED_FROM_EVICTION
        backend = _make_backend()
        limit = 10
        mw = LargeResultEvictionMiddleware(backend=backend, tool_token_limit_before_evict=limit)
        request = _make_tool_request("Glob")
        large_content = "x" * (NUM_CHARS_PER_TOKEN * limit + 100)
        msg = ToolMessage(content=large_content, tool_call_id="call_1", name="Glob")
        handler = AsyncMock(return_value=msg)
        result = await mw.awrap_tool_call(request, handler)
        assert isinstance(result, ToolMessage)
        assert "saved in the filesystem" in result.content


class TestAinterceptCommandBranch:
    """Tests for `_aintercept_large_tool_result` handling pre-wrapped Commands.

    Previously exercised indirectly via the sync `wrap_tool_call` path; after the
    sync path was deleted, this Command-branch handling (tools that return a
    Command with ToolMessages inside `update['messages']`) had no explicit test.
    """

    @pytest.mark.asyncio
    async def test_large_tool_message_inside_command_is_evicted(self):
        from langgraph.types import Command
        backend = _make_backend()
        limit = 10
        mw = LargeResultEvictionMiddleware(
            backend=backend, tool_token_limit_before_evict=limit
        )
        large_content = "x" * (NUM_CHARS_PER_TOKEN * limit + 100)
        large_msg = ToolMessage(content=large_content, tool_call_id="c1", name="CustomTool")
        cmd = Command(update={"messages": [large_msg], "files": {}})
        result = await mw._aintercept_large_tool_result(cmd, runtime=MagicMock())
        assert isinstance(result, Command)
        assert result.update is not None
        # The large message inside the Command is evicted
        processed_msg = result.update["messages"][0]
        assert "saved in the filesystem" in processed_msg.content

    @pytest.mark.asyncio
    async def test_command_branch_preserves_non_tool_messages(self):
        from langchain_core.messages import AIMessage
        from langgraph.types import Command
        backend = _make_backend()
        limit = 10
        mw = LargeResultEvictionMiddleware(
            backend=backend, tool_token_limit_before_evict=limit
        )
        large_content = "x" * (NUM_CHARS_PER_TOKEN * limit + 100)
        large_msg = ToolMessage(content=large_content, tool_call_id="c1", name="X")
        other_msg = AIMessage(content="some ai response")
        cmd = Command(update={"messages": [other_msg, large_msg], "files": {}})
        result = await mw._aintercept_large_tool_result(cmd, runtime=MagicMock())
        assert isinstance(result, Command)
        # AIMessage passes through untouched
        assert result.update["messages"][0] is other_msg
        # ToolMessage is evicted
        assert "saved in the filesystem" in result.update["messages"][1].content

    @pytest.mark.asyncio
    async def test_command_branch_passes_through_when_no_update(self):
        from langgraph.types import Command
        backend = _make_backend()
        mw = LargeResultEvictionMiddleware(
            backend=backend, tool_token_limit_before_evict=10
        )
        # Command with no update dict should pass through unchanged
        cmd = Command(update=None)
        result = await mw._aintercept_large_tool_result(cmd, runtime=MagicMock())
        assert result is cmd
        backend.awrite.assert_not_called()
