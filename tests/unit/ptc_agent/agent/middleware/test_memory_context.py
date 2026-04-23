"""Unit tests for ``MemoryContextMiddleware``.

Confirms the middleware appends both tiers' ``memory.md`` as a new content block
on the system message, and that the behavior degrades gracefully when the
files don't exist yet.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from langchain_core.messages import SystemMessage
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.middleware.memory_context import MemoryContextMiddleware


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _value(content: str) -> dict:
    now = _now()
    return {
        "content": content,
        "encoding": "utf-8",
        "created_at": now,
        "modified_at": now,
    }


class _FakeRequest:
    """Minimal stand-in for ``ModelRequest`` exposing what the middleware uses."""

    def __init__(self, system_message: SystemMessage | None = None) -> None:
        self.system_message = system_message
        self.overrode_with: SystemMessage | None = None

    def override(self, *, system_message: SystemMessage) -> "_FakeRequest":
        clone = _FakeRequest(system_message=system_message)
        clone.overrode_with = system_message
        self.overrode_with = system_message
        return clone


async def _capture_handler(request: _FakeRequest) -> _FakeRequest:
    """Echo handler — just returns whatever was passed in."""
    return request


@pytest.fixture
def store():
    return InMemoryStore()


@pytest.fixture
def middleware(store):
    return MemoryContextMiddleware(
        store=store,
        user_namespace_factory=lambda: ("user_abc", "memory"),
        workspace_namespace_factory=lambda: (
            "user_abc",
            "workspaces",
            "ws_42",
            "memory",
        ),
    )


class TestInjection:
    @pytest.mark.asyncio
    async def test_both_memory_md_injected_when_present(self, store, middleware):
        await store.aput(
            ("user_abc", "memory"), "memory.md", _value("USER INDEX BODY")
        )
        await store.aput(
            ("user_abc", "workspaces", "ws_42", "memory"),
            "memory.md",
            _value("WORKSPACE INDEX BODY"),
        )

        request = _FakeRequest(system_message=SystemMessage(content="base system"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        blocks = result.system_message.content_blocks
        # One appended block containing both memory files
        assert len(blocks) >= 2
        appended = blocks[-1]["text"]
        assert "USER INDEX BODY" in appended
        assert "WORKSPACE INDEX BODY" in appended
        assert ".agents/user/memory/memory.md" in appended
        assert ".agents/workspace/memory/memory.md" in appended

    @pytest.mark.asyncio
    async def test_missing_memory_md_shows_not_created_hint(self, store, middleware):
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = result.system_message.content_blocks[-1]["text"]
        assert "No .agents/user/memory/memory.md exists yet" in appended
        assert "No .agents/workspace/memory/memory.md exists yet" in appended

    @pytest.mark.asyncio
    async def test_only_user_memory_present_still_includes_workspace_hint(
        self, store, middleware
    ):
        await store.aput(
            ("user_abc", "memory"), "memory.md", _value("USER ONLY")
        )
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = result.system_message.content_blocks[-1]["text"]
        assert "USER ONLY" in appended
        assert "No .agents/workspace/memory/memory.md" in appended

    @pytest.mark.asyncio
    async def test_long_content_is_truncated(self, store, middleware):
        huge = "x" * 20_000
        await store.aput(("user_abc", "memory"), "memory.md", _value(huge))
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = result.system_message.content_blocks[-1]["text"]
        assert "[... truncated ...]" in appended

    @pytest.mark.asyncio
    async def test_truncation_snaps_to_newline_boundary(self, store, middleware):
        """Truncation should cut at the last newline before the cap so we never
        hand the model a half-line or half-code-fence."""
        from ptc_agent.agent.middleware.memory_context import MAX_MEMORY_BLOCK_SIZE

        # Build content where a newline sits just inside the cap and content
        # continues past it. The truncated block should end at that newline.
        filler = "a" * (MAX_MEMORY_BLOCK_SIZE - 50)
        tail = "b" * 200
        content = f"{filler}\nLINE_AT_BOUNDARY\n{tail}"
        await store.aput(("user_abc", "memory"), "memory.md", _value(content))

        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = result.system_message.content_blocks[-1]["text"]

        # The `LINE_AT_BOUNDARY` marker lies within the cap and MUST be intact
        # (not split) in the emitted block.
        assert "LINE_AT_BOUNDARY" in appended
        assert "[... truncated ...]" in appended
        # The `b` tail past the cap must not appear.
        assert tail not in appended

    @pytest.mark.asyncio
    async def test_wraps_content_in_memory_tags(self, store, middleware):
        await store.aput(
            ("user_abc", "memory"), "memory.md", _value("hello")
        )
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = result.system_message.content_blocks[-1]["text"]
        assert '<memory path=".agents/user/memory/memory.md">' in appended
        assert "</memory>" in appended


class TestFactoryFailure:
    @pytest.mark.asyncio
    async def test_namespace_factory_error_returns_not_created(self, store):
        def bad_factory() -> tuple[str, ...]:
            raise RuntimeError("no identity")

        middleware = MemoryContextMiddleware(
            store=store,
            user_namespace_factory=bad_factory,
            workspace_namespace_factory=lambda: ("x", "memory"),
        )
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = result.system_message.content_blocks[-1]["text"]
        # User block shows "not created" hint; workspace block also missing
        assert "No .agents/user/memory/memory.md" in appended
