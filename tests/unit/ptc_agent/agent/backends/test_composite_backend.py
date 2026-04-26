"""Unit tests for ``CompositeFilesystemBackend``.

Verifies prefix routing: `.agents/user/memory/**` hits the user-tier
``StoreBackend``, `.agents/workspace/memory/**` hits the workspace-tier
``StoreBackend``, and everything else falls through to the sandbox. Uses
``InMemoryStore`` for the store-backed routes and a ``MagicMock`` for the
sandbox.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends.composite import CompositeFilesystemBackend
from ptc_agent.agent.backends.langgraph_store import StoreBackend

USER_PREFIX = "/home/workspace/.agents/user/memory/"
WORKSPACE_PREFIX = "/home/workspace/.agents/workspace/memory/"
WORKING_DIR = "/home/workspace"


def _make_sandbox():
    """Minimal sandbox stand-in with the rich-method surface used here."""
    sb = MagicMock()
    sb.root_dir = WORKING_DIR

    def _normalize(p: str) -> str:
        if p.startswith("/"):
            return p
        return f"{WORKING_DIR}/{p}"

    sb.normalize_path.side_effect = _normalize
    sb.virtualize_path.side_effect = lambda p: (
        p[len(WORKING_DIR):] if p.startswith(WORKING_DIR) else p
    )
    sb.validate_path.return_value = True
    sb.filesystem_config.enable_path_validation = True
    sb.sandbox_id = "sbx-1"
    sb.id = "sbx-1"
    sb.skills_manifest = None

    # Async stubs for the rich-method surface
    sb.aread_text = AsyncMock(return_value="sandbox content")
    sb.aread_range = AsyncMock(return_value="sandbox range")
    sb.awrite_text = AsyncMock(return_value=True)
    sb.aedit_text = AsyncMock(return_value={"success": True, "occurrences": 1})
    sb.aglob_paths = AsyncMock(return_value=[f"{WORKING_DIR}/work/sandbox_match.md"])
    sb.agrep_rich = AsyncMock(return_value=[f"{WORKING_DIR}/work/sandbox_hit.md"])
    return sb


@pytest.fixture
def sandbox():
    return _make_sandbox()


@pytest.fixture
def store():
    return InMemoryStore()


@pytest.fixture
def composite(sandbox, store):
    user_backend = StoreBackend(
        store=store,
        namespace_factory=lambda: ("user_abc", "memory"),
        root_prefix=USER_PREFIX,
        sandbox_backend=sandbox,
    )
    workspace_backend = StoreBackend(
        store=store,
        namespace_factory=lambda: ("user_abc", "workspaces", "ws_42", "memory"),
        root_prefix=WORKSPACE_PREFIX,
        sandbox_backend=sandbox,
    )
    return CompositeFilesystemBackend(
        sandbox=sandbox, routes=[user_backend, workspace_backend]
    )


class TestRouting:
    @pytest.mark.asyncio
    async def test_user_memory_path_routes_to_store(self, composite, sandbox, store):
        await composite.awrite_text(USER_PREFIX + "memory.md", "user-memory")
        # Store has the item
        item = await store.aget(("user_abc", "memory"), "memory.md")
        assert item is not None
        assert item.value["content"] == "user-memory"
        # Sandbox write was never called
        sandbox.awrite_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_workspace_memory_path_routes_to_store(self, composite, store, sandbox):
        await composite.awrite_text(WORKSPACE_PREFIX + "memory.md", "workspace-memory")
        item = await store.aget(
            ("user_abc", "workspaces", "ws_42", "memory"), "memory.md"
        )
        assert item is not None
        assert item.value["content"] == "workspace-memory"
        sandbox.awrite_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sandbox_path_bypasses_store(self, composite, sandbox, store):
        await composite.awrite_text(f"{WORKING_DIR}/work/scratch.md", "just sandbox")
        sandbox.awrite_text.assert_awaited_once_with(
            f"{WORKING_DIR}/work/scratch.md", "just sandbox"
        )
        # Store untouched — list all namespaces, should be empty
        namespaces = await store.alist_namespaces()
        assert namespaces == []

    @pytest.mark.asyncio
    async def test_read_routes_correctly(self, composite, sandbox):
        # Memory path — pre-seed via write, then read
        await composite.awrite_text(USER_PREFIX + "note.md", "from memory")
        content = await composite.aread_text(USER_PREFIX + "note.md")
        assert content == "from memory"

        # Sandbox path — handler returns mock value
        content = await composite.aread_text(f"{WORKING_DIR}/work/other.md")
        assert content == "sandbox content"
        sandbox.aread_text.assert_awaited_once_with(f"{WORKING_DIR}/work/other.md")

    @pytest.mark.asyncio
    async def test_edit_routes_correctly(self, composite, sandbox):
        await composite.awrite_text(USER_PREFIX + "note.md", "hello world")
        result = await composite.aedit_text(
            USER_PREFIX + "note.md", "world", "WORLD"
        )
        assert result["success"] is True
        assert await composite.aread_text(USER_PREFIX + "note.md") == "hello WORLD"
        # Sandbox edit was never called
        sandbox.aedit_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_longest_prefix_wins(self, sandbox, store):
        """Overlapping routes: the longer prefix must be selected."""
        # Fabricate an overlapping outer backend to verify ordering
        outer = StoreBackend(
            store=store,
            namespace_factory=lambda: ("outer",),
            root_prefix="/home/workspace/.agents/",
            sandbox_backend=sandbox,
        )
        inner = StoreBackend(
            store=store,
            namespace_factory=lambda: ("inner",),
            root_prefix=USER_PREFIX,
            sandbox_backend=sandbox,
        )
        composite = CompositeFilesystemBackend(
            sandbox=sandbox, routes=[outer, inner]
        )

        await composite.awrite_text(USER_PREFIX + "file.md", "inner")
        inner_item = await store.aget(("inner",), "file.md")
        assert inner_item is not None
        outer_item = await store.aget(("outer",), "user/memory/file.md")
        assert outer_item is None


class TestPathHelpers:
    def test_normalize_path_delegates_to_sandbox(self, composite, sandbox):
        assert composite.normalize_path("foo.md") == f"{WORKING_DIR}/foo.md"
        sandbox.normalize_path.assert_called_with("foo.md")

    def test_virtualize_path_delegates_to_sandbox(self, composite):
        assert composite.virtualize_path(f"{WORKING_DIR}/foo.md") == "/foo.md"

    def test_filesystem_config_delegates_to_sandbox(self, composite):
        assert composite.filesystem_config.enable_path_validation is True

    def test_validate_path_delegates_to_sandbox(self, composite):
        assert composite.validate_path("/anywhere") is True


class TestPathTraversal:
    def test_memory_path_with_dot_dot_is_rejected(self, composite):
        """Traversal-looking writes aimed at memory must NOT silently reroute
        to the sandbox FS after normalization resolves the `..`. The composite
        refuses them at the perimeter instead."""
        with pytest.raises(ValueError, match="traversal"):
            composite.normalize_path(".agents/user/memory/../escape.md")

    def test_non_memory_dot_dot_paths_still_normalize(self, composite, sandbox):
        """Traversal only triggers rejection when aimed at a memory tier;
        regular sandbox-bound paths retain normal sandbox behavior."""
        # Does not raise — sandbox normalizer is still consulted (returns whatever).
        composite.normalize_path("work/../other.md")


class TestRelativePathRouting:
    @pytest.mark.asyncio
    async def test_relative_memory_path_routes_to_store(self, composite, store, sandbox):
        """Callers that pass a relative path must still hit the memory backend.
        Composite normalizes before routing — the sandbox write stays untouched.
        """
        await composite.awrite_text(".agents/user/memory/rel.md", "rel-body")
        item = await store.aget(("user_abc", "memory"), "rel.md")
        assert item is not None
        assert item.value["content"] == "rel-body"
        sandbox.awrite_text.assert_not_awaited()


class TestDelegation:
    def test_getattr_falls_through_to_sandbox(self, composite, sandbox):
        """Attributes not defined on the composite delegate to the sandbox so
        callers swapping raw backend -> composite don't trip AttributeError."""
        sandbox.arbitrary_method = "sentinel"
        assert composite.arbitrary_method == "sentinel"


class TestSearchFanOut:
    @pytest.mark.asyncio
    async def test_glob_at_workspace_root_includes_memory(self, composite):
        # Seed user memory
        await composite.awrite_text(USER_PREFIX + "a.md", "A")
        await composite.awrite_text(WORKSPACE_PREFIX + "b.md", "B")

        matches = await composite.aglob_paths("*.md", WORKING_DIR)
        # Sandbox-returned matches are included
        assert f"{WORKING_DIR}/work/sandbox_match.md" in matches
        # Memory-tier matches are included too
        assert USER_PREFIX + "a.md" in matches
        assert WORKSPACE_PREFIX + "b.md" in matches

    @pytest.mark.asyncio
    async def test_glob_inside_memory_tier_skips_sandbox(self, composite, sandbox):
        await composite.awrite_text(USER_PREFIX + "a.md", "A")
        matches = await composite.aglob_paths("*.md", USER_PREFIX)
        assert matches == [USER_PREFIX + "a.md"]
        sandbox.aglob_paths.assert_not_awaited()


MEMO_PREFIX = "/home/workspace/.agents/user/memo/"


@pytest.fixture
def three_route_composite(sandbox, store):
    """Composite with memory (writable) + memo (read-only) + sandbox."""
    user_memory = StoreBackend(
        store=store,
        namespace_factory=lambda: ("user_abc", "memory"),
        root_prefix=USER_PREFIX,
        sandbox_backend=sandbox,
    )
    workspace_memory = StoreBackend(
        store=store,
        namespace_factory=lambda: ("user_abc", "workspaces", "ws_42", "memory"),
        root_prefix=WORKSPACE_PREFIX,
        sandbox_backend=sandbox,
    )
    user_memo = StoreBackend(
        store=store,
        namespace_factory=lambda: ("user_abc", "memos"),
        root_prefix=MEMO_PREFIX,
        sandbox_backend=sandbox,
        read_only=True,
        read_only_error="Memo is user-managed. Ask the user to edit via the memo panel.",
    )
    return CompositeFilesystemBackend(
        sandbox=sandbox, routes=[user_memory, workspace_memory, user_memo]
    )


class TestThreeRouteBoundary:
    """Memory, memo, and sandbox coexist under `.agents/user/` — confirm no cross-wire."""

    @pytest.mark.asyncio
    async def test_write_to_memory_still_lands_in_store(
        self, three_route_composite, store, sandbox
    ):
        ok = await three_route_composite.awrite_text(USER_PREFIX + "foo.md", "memory body")
        assert ok is True
        item = await store.aget(("user_abc", "memory"), "foo.md")
        assert item is not None and item.value["content"] == "memory body"
        sandbox.awrite_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_write_to_memo_is_rejected_as_read_only(
        self, three_route_composite, store, sandbox
    ):
        from ptc_agent.agent.backends import ReadOnlyStoreError

        with pytest.raises(ReadOnlyStoreError) as excinfo:
            await three_route_composite.awrite_text(
                MEMO_PREFIX + "doc.md", "agent tried"
            )
        assert "memo panel" in str(excinfo.value).lower()
        # Nothing stored in either memory or memo namespace
        assert await store.aget(("user_abc", "memos"), "doc.md") is None
        assert await store.aget(("user_abc", "memory"), "doc.md") is None
        # Sandbox not touched either
        sandbox.awrite_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_edit_on_memo_returns_read_only_error(
        self, three_route_composite
    ):
        result = await three_route_composite.aedit_text(
            MEMO_PREFIX + "doc.md", "old", "new"
        )
        assert result["success"] is False
        assert "memo panel" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_read_on_memo_hits_memo_namespace(
        self, three_route_composite, store
    ):
        # Server-side write to seed (bypasses the read_only flag).
        await store.aput(
            ("user_abc", "memos"),
            "q1-thesis.md",
            {
                "content": "memo body",
                "encoding": "utf-8",
                "created_at": "2026-04-24T10:00:00Z",
                "modified_at": "2026-04-24T10:00:00Z",
            },
        )
        content = await three_route_composite.aread_text(MEMO_PREFIX + "q1-thesis.md")
        assert content == "memo body"

    @pytest.mark.asyncio
    async def test_write_outside_all_routes_goes_to_sandbox(
        self, three_route_composite, sandbox, store
    ):
        await three_route_composite.awrite_text(
            f"{WORKING_DIR}/work/scratch.md", "just sandbox"
        )
        sandbox.awrite_text.assert_awaited_once()
        # Neither memory nor memo namespace touched
        assert await store.aget(("user_abc", "memory"), "scratch.md") is None
        assert await store.aget(("user_abc", "memos"), "scratch.md") is None

    @pytest.mark.asyncio
    async def test_glob_at_user_prefix_sees_memory_and_memo(
        self, three_route_composite, store
    ):
        # Seed memory
        await three_route_composite.awrite_text(USER_PREFIX + "m.md", "memory entry")
        # Seed memo (direct store put bypasses read-only)
        await store.aput(
            ("user_abc", "memos"),
            "p.md",
            {
                "content": "memo entry",
                "encoding": "utf-8",
                "created_at": "2026-04-24T10:00:00Z",
                "modified_at": "2026-04-24T10:00:00Z",
            },
        )
        matches = await three_route_composite.aglob_paths(
            "*.md", "/home/workspace/.agents/user"
        )
        assert USER_PREFIX + "m.md" in matches
        assert MEMO_PREFIX + "p.md" in matches

    @pytest.mark.asyncio
    async def test_similar_prefix_no_false_match(
        self, three_route_composite, store, sandbox
    ):
        # `.agents/user/memory/x.md` MUST NOT be routed to memo even though
        # "memo" starts the same as "memory". Regression guard for prefix matching.
        await three_route_composite.awrite_text(
            USER_PREFIX + "nested.md", "still memory"
        )
        assert await store.aget(("user_abc", "memory"), "nested.md") is not None
        assert await store.aget(("user_abc", "memos"), "nested.md") is None
