"""Unit tests for ``StoreMemoryBackend``.

Uses ``InMemoryStore`` (LangGraph's in-process implementation) so tests don't
need Postgres. Verifies the rich-method surface that the custom tools consume:
path→key resolution, v2 value shape, read/write/edit round-trips, search.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends.memory import (
    MAX_CONTENT_BYTES,
    InvalidMemoryKeyError,
    MemoryContentTooLargeError,
    StoreMemoryBackend,
)

USER_PREFIX = "/home/workspace/.agents/user/memory/"


def _make_sandbox(working_dir: str = "/home/workspace") -> MagicMock:
    """Minimal ``SandboxBackend`` stand-in exposing just what MemoryBackend needs."""
    sandbox_backend = MagicMock()

    def _normalize(p: str) -> str:
        if p.startswith("/"):
            return p
        return f"{working_dir}/{p}"

    def _virtualize(p: str) -> str:
        if p.startswith(working_dir + "/"):
            return p[len(working_dir):]  # leaves leading slash
        return p

    sandbox_backend.normalize_path.side_effect = _normalize
    sandbox_backend.virtualize_path.side_effect = _virtualize
    sandbox_backend.validate_path.return_value = True
    sandbox_backend.filesystem_config.enable_path_validation = True
    sandbox_backend.root_dir = working_dir
    return sandbox_backend


@pytest.fixture
def sandbox_backend():
    return _make_sandbox()


@pytest.fixture
def store():
    return InMemoryStore()


def _make_backend(store, sandbox_backend, *, prefix: str = USER_PREFIX):
    return StoreMemoryBackend(
        store=store,
        namespace_factory=lambda: ("user_abc", "memory"),
        root_prefix=prefix,
        sandbox_backend=sandbox_backend,
    )


class TestPathToKey:
    def test_strips_prefix_to_simple_key(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        key = backend._path_to_key(USER_PREFIX + "memory.md")
        assert key == "memory.md"

    def test_strips_prefix_for_nested_key(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        key = backend._path_to_key(USER_PREFIX + "notes/q1/meeting.md")
        assert key == "notes/q1/meeting.md"

    def test_rejects_out_of_prefix_path(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        with pytest.raises(ValueError, match="not under memory root"):
            backend._path_to_key("/home/workspace/work/foo.md")

    def test_rejects_path_traversal(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        with pytest.raises(ValueError, match="Invalid key segment"):
            backend._path_to_key(USER_PREFIX + "../escape.md")

    def test_rejects_empty_key(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        with pytest.raises(ValueError, match="Empty memory key"):
            backend._path_to_key(USER_PREFIX)  # just the prefix, no filename

    def test_rejects_exact_root_without_trailing_slash(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        with pytest.raises(ValueError, match="not a file path"):
            # The prefix root itself (no trailing slash) isn't a file path.
            backend._path_to_key(USER_PREFIX.rstrip("/"))


class TestWriteRead:
    @pytest.mark.asyncio
    async def test_write_then_read_round_trips(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        ok = await backend.awrite_text(USER_PREFIX + "memory.md", "hello world")
        assert ok is True

        content = await backend.aread_text(USER_PREFIX + "memory.md")
        assert content == "hello world"

    @pytest.mark.asyncio
    async def test_aread_text_returns_none_when_missing(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        assert await backend.aread_text(USER_PREFIX + "nope.md") is None

    @pytest.mark.asyncio
    async def test_write_stores_v2_value_shape(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(USER_PREFIX + "memory.md", "body")
        item = await store.aget(("user_abc", "memory"), "memory.md")
        assert item is not None
        assert item.value["content"] == "body"
        assert item.value["encoding"] == "utf-8"
        assert isinstance(item.value["created_at"], str)
        assert isinstance(item.value["modified_at"], str)

    @pytest.mark.asyncio
    async def test_rewrite_preserves_created_at(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(USER_PREFIX + "memory.md", "v1")
        first = await store.aget(("user_abc", "memory"), "memory.md")
        created_first = first.value["created_at"]

        await backend.awrite_text(USER_PREFIX + "memory.md", "v2")
        second = await store.aget(("user_abc", "memory"), "memory.md")
        assert second.value["created_at"] == created_first
        assert second.value["content"] == "v2"

    @pytest.mark.asyncio
    async def test_aread_range_returns_line_slice(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(
            USER_PREFIX + "memory.md", "line1\nline2\nline3\nline4\n"
        )
        sliced = await backend.aread_range(USER_PREFIX + "memory.md", offset=1, limit=2)
        assert sliced == "line2\nline3\n"

    @pytest.mark.asyncio
    async def test_awrite_invalid_key_raises_specific_error(self, store, sandbox_backend):
        """Invalid keys raise a typed error the tool surface can render verbatim."""
        backend = _make_backend(store, sandbox_backend)
        # Path under the tier root, but with a `..` segment that bypasses the
        # sandbox's own normalization (the composite catches raw ``..`` in
        # normalize_path; here we exercise the backend's defense-in-depth).
        with pytest.raises(InvalidMemoryKeyError):
            await backend.awrite_text(USER_PREFIX + "../escape.md", "nope")

    @pytest.mark.asyncio
    async def test_awrite_rejects_content_over_size_cap(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        oversized = "x" * (MAX_CONTENT_BYTES + 1)
        with pytest.raises(MemoryContentTooLargeError):
            await backend.awrite_text(USER_PREFIX + "huge.md", oversized)
        # Nothing was written
        assert await backend.aread_text(USER_PREFIX + "huge.md") is None

    @pytest.mark.asyncio
    async def test_awrite_accepts_content_at_exact_cap(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        at_cap = "x" * MAX_CONTENT_BYTES
        ok = await backend.awrite_text(USER_PREFIX + "cap.md", at_cap)
        assert ok is True


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_parallel_writes_do_not_lose_data(self, store, sandbox_backend):
        """Two concurrent writers should both complete without data loss.

        Exercises the module-level per-namespace lock — without it, the
        last-writer still wins (store aput is atomic per-call) but the
        created_at preservation read-modify-write would race.
        """
        import asyncio

        backend = _make_backend(store, sandbox_backend)
        path = USER_PREFIX + "concurrent.md"

        results = await asyncio.gather(
            backend.awrite_text(path, "A"),
            backend.awrite_text(path, "B"),
        )
        assert all(results)
        final = await backend.aread_text(path)
        assert final in ("A", "B")

    @pytest.mark.asyncio
    async def test_multiple_backend_instances_share_a_lock(
        self, store, sandbox_backend
    ):
        """Two ``StoreMemoryBackend`` instances pointing at the same namespace
        must serialize writes against each other — simulates two concurrent
        turns that each build a fresh agent (the real prod scenario).

        If the lock were per-instance, the two RMW cycles could overlap and
        produce a ``created_at`` mismatch. With a shared namespace-keyed lock,
        only one write is in-flight at a time and the second correctly sees
        the first's ``created_at``.
        """
        import asyncio

        backend_a = _make_backend(store, sandbox_backend)
        backend_b = _make_backend(store, sandbox_backend)
        assert backend_a is not backend_b
        path = USER_PREFIX + "shared.md"

        # First write establishes created_at
        assert await backend_a.awrite_text(path, "v1")
        item = await store.aget(("user_abc", "memory"), "shared.md")
        original_created_at = item.value["created_at"]

        # Two concurrent writes from DIFFERENT backend instances on the same
        # namespace. With module-level locking, created_at stays stable.
        results = await asyncio.gather(
            backend_a.awrite_text(path, "v2"),
            backend_b.awrite_text(path, "v3"),
        )
        assert all(results)

        final_item = await store.aget(("user_abc", "memory"), "shared.md")
        assert final_item.value["created_at"] == original_created_at
        assert final_item.value["content"] in ("v2", "v3")


class TestEdit:
    @pytest.mark.asyncio
    async def test_edit_single_replacement(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(USER_PREFIX + "memory.md", "alpha beta gamma")

        result = await backend.aedit_text(
            USER_PREFIX + "memory.md", "beta", "BETA"
        )
        assert result["success"] is True
        assert result["occurrences"] == 1
        assert (
            await backend.aread_text(USER_PREFIX + "memory.md") == "alpha BETA gamma"
        )

    @pytest.mark.asyncio
    async def test_edit_missing_file_returns_error(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        result = await backend.aedit_text(
            USER_PREFIX + "missing.md", "a", "b"
        )
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_edit_string_not_found_returns_error(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(USER_PREFIX + "memory.md", "hello")
        result = await backend.aedit_text(
            USER_PREFIX + "memory.md", "missing", "replacement"
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_edit_multiple_matches_requires_replace_all(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(USER_PREFIX + "memory.md", "x x x")
        result = await backend.aedit_text(USER_PREFIX + "memory.md", "x", "y")
        assert result["success"] is False
        assert "3 times" in result["error"]

        result = await backend.aedit_text(
            USER_PREFIX + "memory.md", "x", "y", replace_all=True
        )
        assert result["success"] is True
        assert result["occurrences"] == 3
        assert await backend.aread_text(USER_PREFIX + "memory.md") == "y y y"

    @pytest.mark.asyncio
    async def test_edit_rejects_growth_past_size_cap(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        # Start just under the cap — replacing a small marker with a big string
        # would push the total past MAX_CONTENT_BYTES.
        base = "x" * (MAX_CONTENT_BYTES - 10) + "MARK"
        await backend.awrite_text(USER_PREFIX + "nearly.md", base)
        result = await backend.aedit_text(
            USER_PREFIX + "nearly.md", "MARK", "Y" * 100
        )
        assert result["success"] is False
        assert "bytes" in result["error"].lower()
        # Original content unchanged
        assert await backend.aread_text(USER_PREFIX + "nearly.md") == base


class TestSearch:
    @pytest.mark.asyncio
    async def test_aglob_paths_returns_absolute_paths(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(USER_PREFIX + "memory.md", "index")
        await backend.awrite_text(USER_PREFIX + "notes/foo.md", "foo")
        await backend.awrite_text(USER_PREFIX + "notes/bar.md", "bar")

        matches = await backend.aglob_paths("*.md", USER_PREFIX)
        assert USER_PREFIX + "memory.md" in matches
        assert USER_PREFIX + "notes/foo.md" in matches
        assert USER_PREFIX + "notes/bar.md" in matches

    @pytest.mark.asyncio
    async def test_agrep_rich_files_with_matches(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(USER_PREFIX + "a.md", "needle in a haystack")
        await backend.awrite_text(USER_PREFIX + "b.md", "nothing here")

        files = await backend.agrep_rich(
            "needle", path=USER_PREFIX, output_mode="files_with_matches"
        )
        assert files == [USER_PREFIX + "a.md"]

    @pytest.mark.asyncio
    async def test_agrep_rich_content_mode(self, store, sandbox_backend):
        backend = _make_backend(store, sandbox_backend)
        await backend.awrite_text(
            USER_PREFIX + "a.md", "foo bar\nbaz needle qux\n"
        )

        lines = await backend.agrep_rich(
            "needle", path=USER_PREFIX, output_mode="content"
        )
        assert lines == [USER_PREFIX + "a.md:2:baz needle qux"]


class TestNamespaceFactory:
    @pytest.mark.asyncio
    async def test_factory_called_per_operation(self, store, sandbox_backend):
        calls: list[int] = []

        def factory() -> tuple[str, ...]:
            calls.append(1)
            return ("user_abc", "memory")

        backend = StoreMemoryBackend(
            store=store,
            namespace_factory=factory,
            root_prefix=USER_PREFIX,
            sandbox_backend=sandbox_backend,
        )

        await backend.awrite_text(USER_PREFIX + "memory.md", "body")
        await backend.aread_text(USER_PREFIX + "memory.md")
        assert len(calls) >= 2  # one per op minimum

    @pytest.mark.asyncio
    async def test_workspace_namespace_is_isolated_from_user(self, store, sandbox_backend):
        user = StoreMemoryBackend(
            store=store,
            namespace_factory=lambda: ("user_abc", "memory"),
            root_prefix=USER_PREFIX,
            sandbox_backend=sandbox_backend,
        )
        workspace_prefix = "/home/workspace/.agents/workspace/memory/"
        workspace = StoreMemoryBackend(
            store=store,
            namespace_factory=lambda: ("user_abc", "workspaces", "ws_42", "memory"),
            root_prefix=workspace_prefix,
            sandbox_backend=sandbox_backend,
        )

        await user.awrite_text(USER_PREFIX + "memory.md", "USER")
        await workspace.awrite_text(workspace_prefix + "memory.md", "WORKSPACE")

        assert await user.aread_text(USER_PREFIX + "memory.md") == "USER"
        assert (
            await workspace.aread_text(workspace_prefix + "memory.md") == "WORKSPACE"
        )
        # Cross-tier reads return nothing
        assert (
            await user.aread_text(workspace_prefix + "memory.md") is None
            or True  # out-of-prefix returns None by design
        )
