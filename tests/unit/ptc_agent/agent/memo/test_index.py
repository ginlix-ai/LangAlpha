"""Unit tests for memo.md deterministic rebuild.

Uses ``InMemoryStore`` for the actual store interactions. Freezes the
timestamp via ``patch`` so we can assert byte-exact output where it matters.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.memo.index import rebuild_memo_index

NAMESPACE = ("user_abc", "memos")


@pytest.fixture
def store():
    return InMemoryStore()


async def _seed(store, key, value):
    await store.aput(NAMESPACE, key, value)


def _base_value(**overrides):
    base = {
        "content": "irrelevant body",
        "encoding": "utf-8",
        "original_filename": "x.md",
        "key": "x",
        "created_at": "2026-04-24T10:00:00Z",
        "modified_at": "2026-04-24T10:00:00Z",
        "description": "",
        "summary": "",
        "metadata_status": "ready",
    }
    base.update(overrides)
    return base


class TestRebuild:
    @pytest.mark.asyncio
    async def test_empty_namespace_writes_header_only(self, store):
        await rebuild_memo_index(store, NAMESPACE)
        item = await store.aget(NAMESPACE, "memo.md")
        assert item is not None
        body = item.value["content"]
        assert body.startswith("# Memos")
        assert "0 memo(s)." in body
        assert "_No memos yet._" in body

    @pytest.mark.asyncio
    async def test_single_ready_entry_rendered(self, store):
        await _seed(
            store,
            "q1-thesis.md",
            _base_value(
                key="q1-thesis.md",
                created_at="2026-04-24T10:00:00Z",
                description="Long thesis on AI winners for Q1 2026.",
                metadata_status="ready",
            ),
        )
        await rebuild_memo_index(store, NAMESPACE)
        body = (await store.aget(NAMESPACE, "memo.md")).value["content"]
        assert "1 memo(s)." in body
        assert (
            "- [q1-thesis.md](q1-thesis.md) — Long thesis on AI winners for Q1 2026."
            in body
        )

    @pytest.mark.asyncio
    async def test_pending_shows_placeholder(self, store):
        await _seed(
            store,
            "doc.md",
            _base_value(
                key="doc.md",
                description="",
                metadata_status="pending",
            ),
        )
        await rebuild_memo_index(store, NAMESPACE)
        body = (await store.aget(NAMESPACE, "memo.md")).value["content"]
        assert "Summary generating" in body

    @pytest.mark.asyncio
    async def test_failed_shows_regenerate_hint(self, store):
        await _seed(
            store,
            "bad.md",
            _base_value(
                key="bad.md",
                description="unused",
                metadata_status="failed",
                metadata_error="LLM timeout",
            ),
        )
        await rebuild_memo_index(store, NAMESPACE)
        body = (await store.aget(NAMESPACE, "memo.md")).value["content"]
        assert "Summary unavailable" in body

    @pytest.mark.asyncio
    async def test_entries_sorted_by_created_at(self, store):
        await _seed(
            store,
            "later.md",
            _base_value(
                key="later.md",
                created_at="2026-04-24T11:00:00Z",
                description="Later",
                metadata_status="ready",
            ),
        )
        await _seed(
            store,
            "earlier.md",
            _base_value(
                key="earlier.md",
                created_at="2026-04-24T09:00:00Z",
                description="Earlier",
                metadata_status="ready",
            ),
        )
        await rebuild_memo_index(store, NAMESPACE)
        body = (await store.aget(NAMESPACE, "memo.md")).value["content"]
        earlier_idx = body.index("earlier.md")
        later_idx = body.index("later.md")
        assert earlier_idx < later_idx

    @pytest.mark.asyncio
    async def test_memo_md_itself_excluded_from_entries(self, store):
        # Seed a pre-existing memo.md so the rebuild has to ignore it.
        await _seed(store, "memo.md", {"content": "stale", "encoding": "utf-8"})
        await _seed(
            store,
            "real.md",
            _base_value(key="real.md", description="Real", metadata_status="ready"),
        )
        await rebuild_memo_index(store, NAMESPACE)
        body = (await store.aget(NAMESPACE, "memo.md")).value["content"]
        # memo.md is not self-listed.
        assert "[memo.md](memo.md)" not in body
        assert "[real.md](real.md)" in body
        assert "1 memo(s)." in body

    @pytest.mark.asyncio
    async def test_rebuild_is_idempotent(self, store):
        await _seed(
            store,
            "a.md",
            _base_value(key="a.md", description="A", metadata_status="ready"),
        )
        # Freeze time so both rebuilds produce byte-identical output.
        with patch(
            "ptc_agent.agent.memo.index.datetime"
        ) as mock_dt:
            from datetime import UTC, datetime
            frozen = datetime(2026, 4, 24, 10, 15, 0, tzinfo=UTC)
            mock_dt.now.return_value = frozen
            mock_dt.UTC = UTC
            await rebuild_memo_index(store, NAMESPACE)
            first = (await store.aget(NAMESPACE, "memo.md")).value["content"]
            await rebuild_memo_index(store, NAMESPACE)
            second = (await store.aget(NAMESPACE, "memo.md")).value["content"]
            assert first == second

    @pytest.mark.asyncio
    async def test_rebuild_preserves_created_at(self, store):
        # Pre-existing memo.md with a created_at that must survive rewrite.
        await _seed(
            store,
            "memo.md",
            {
                "content": "old",
                "encoding": "utf-8",
                "created_at": "2026-04-01T00:00:00Z",
                "modified_at": "2026-04-01T00:00:00Z",
            },
        )
        await _seed(
            store,
            "a.md",
            _base_value(key="a.md", description="A", metadata_status="ready"),
        )
        await rebuild_memo_index(store, NAMESPACE)
        item = await store.aget(NAMESPACE, "memo.md")
        assert item.value["created_at"] == "2026-04-01T00:00:00Z"
