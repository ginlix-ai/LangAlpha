"""Unit tests for ``generate_memo_metadata``.

Verifies the pending → ready (and pending → failed) transitions and the
idempotent memo.md rebuild after each path. Mocks the LLMService so no
network calls are made.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.memo.metadata import generate_memo_metadata
from ptc_agent.agent.memo.schema import MemoMetadata

NAMESPACE = ("user_abc", "memos")


@pytest.fixture
def store():
    return InMemoryStore()


async def _seed_pending(store, key: str = "q1.md") -> None:
    await store.aput(
        NAMESPACE,
        key,
        {
            "content": "Long document body describing Q1 thesis.",
            "encoding": "utf-8",
            "mime_type": "text/markdown",
            "original_filename": "Q1 Thesis.md",
            "key": key,
            "description": "Summary generating…",
            "summary": "",
            "metadata_status": "pending",
            "metadata_error": None,
            "created_at": "2026-04-24T10:00:00Z",
            "modified_at": "2026-04-24T10:00:00Z",
        },
    )


def _make_llm_service(return_value=None, side_effect=None) -> MagicMock:
    svc = MagicMock()
    svc.complete = AsyncMock(side_effect=side_effect, return_value=return_value)
    return svc


class TestGenerate:
    @pytest.mark.asyncio
    async def test_success_writes_ready_and_rebuilds_index(self, store):
        await _seed_pending(store, "q1.md")
        metadata = MemoMetadata(
            description="A thesis on Q1 2026 AI winners.",
            summary="A 2-3 paragraph summary of the thesis...",
        )
        llm_service = _make_llm_service(return_value=metadata)

        await generate_memo_metadata(
            store=store,
            namespace=NAMESPACE,
            key="q1.md",
            user_id="user_abc",
            llm_service=llm_service,
        )

        item = await store.aget(NAMESPACE, "q1.md")
        assert item.value["metadata_status"] == "ready"
        assert item.value["description"] == metadata.description
        assert item.value["summary"] == metadata.summary
        assert item.value["metadata_generated_at"]
        assert item.value["metadata_error"] is None

        memo_md = await store.aget(NAMESPACE, "memo.md")
        assert memo_md is not None
        assert "A thesis on Q1 2026 AI winners." in memo_md.value["content"]
        assert "1 memo(s)." in memo_md.value["content"]

    @pytest.mark.asyncio
    async def test_failure_writes_failed_status_and_rebuilds(self, store):
        await _seed_pending(store, "q1.md")
        llm_service = _make_llm_service(side_effect=RuntimeError("LLM down"))

        await generate_memo_metadata(
            store=store,
            namespace=NAMESPACE,
            key="q1.md",
            user_id="user_abc",
            llm_service=llm_service,
        )

        item = await store.aget(NAMESPACE, "q1.md")
        assert item.value["metadata_status"] == "failed"
        assert "LLM down" in item.value["metadata_error"]

        memo_md = await store.aget(NAMESPACE, "memo.md")
        # Failed memos still show up in memo.md with a specific hint.
        assert "Summary unavailable" in memo_md.value["content"]

    @pytest.mark.asyncio
    async def test_missing_key_returns_without_raising(self, store):
        """Background tasks never raise out."""
        llm_service = _make_llm_service(
            return_value=MemoMetadata(description="d", summary="s"),
        )
        # key doesn't exist in store
        await generate_memo_metadata(
            store=store,
            namespace=NAMESPACE,
            key="nonexistent.md",
            user_id="user_abc",
            llm_service=llm_service,
        )
        # LLM should not have been called
        llm_service.complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_llm_service_called_with_memo_metadata_schema(self, store):
        await _seed_pending(store, "q1.md")
        metadata = MemoMetadata(description="d", summary="s")
        llm_service = _make_llm_service(return_value=metadata)

        await generate_memo_metadata(
            store=store,
            namespace=NAMESPACE,
            key="q1.md",
            user_id="user_abc",
            llm_service=llm_service,
        )

        kwargs = llm_service.complete.await_args.kwargs
        assert kwargs["response_schema"] is MemoMetadata
        assert kwargs["mode"] == "flash"
        assert kwargs["user_id"] == "user_abc"
        # The filename and mime type flow into the prompt
        assert "Q1 Thesis.md" in kwargs["user_prompt"]
        assert "text/markdown" in kwargs["user_prompt"]

    @pytest.mark.asyncio
    async def test_merge_skips_when_row_modified_during_llm_call(self, store):
        """Concurrent edit during LLM call → post-LLM merge must NOT win.

        Regression: previously the pre-LLM ``base_value`` was trusted when
        its ``modified_at`` matched ``expected_modified_at`` — but those
        two values came from the same snapshot, so the check always
        succeeded and silently allowed stale-metadata writes over newer
        content. The fix re-fetches the current row before merging.
        """
        await _seed_pending(store, "q1.md")

        async def _slow_llm(*_args, **_kwargs):
            item = await store.aget(NAMESPACE, "q1.md")
            # Simulate the user editing the memo while the LLM was running.
            await store.aput(
                NAMESPACE,
                "q1.md",
                {**item.value, "modified_at": "2099-01-01T00:00:00Z"},
            )
            return MemoMetadata(description="STALE", summary="STALE")

        llm_service = MagicMock()
        llm_service.complete = AsyncMock(side_effect=_slow_llm)

        await generate_memo_metadata(
            store=store,
            namespace=NAMESPACE,
            key="q1.md",
            user_id="user_abc",
            llm_service=llm_service,
        )

        item = await store.aget(NAMESPACE, "q1.md")
        assert item.value["description"] != "STALE"
        assert item.value["modified_at"] == "2099-01-01T00:00:00Z"

    @pytest.mark.asyncio
    async def test_merge_skips_when_row_deleted_during_llm_call(self, store):
        """A row deleted mid-LLM-call must not be resurrected on merge."""
        await _seed_pending(store, "q1.md")

        async def _delete_then_return(*_args, **_kwargs):
            await store.adelete(NAMESPACE, "q1.md")
            return MemoMetadata(description="ZOMBIE", summary="ZOMBIE")

        llm_service = MagicMock()
        llm_service.complete = AsyncMock(side_effect=_delete_then_return)

        await generate_memo_metadata(
            store=store,
            namespace=NAMESPACE,
            key="q1.md",
            user_id="user_abc",
            llm_service=llm_service,
        )

        assert await store.aget(NAMESPACE, "q1.md") is None
