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

    @pytest.mark.asyncio
    async def test_cross_worker_cancel_flag_short_circuits_before_llm(self, store):
        """When the Redis cancel flag is set, the LLM call must be skipped entirely."""
        await _seed_pending(store, "q1.md")
        llm_service = _make_llm_service(
            return_value=MemoMetadata(description="d", summary="s"),
        )

        from unittest.mock import AsyncMock as _AsyncMock, patch as _patch
        fake_cache = MagicMock()
        fake_cache.get = _AsyncMock(return_value="1")  # cancel flag is set
        fake_cache.set = _AsyncMock(return_value=True)
        fake_cache.delete = _AsyncMock(return_value=True)

        with _patch(
            "ptc_agent.agent.memo.metadata.get_cache_client",
            return_value=fake_cache,
        ):
            await generate_memo_metadata(
                store=store,
                namespace=NAMESPACE,
                key="q1.md",
                user_id="user_abc",
                llm_service=llm_service,
            )

        llm_service.complete.assert_not_called()
        # Row stays in 'pending' since merge never ran.
        item = await store.aget(NAMESPACE, "q1.md")
        assert item.value["metadata_status"] == "pending"

    @pytest.mark.asyncio
    async def test_cross_worker_cancel_after_llm_skips_merge(self, store):
        """A cancel flag raised mid-LLM blocks the post-LLM merge."""
        await _seed_pending(store, "q1.md")
        llm_service = _make_llm_service(
            return_value=MemoMetadata(description="d", summary="s"),
        )

        # First poll (pre-LLM) returns no flag; second poll (post-LLM) returns "1".
        from unittest.mock import AsyncMock as _AsyncMock, patch as _patch
        fake_cache = MagicMock()
        fake_cache.get = _AsyncMock(side_effect=[None, "1"])
        fake_cache.set = _AsyncMock(return_value=True)
        fake_cache.delete = _AsyncMock(return_value=True)

        with _patch(
            "ptc_agent.agent.memo.metadata.get_cache_client",
            return_value=fake_cache,
        ):
            await generate_memo_metadata(
                store=store,
                namespace=NAMESPACE,
                key="q1.md",
                user_id="user_abc",
                llm_service=llm_service,
            )

        llm_service.complete.assert_called_once()
        item = await store.aget(NAMESPACE, "q1.md")
        # Merge skipped: row still pending, no description from this run.
        assert item.value["metadata_status"] == "pending"

    @pytest.mark.asyncio
    async def test_cross_worker_cancel_fails_open_on_redis_error(self, store):
        """A Redis outage must not strand metadata generation — fail open."""
        await _seed_pending(store, "q1.md")
        llm_service = _make_llm_service(
            return_value=MemoMetadata(description="ok", summary="ok"),
        )

        from unittest.mock import AsyncMock as _AsyncMock, patch as _patch
        fake_cache = MagicMock()
        fake_cache.get = _AsyncMock(side_effect=RuntimeError("redis down"))

        with _patch(
            "ptc_agent.agent.memo.metadata.get_cache_client",
            return_value=fake_cache,
        ):
            await generate_memo_metadata(
                store=store,
                namespace=NAMESPACE,
                key="q1.md",
                user_id="user_abc",
                llm_service=llm_service,
            )

        item = await store.aget(NAMESPACE, "q1.md")
        assert item.value["metadata_status"] == "ready"
        assert item.value["description"] == "ok"

    @pytest.mark.asyncio
    async def test_post_llm_merge_holds_namespace_lock(self, store):
        """Merge + rebuild must serialize against concurrent delete via lock_for_namespace.

        Without the lock, a concurrent delete handler can complete between
        _merge_metadata's CAS read and its aput, resurrecting the row.
        """
        import asyncio

        from ptc_agent.agent.backends import lock_for_namespace

        await _seed_pending(store, "q1.md")
        llm_service = _make_llm_service(
            return_value=MemoMetadata(description="d", summary="s"),
        )

        # Externally hold the lock; metadata-finish must block on it.
        async with lock_for_namespace(NAMESPACE):
            task = asyncio.create_task(
                generate_memo_metadata(
                    store=store,
                    namespace=NAMESPACE,
                    key="q1.md",
                    user_id="user_abc",
                    llm_service=llm_service,
                ),
            )
            # Yield enough times for the LLM call to resolve and the task to
            # reach the lock-protected merge/rebuild block.
            for _ in range(20):
                await asyncio.sleep(0)
            assert not task.done(), "metadata-finish should be blocked by external lock"

        await asyncio.wait_for(task, timeout=2.0)

        item = await store.aget(NAMESPACE, "q1.md")
        assert item.value["metadata_status"] == "ready"

    @pytest.mark.asyncio
    async def test_concurrent_delete_during_merge_does_not_resurrect_row(self, store):
        """Real concurrent delete + merge must end with the row absent.

        Stronger property than ``test_post_llm_merge_holds_namespace_lock``,
        which only proves the metadata task waits when the lock is held by
        an external acquirer. Here we run the merge against a delete that
        holds the lock first — once delete drops the lock, the metadata
        task acquires it, but its CAS on ``modified_at`` must skip the
        merge because the row was deleted under it. End state: no row.
        """
        import asyncio as _asyncio

        from ptc_agent.agent.backends import lock_for_namespace

        await _seed_pending(store, "q1.md")
        llm_service = _make_llm_service(
            return_value=MemoMetadata(description="ZOMBIE", summary="ZOMBIE"),
        )

        # Hold the lock while the metadata task starts up. While we hold it,
        # delete the row. When we release the lock, the metadata task should
        # run its post-LLM CAS, see the row gone, and skip the merge.
        async with lock_for_namespace(NAMESPACE):
            task = _asyncio.create_task(
                generate_memo_metadata(
                    store=store,
                    namespace=NAMESPACE,
                    key="q1.md",
                    user_id="user_abc",
                    llm_service=llm_service,
                ),
            )
            # Yield enough that the LLM mock resolves and the task is queued
            # on the lock acquisition for the merge step.
            for _ in range(20):
                await _asyncio.sleep(0)
            await store.adelete(NAMESPACE, "q1.md")

        await _asyncio.wait_for(task, timeout=2.0)

        # Row stays deleted: merge saw item is None and skipped the aput.
        assert await store.aget(NAMESPACE, "q1.md") is None

    @pytest.mark.asyncio
    async def test_legacy_second_precision_modified_at_does_not_block_merge(self, store):
        """Pre-microsecond rows should still flip pending → ready cleanly.

        Older memos have ``modified_at`` at second precision
        (``2026-04-24T10:00:00Z``); freshly written rows use microseconds
        (``2026-04-24T10:00:00.123456Z``). The CAS in ``_merge_metadata``
        compares strings, so it must compare the snapshot's value to the
        re-fetched value — not to a fresh ``now_iso()``. This test seeds a
        legacy timestamp, runs metadata, and asserts the row reaches ready
        with the new microsecond timestamp on it.
        """
        await store.aput(
            NAMESPACE,
            "legacy.md",
            {
                "content": "Legacy memo body that is plenty long enough.",
                "encoding": "utf-8",
                "mime_type": "text/markdown",
                "original_filename": "legacy.md",
                "key": "legacy.md",
                "description": "...",
                "summary": "",
                "metadata_status": "pending",
                "metadata_error": None,
                "created_at": "2026-04-24T10:00:00Z",
                "modified_at": "2026-04-24T10:00:00Z",  # legacy second precision
            },
        )
        llm_service = _make_llm_service(
            return_value=MemoMetadata(description="legacy ok", summary="ok"),
        )

        await generate_memo_metadata(
            store=store,
            namespace=NAMESPACE,
            key="legacy.md",
            user_id="user_abc",
            llm_service=llm_service,
        )

        item = await store.aget(NAMESPACE, "legacy.md")
        assert item.value["metadata_status"] == "ready"
        assert item.value["description"] == "legacy ok"
        # Microsecond format on the new modified_at, distinguishable from the
        # legacy seed.
        assert "." in item.value["modified_at"]
        assert item.value["modified_at"] != "2026-04-24T10:00:00Z"
