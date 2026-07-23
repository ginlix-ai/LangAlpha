"""Integration tests for conversation persistence (threads, queries, responses)."""

from __future__ import annotations

import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestCreateThread:
    """Test thread creation."""

    async def test_create_thread_basic(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation import create_thread

        thread_id = str(uuid.uuid4())
        ws_id = str(seed_workspace["workspace_id"])

        thread = await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="ptc",
            title="Hello world",
        )

        assert str(thread["conversation_thread_id"]) == thread_id
        assert str(thread["workspace_id"]) == ws_id
        assert thread["current_status"] == "in_progress"
        assert thread["msg_type"] == "ptc"
        assert thread["title"] == "Hello world"
        assert thread["thread_index"] == 0

    async def test_create_thread_auto_increments_index(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation import create_thread

        ws_id = str(seed_workspace["workspace_id"])

        t1 = await create_thread(
            conversation_thread_id=str(uuid.uuid4()),
            workspace_id=ws_id,
            current_status="completed",
            msg_type="flash",
        )
        t2 = await create_thread(
            conversation_thread_id=str(uuid.uuid4()),
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="flash",
        )

        assert t1["thread_index"] == 0
        assert t2["thread_index"] == 1

    async def test_create_thread_with_external_id(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation import create_thread

        ws_id = str(seed_workspace["workspace_id"])
        thread = await create_thread(
            conversation_thread_id=str(uuid.uuid4()),
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="flash",
            external_id="telegram:12345:42",
            platform="telegram",
        )

        assert thread["external_id"] == "telegram:12345:42"
        assert thread["platform"] == "telegram"


class TestEnsureThreadExists:
    """Test the ensure_thread_exists helper."""

    async def test_creates_new_thread(
        self, seed_workspace, seed_user, patched_get_db_connection
    ):
        from src.server.database.conversation import (
            ensure_thread_exists,
            get_workspace_threads,
        )

        ws_id = str(seed_workspace["workspace_id"])
        thread_id = str(uuid.uuid4())

        await ensure_thread_exists(
            workspace_id=ws_id,
            conversation_thread_id=thread_id,
            user_id=seed_user["user_id"],
            initial_query="What is AAPL trading at?",
            msg_type="flash",
        )

        threads, total = await get_workspace_threads(ws_id)
        assert total == 1
        assert str(threads[0]["conversation_thread_id"]) == thread_id
        assert threads[0]["title"] == "What is AAPL trading at?"

    async def test_resumes_existing_thread(
        self, seed_workspace, seed_user, patched_get_db_connection
    ):
        from src.server.database.conversation import (
            create_thread,
            ensure_thread_exists,
            get_workspace_threads,
        )

        ws_id = str(seed_workspace["workspace_id"])
        thread_id = str(uuid.uuid4())

        # Create thread first
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=ws_id,
            current_status="completed",
            msg_type="ptc",
        )

        # ensure_thread_exists should update status, not create duplicate
        await ensure_thread_exists(
            workspace_id=ws_id,
            conversation_thread_id=thread_id,
            user_id=seed_user["user_id"],
            initial_query="Follow up",
            initial_status="in_progress",
            msg_type="ptc",
        )

        threads, total = await get_workspace_threads(ws_id)
        assert total == 1
        assert threads[0]["current_status"] == "in_progress"


class TestQueryCRUD:
    """Test query creation and retrieval."""

    async def test_create_and_get_queries(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation import (
            create_query,
            create_thread,
            get_queries_for_thread,
        )

        ws_id = str(seed_workspace["workspace_id"])
        thread_id = str(uuid.uuid4())
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="ptc",
        )

        # Insert two queries
        await create_query(
            conversation_query_id=str(uuid.uuid4()),
            conversation_thread_id=thread_id,
            turn_index=0,
            content="What is the PE ratio of AAPL?",
            query_type="initial",
        )
        await create_query(
            conversation_query_id=str(uuid.uuid4()),
            conversation_thread_id=thread_id,
            turn_index=1,
            content="Compare it with MSFT",
            query_type="follow_up",
        )

        queries, total = await get_queries_for_thread(thread_id)
        assert total == 2
        assert queries[0]["content"] == "What is the PE ratio of AAPL?"
        assert queries[0]["type"] == "initial"
        assert queries[1]["content"] == "Compare it with MSFT"
        assert queries[1]["type"] == "follow_up"

    async def test_idempotent_query_insert(
        self, seed_workspace, patched_get_db_connection
    ):
        import pytest

        from src.server.database.conversation import (
            QueryConflictError,
            create_query,
            create_thread,
            get_queries_for_thread,
        )

        ws_id = str(seed_workspace["workspace_id"])
        thread_id = str(uuid.uuid4())
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="ptc",
        )

        query_id = str(uuid.uuid4())
        await create_query(
            conversation_query_id=query_id,
            conversation_thread_id=thread_id,
            turn_index=0,
            content="Original",
            query_type="initial",
            idempotent=True,
        )

        # Same (thread, turn_index, content) is a true no-op.
        await create_query(
            conversation_query_id=str(uuid.uuid4()),
            conversation_thread_id=thread_id,
            turn_index=0,
            content="Original",
            query_type="initial",
            idempotent=True,
        )

        # Same (thread, turn_index) with different content raises — the
        # content-gated upsert refuses to silently overwrite a concurrent
        # write's row.
        with pytest.raises(QueryConflictError):
            await create_query(
                conversation_query_id=str(uuid.uuid4()),
                conversation_thread_id=thread_id,
                turn_index=0,
                content="Updated",
                query_type="initial",
                idempotent=True,
            )

        queries, total = await get_queries_for_thread(thread_id)
        assert total == 1
        assert queries[0]["content"] == "Original"


class TestResponseCRUD:
    """Test response creation and retrieval."""

    async def test_create_and_get_responses(
        self, seed_workspace, patched_get_db_connection, seed_response
    ):
        from src.server.database.conversation import (
            create_thread,
            get_responses_for_thread,
        )

        ws_id = str(seed_workspace["workspace_id"])
        thread_id = str(uuid.uuid4())
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="ptc",
        )

        await seed_response(
            conversation_response_id=str(uuid.uuid4()),
            conversation_thread_id=thread_id,
            turn_index=0,
            status="completed",
            execution_time=2.5,
        )

        responses, total = await get_responses_for_thread(thread_id)
        assert total == 1
        assert responses[0]["status"] == "completed"
        assert responses[0]["execution_time"] == pytest.approx(2.5)


class TestThreadLifecycle:
    """Test thread update and delete operations."""

    async def test_update_thread_status(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation import (
            create_thread,
            update_thread_status,
        )

        ws_id = str(seed_workspace["workspace_id"])
        thread_id = str(uuid.uuid4())
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="ptc",
        )

        result = await update_thread_status(thread_id, "completed")
        assert result is True

    async def test_update_thread_title(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation import (
            create_thread,
            update_thread_title,
        )

        ws_id = str(seed_workspace["workspace_id"])
        thread_id = str(uuid.uuid4())
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="ptc",
            title="Original title",
        )

        updated = await update_thread_title(thread_id, "Updated title")
        assert updated is not None
        assert updated["title"] == "Updated title"

    async def test_delete_thread_cascades(
        self, seed_workspace, patched_get_db_connection, seed_response
    ):
        from src.server.database.conversation import (
            create_query,
            create_thread,
            delete_thread,
            get_queries_for_thread,
            get_responses_for_thread,
        )

        ws_id = str(seed_workspace["workspace_id"])
        thread_id = str(uuid.uuid4())
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=ws_id,
            current_status="in_progress",
            msg_type="ptc",
        )

        await create_query(
            conversation_query_id=str(uuid.uuid4()),
            conversation_thread_id=thread_id,
            turn_index=0,
            content="Test query",
            query_type="initial",
        )
        await seed_response(
            conversation_response_id=str(uuid.uuid4()),
            conversation_thread_id=thread_id,
            turn_index=0,
            status="completed",
        )

        deleted = await delete_thread(thread_id)
        assert deleted is True

        # Queries and responses should be cascade-deleted
        queries, q_total = await get_queries_for_thread(thread_id)
        responses, r_total = await get_responses_for_thread(thread_id)
        assert q_total == 0
        assert r_total == 0
