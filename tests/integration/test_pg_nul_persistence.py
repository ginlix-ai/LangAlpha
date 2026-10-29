"""Integration tests for PG NUL/`\\u0000` persistence safety.

With sanitization in place at the persistence boundary (`SafeJson` +
`strip_pg_nul_str`) and at the source (tool-result middleware), these tests
pin the contracted behavior:

- `workspace_files.content_text` accepts NUL-bearing input without aborting
  the whole batch (sandbox files can carry `\\x00` in binary-looking text).
- `conversation_queries.content` accepts NUL in user-typed input.
"""

from __future__ import annotations

import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestWorkspaceFileBulkUpsert:
    async def test_content_text_with_nul_inserts_cleanly(
        self, seed_workspace, patched_get_db_connection
    ):
        """bulk_upsert must not abort the whole batch when one file contains NUL bytes."""
        from src.server.database.workspace_file import (
            bulk_upsert_files,
            get_file,
        )

        ws_id = str(seed_workspace["workspace_id"])

        files = [
            {
                "file_path": "work/clean.txt",
                "file_name": "clean.txt",
                "file_size": 5,
                "content_text": "hello",
                "is_binary": False,
            },
            {
                "file_path": "work/nulfile.txt",
                "file_name": "nulfile.txt",
                "file_size": 7,
                "content_text": "pre\x00post",
                "is_binary": False,
            },
        ]

        count = await bulk_upsert_files(ws_id, files)

        # Both rows must insert; the NUL-bearing row's content must be stripped.
        assert count == 2

        clean = await get_file(ws_id, "work/clean.txt")
        assert clean is not None
        assert clean["content_text"] == "hello"

        scrubbed = await get_file(ws_id, "work/nulfile.txt")
        assert scrubbed is not None
        assert "\x00" not in scrubbed["content_text"]
        assert scrubbed["content_text"] == "prepost"

    async def test_nul_in_path_components_does_not_break_batch(
        self, seed_workspace, patched_get_db_connection
    ):
        """Defense in depth — file_path/file_name are also TEXT columns."""
        from src.server.database.workspace_file import (
            bulk_upsert_files,
            get_files_for_workspace,
        )

        ws_id = str(seed_workspace["workspace_id"])

        # Stripped path becomes "work/odd.txt".
        files = [
            {
                "file_path": "work/odd\x00.txt",
                "file_name": "odd\x00.txt",
                "file_size": 0,
                "content_text": "",
                "is_binary": False,
            },
        ]

        count = await bulk_upsert_files(ws_id, files)
        assert count == 1

        rows = await get_files_for_workspace(ws_id)
        assert any(r["file_path"] == "work/odd.txt" for r in rows)


class TestConversationQueryUserInput:
    async def test_user_input_with_nul_persists(
        self, seed_workspace, patched_get_db_connection
    ):
        """User pastes binary noise — server must not 500."""
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

        await create_query(
            conversation_query_id=str(uuid.uuid4()),
            conversation_thread_id=thread_id,
            turn_index=0,
            content="What is\x00 going on?",
            query_type="initial",
            metadata={"hint": "weird\x00paste"},
        )

        queries, total = await get_queries_for_thread(thread_id)
        assert total == 1
        assert "\x00" not in queries[0]["content"]
        assert queries[0]["content"] == "What is going on?"
        # metadata JSONB also clean
        meta = queries[0]["metadata"] or {}
        assert "\x00" not in (meta.get("hint") or "")
