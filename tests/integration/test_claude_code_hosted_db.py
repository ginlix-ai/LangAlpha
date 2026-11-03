"""The choice for cloud computers and the count of a user's turns there,
against real PostgreSQL (migration 105's ``claude_code_custody`` and the run
ledger)."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from psycopg.rows import dict_row, tuple_row
from psycopg.types.json import Jsonb

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def user(seed_user, patched_get_db_connection, monkeypatch):
    """The module's reads get tuple rows, as the app's pool hands them; the
    test pool defaults to dict rows, which would hide a read by name."""
    from src.server.database import claude_code_hosted

    @asynccontextmanager
    async def _app_rows(conn=None):
        async with patched_get_db_connection(conn) as owned:
            owned.row_factory = tuple_row
            try:
                yield owned
            finally:
                owned.row_factory = dict_row

    monkeypatch.setattr(claude_code_hosted, "get_db_connection", _app_rows)
    yield seed_user["user_id"]
    async with patched_get_db_connection() as conn:
        await conn.execute("DELETE FROM claude_code_custody WHERE user_id = %s", (seed_user["user_id"],))


async def test_no_choice_reads_as_local_and_a_choice_replaces_the_last(user):
    from src.server.database.claude_code_hosted import get_custody_mode, set_custody_mode

    assert await get_custody_mode(user) == "local"
    await set_custody_mode(user, "api_key")
    assert await get_custody_mode(user) == "api_key"
    await set_custody_mode(user, "local")
    assert await get_custody_mode(user) == "local"


async def test_only_the_users_runs_in_progress_on_a_worker_are_counted(user, seed_workspace, patched_get_db_connection):
    from src.server.database.claude_code_hosted import WORKER_TURN, live_worker_turns

    threads = [str(uuid.uuid4()) for _ in range(4)]
    async with patched_get_db_connection() as conn, conn.cursor() as cur:
        for index, thread in enumerate(threads):
            await cur.execute(
                "INSERT INTO conversation_threads (conversation_thread_id, workspace_id, current_status, thread_index) "
                "VALUES (%s, %s, 'in_progress', %s)",
                (thread, seed_workspace["workspace_id"], index),
            )
        for thread, (owner, metadata) in zip(threads, [
            (user, WORKER_TURN), (user, WORKER_TURN), (user, {}), ("someone-else", WORKER_TURN),
        ]):
            await cur.execute(
                "INSERT INTO conversation_responses (conversation_thread_id, turn_index, status, metadata, created_at, user_id) "
                "VALUES (%s, 0, 'in_progress', %s, NOW(), %s)",
                (thread, Jsonb(metadata), owner),
            )
        await cur.execute(
            "UPDATE conversation_responses SET status = 'completed' WHERE conversation_thread_id = %s", (threads[1],)
        )
    assert await live_worker_turns(user) == 1
    assert await live_worker_turns(user, besides=threads[0]) == 0


@pytest.mark.asyncio
async def test_an_anthropic_key_reads_as_none_while_the_users_own_keys_are_off(user, monkeypatch):
    from src.server.database.api_keys import get_key_with_base_url, saved_key, set_byok_enabled, upsert_api_key

    monkeypatch.setenv("BYOK_ENCRYPTION_KEY", "claude-code-byok-test-key-32chars")
    await upsert_api_key(user, "anthropic", "sk-ant-test")
    await set_byok_enabled(user, False)
    assert await saved_key(user, "anthropic") is None
    assert await get_key_with_base_url(user, "anthropic") is None
    await set_byok_enabled(user, True)
    assert await saved_key(user, "anthropic") == {"base_url": None}
    assert await get_key_with_base_url(user, "anthropic") == {"api_key": "sk-ant-test", "base_url": None}
