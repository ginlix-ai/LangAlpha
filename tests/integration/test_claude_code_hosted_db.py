"""The choice for cloud computers, an operator's block, and the user's turns
there, against real PostgreSQL (migration 105's ``claude_code_custody``, the
token store and the run ledger)."""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio
from psycopg.rows import dict_row, tuple_row
from psycopg.types.json import Jsonb

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
def _encryption_key():
    with patch.dict(os.environ, {"BYOK_ENCRYPTION_KEY": "test-byok-encryption-key-32char!"}):
        yield


@pytest_asyncio.fixture
async def user(seed_user, patched_get_db_connection, monkeypatch):
    """The custody module's reads and writes get tuple rows, as the app's pool
    hands them, the token store's lock included; the test pool defaults to
    dict rows, which would hide a read by name."""
    from src.server.database import claude_code_hosted, oauth_tokens

    @asynccontextmanager
    async def _app_rows(conn=None):
        async with patched_get_db_connection(conn) as owned:
            owned.row_factory = tuple_row
            try:
                yield owned
            finally:
                owned.row_factory = dict_row

    monkeypatch.setattr(claude_code_hosted, "get_db_connection", _app_rows)
    monkeypatch.setattr(oauth_tokens, "get_db_connection", _app_rows)
    yield seed_user["user_id"]
    monkeypatch.undo()
    await oauth_tokens.delete_oauth_tokens(seed_user["user_id"], oauth_tokens.CLAUDE_CODE_PROVIDER)
    async with patched_get_db_connection() as conn:
        await conn.execute("DELETE FROM claude_code_custody WHERE user_id = %s", (seed_user["user_id"],))


async def _store(user_id: str) -> None:
    from src.server.database.oauth_tokens import CLAUDE_CODE_PROVIDER, upsert_oauth_tokens

    await upsert_oauth_tokens(
        user_id, CLAUDE_CODE_PROVIDER, access_token="sk-ant-oat01-stored", refresh_token="", account_id="",
        expires_at=datetime.now(timezone.utc) + timedelta(days=300),
    )


async def test_no_choice_reads_as_local_and_a_choice_replaces_the_last(user):
    from src.server.database.claude_code_hosted import Custody, get_custody, write_custody

    assert await get_custody(user) == Custody("local", blocked=False)
    await write_custody(user, "api_key")
    assert (await get_custody(user)).mode == "api_key"
    await write_custody(user, "local")
    assert (await get_custody(user)).mode == "local"


async def test_the_consent_accepted_for_storing_stays_on_record_after_a_later_choice(user, patched_get_db_connection):
    from src.server.database.claude_code_hosted import write_custody

    await write_custody(user, "stored", disclosure="2026-11-v1")
    await write_custody(user, "api_key")
    async with patched_get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT mode, disclosure FROM claude_code_custody WHERE user_id = %s", (user,))
        assert dict(await cur.fetchone()) == {"mode": "api_key", "disclosure": "2026-11-v1"}


async def test_any_choice_but_storing_deletes_the_token_with_it_and_a_block_outlasts_the_users_writes(user):
    from src.server.database.claude_code_hosted import (
        Custody, StoringBlocked, get_custody, stored_users, unblock, write_custody,
    )
    from src.server.database.oauth_tokens import CLAUDE_CODE_PROVIDER, get_oauth_connection

    await write_custody(user, "stored", disclosure="2026-11-v1")
    await _store(user)
    assert await write_custody(user, "stored") is None
    assert await get_oauth_connection(user, CLAUDE_CODE_PROVIDER) is not None
    assert user in await stored_users()
    assert await write_custody(user, "api_key") == "sk-ant-oat01-stored"
    assert await get_oauth_connection(user, CLAUDE_CODE_PROVIDER) is None
    assert await get_custody(user) == Custody("api_key", blocked=False)
    assert user not in await stored_users()
    assert await write_custody(user, "local") is None

    await _store(user)
    await write_custody(user, "local", block_reason="terms")
    assert await get_oauth_connection(user, CLAUDE_CODE_PROVIDER) is None
    await write_custody(user, "api_key")
    await write_custody(user, "local")
    assert await get_custody(user) == Custody("local", blocked=True)
    with pytest.raises(StoringBlocked):
        await write_custody(user, "stored", disclosure="2026-11-v1")
    assert await get_custody(user) == Custody("local", blocked=True)
    assert await unblock(user) and not await unblock(user)
    assert await get_custody(user) == Custody("local", blocked=False)


async def test_a_token_unreadable_under_the_current_key_is_deleted_with_any_other_choice(user, patched_get_db_connection):
    from src.server.database.claude_code_hosted import get_custody, write_custody

    await write_custody(user, "stored", disclosure="2026-11-v1")
    await _store(user)
    with patch.dict(os.environ, {"BYOK_ENCRYPTION_KEY": "another-byok-encryption-key-32ch"}):
        assert await write_custody(user, "local") is None
    async with patched_get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT count(*) AS n FROM user_oauth_tokens WHERE user_id = %s", (user,))
        assert (await cur.fetchone())["n"] == 0
    assert (await get_custody(user)).mode == "local"


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


async def test_only_the_users_runs_in_progress_on_the_stored_token_are_found(
    user, seed_workspace, patched_get_db_connection,
):
    from src.server.database.claude_code_hosted import STORED_TURN, WORKER_TURN, live_stored_turns

    threads = [str(uuid.uuid4()) for _ in range(3)]
    async with patched_get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        for index, (thread, (owner, metadata)) in enumerate(zip(threads, [
            (user, {**WORKER_TURN, **STORED_TURN}), (user, WORKER_TURN), ("someone-else", STORED_TURN),
        ])):
            await cur.execute(
                "INSERT INTO conversation_threads (conversation_thread_id, workspace_id, current_status, thread_index) "
                "VALUES (%s, %s, 'in_progress', %s)",
                (thread, seed_workspace["workspace_id"], index),
            )
            await cur.execute(
                "INSERT INTO conversation_responses (conversation_thread_id, turn_index, status, metadata, created_at, user_id) "
                "VALUES (%s, 0, 'in_progress', %s, NOW(), %s) RETURNING conversation_response_id",
                (thread, Jsonb(metadata), owner),
            )
            if index == 0:
                run_id = str((await cur.fetchone())["conversation_response_id"])
    assert await live_stored_turns(user) == [(threads[0], run_id)]


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
