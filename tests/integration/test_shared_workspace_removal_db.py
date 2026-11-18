"""What someone who loses a shared workspace leaves running there, real
PostgreSQL: the computer's market-data token minted on their seat, and their
background commands."""

from __future__ import annotations

import uuid
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

A = "sw-removal-a"
B = "sw-removal-b"
HOLDER = "shared-00000000-0000-4000-8000-0000000000bb"


async def _seed(conn) -> dict:
    ws, computer, thread = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with conn.cursor() as cur:
        for uid in (A, B):
            await cur.execute("INSERT INTO users (user_id, name) VALUES (%s, %s)", (uid, uid))
        await cur.execute(
            "INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (HOLDER,)
        )
        await cur.execute(
            "INSERT INTO computers (computer_id, user_id, status, provider_ref)"
            " VALUES (%s, %s, 'running', 'sb-1')",
            (computer, HOLDER),
        )
        await cur.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status, computer_id)"
            " VALUES (%s, %s, 'Desk', 'running', %s)",
            (ws, HOLDER, computer),
        )
        await cur.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, created_by)"
            " VALUES (%s, %s, 'org', %s)",
            (ws, HOLDER, A),
        )
        for uid, role in ((A, "manager"), (B, "member")):
            await cur.execute(
                "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)"
                " VALUES (%s, %s, %s, %s)",
                (ws, uid, role, A),
            )
        await cur.execute(
            "INSERT INTO conversation_threads"
            " (conversation_thread_id, workspace_id, current_status, thread_index, created_by)"
            " VALUES (%s, %s, 'completed', 0, %s)",
            (thread, ws, B),
        )
        await cur.execute(
            "INSERT INTO computer_data_tokens"
            " (computer_id, user_id, expires_at, tokens_until, held_by, generation)"
            " VALUES (%s, %s, NOW() + interval '1 hour', NOW() + interval '1 hour', 'sb-1', 3)",
            (computer, B),
        )
        for command_id, uid in (("bg-00000000000b", B), ("bg-00000000000a", A)):
            await cur.execute(
                "INSERT INTO command_runs (command_id, conversation_thread_id, user_id,"
                " workspace_id, computer_id, tool, log_path, job_ref, started_at, next_check_at)"
                " VALUES (%s, %s, %s, %s, %s, 'Bash', 'x.log', '{}', NOW(), NOW())",
                (command_id, thread, uid, ws, computer),
            )
    return {"ws": str(ws), "computer": str(computer)}


async def _one(conn, sql: str, *args):
    async with conn.cursor() as cur:
        await cur.execute(sql, args)
        row = await cur.fetchone()
    return next(iter(row.values())) if isinstance(row, dict) else row[0]


@pytest.fixture
def platform():
    """The data-token service configured, its revocation confirmed."""
    from src.server.services import data_tokens

    sent = AsyncMock(return_value="revoked")
    with patch.object(data_tokens, "configured", return_value=True), patch.object(
        data_tokens, "_revoke_upstream", sent
    ):
        yield sent


async def test_a_removed_members_token_and_commands_end_with_their_access(
    patched_get_db_connection, cleanup_tables, platform
):
    from src.server.database import computer_data_tokens
    from src.server.services.shared_workspaces.service import end_access

    async with patched_get_db_connection() as conn:
        seeded = await _seed(conn)

    with patch(
        "src.server.services.cancel_dispatch.cancel_workflow", AsyncMock()
    ), patch("src.server.services.preview_capabilities.revoke", AsyncMock()):
        await end_access(seeded["ws"], HOLDER, B)

    # The token minted on B's seat is revoked, and the sweep no longer
    # refreshes as B: the row names the holder, for whom nothing mints.
    platform.assert_awaited_once_with(B, seeded["computer"])
    row = await computer_data_tokens.load(seeded["computer"])
    assert row.user_id == HOLDER and row.expires_at is None and row.held_by is None
    due = await computer_data_tokens.claim_due(
        refresh_below=timedelta(minutes=30),
        retry_after=timedelta(seconds=0),
        limit=10,
    )
    assert all(r["user_id"] != B for r in due if r["computer_id"] == seeded["computer"])

    async with patched_get_db_connection() as conn:
        # B's command is marked to stop, as a suspension marks one; A's runs on.
        assert await _one(conn, "SELECT stopped_by FROM command_runs WHERE command_id = 'bg-00000000000b'") == "removed"
        assert await _one(conn, "SELECT stopped_by FROM command_runs WHERE command_id = 'bg-00000000000a'") is None


async def test_a_token_another_member_minted_is_left_alone(
    patched_get_db_connection, cleanup_tables, platform
):
    from src.server.database import computer_data_tokens
    from src.server.services.shared_workspaces.turns import revoke_former_members_tokens

    async with patched_get_db_connection() as conn:
        seeded = await _seed(conn)

    await revoke_former_members_tokens(seeded["ws"], HOLDER, A)

    platform.assert_not_awaited()
    assert (await computer_data_tokens.load(seeded["computer"])).user_id == B
