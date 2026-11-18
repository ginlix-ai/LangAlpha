"""141 against real PostgreSQL: shared workspaces.

Seeds a database at 140 with a person's workspace and thread, upgrades to 141
and checks what it adds: the two tables under their checks, a NULL
``created_by`` on the existing thread, and the member grant key. The
documented rollback refuses while a shared workspace is live, goes through
once it is deleted, and removes all of it; the upgrade then runs again.
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

import psycopg
import pytest
import pytest_asyncio

from tests.integration.test_117_legacy_annotations_to_layers_migration import (
    _alembic,
    _uri,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

SCRATCH_DB = "langalpha_migration_129"
KEY = "shared_file_grant"
HOLDER = "shared-00000000-0000-4000-8000-000000000130"


@pytest_asyncio.fixture
async def scratch():
    admin = _uri("postgres")

    async def _recreate(create: bool) -> None:
        async with await psycopg.AsyncConnection.connect(admin, autocommit=True) as c:
            await c.execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}"')
            if create:
                await c.execute(f'CREATE DATABASE "{SCRATCH_DB}"')

    await _recreate(create=True)
    try:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "agent_config.yaml"
            config.write_text("{}\n")
            mp.setenv("PTC_CONFIG_FILE", str(config))
            yield _uri(SCRATCH_DB)
    finally:
        await _recreate(create=False)


async def _tables(c) -> set[str]:
    rows = await (
        await c.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    ).fetchall()
    return {r[0] for r in rows}


async def _has_created_by(c) -> bool:
    row = await (
        await c.execute(
            "SELECT 1 FROM information_schema.columns"
            " WHERE table_name = 'conversation_threads' AND column_name = 'created_by'"
        )
    ).fetchone()
    return row is not None


async def _key(c) -> bool:
    row = await (await c.execute("SELECT 1 FROM server_keys WHERE name = %s", (KEY,))).fetchone()
    return row is not None


async def test_141_up_down_up(scratch):
    await _alembic(scratch, "upgrade", "140")
    ws, thread, shared_ws = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        await c.execute("INSERT INTO users (user_id, email) VALUES ('u-140', 'u@example.com')")
        await c.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, 'u-140', 'W', 'running')",
            (ws,),
        )
        await c.execute(
            "INSERT INTO conversation_threads (conversation_thread_id, workspace_id, current_status, thread_index)"
            " VALUES (%s, %s, 'completed', 0)",
            (thread, ws),
        )

    await _alembic(scratch, "upgrade", "141")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert {"shared_workspaces", "shared_workspace_members"} <= await _tables(c)
        assert await _key(c)
        # Nothing existing changes: the person's thread names no starter.
        row = await (await c.execute("SELECT created_by FROM conversation_threads")).fetchone()
        assert row == (None,)
        await c.execute(
            "INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (HOLDER,)
        )
        await c.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, 'Desk', 'stopped')",
            (shared_ws, HOLDER),
        )
        # A holder is a shared- id, never a person's.
        with pytest.raises(psycopg.errors.CheckViolation):
            await c.execute(
                "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, created_by)"
                " VALUES (%s, 'u-140', 'org', 'u-140')",
                (shared_ws,),
            )
        await c.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, created_by)"
            " VALUES (%s, %s, 'org', 'u-140')",
            (shared_ws, HOLDER),
        )
        with pytest.raises(psycopg.errors.CheckViolation):
            await c.execute(
                "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)"
                " VALUES (%s, 'u-140', 'owner', 'u-140')",
                (shared_ws,),
            )
        await c.execute(
            "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)"
            " VALUES (%s, 'u-140', 'manager', 'u-140')",
            (shared_ws,),
        )

    # A live shared workspace keeps the rollback from orphaning its rows.
    with pytest.raises(Exception, match="shared workspaces exist"):
        await _alembic(scratch, "downgrade", "140")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "shared_workspaces" in await _tables(c)
        await c.execute("UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s", (shared_ws,))

    await _alembic(scratch, "downgrade", "140")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert not {"shared_workspaces", "shared_workspace_members"} & await _tables(c)
        assert not await _has_created_by(c)
        assert not await _key(c)
        row = await (await c.execute("SELECT count(*) FROM conversation_threads")).fetchone()
        assert row == (1,)

    await _alembic(scratch, "upgrade", "141")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert {"shared_workspaces", "shared_workspace_members"} <= await _tables(c)
        assert await _has_created_by(c) and await _key(c)
