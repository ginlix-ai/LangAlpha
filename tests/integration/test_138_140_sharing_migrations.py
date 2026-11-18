"""138 to 140 against real PostgreSQL: members-only links and the library.

Seeds a database at 137 with a shared file link, upgrades to 140 and checks
what each migration adds: the link reads as public (NULL audience), the two
member grant keys exist, and the library takes thread, file and skill items
under its shape check. Then the documented rollback to 137, which removes
all of it and leaves the link as it was, and the upgrade again.
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

SCRATCH_DB = "langalpha_migration_126"
KEYS = ("member_share_grant", "artifact_member_grant")


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


async def _columns(c, table: str) -> set[str]:
    rows = await (
        await c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table,),
        )
    ).fetchall()
    return {r[0] for r in rows}


async def _tables(c) -> set[str]:
    rows = await (
        await c.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    ).fetchall()
    return {r[0] for r in rows}


async def _keys(c) -> set[str]:
    rows = await (
        await c.execute("SELECT name FROM server_keys WHERE name = ANY(%s)", (list(KEYS),))
    ).fetchall()
    return {r[0] for r in rows}


async def test_138_to_140_up_down_up(scratch):
    await _alembic(scratch, "upgrade", "137")
    ws, thread, artifact = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        await c.execute("INSERT INTO users (user_id, email) VALUES ('u-137', 'u@example.com')")
        await c.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, 'u-137', 'W', 'running')",
            (ws,),
        )
        await c.execute(
            "INSERT INTO conversation_threads (conversation_thread_id, workspace_id, current_status, thread_index)"
            " VALUES (%s, %s, 'completed', 0)",
            (thread, ws),
        )
        await c.execute(
            "INSERT INTO share_links (code, workspace_id, kind, path, shared_at, files, artifact_id)"
            " VALUES ('c126aaaaaaaa', %s, 'file', 'r.html', now(), ARRAY['r.html'], %s)",
            (ws, artifact),
        )

    await _alembic(scratch, "upgrade", "140")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert (await (await c.execute("SELECT audience FROM share_links")).fetchone()) == (None,)
        assert await _keys(c) == set(KEYS)
        with pytest.raises(psycopg.errors.CheckViolation):
            await c.execute("UPDATE share_links SET audience = 'everyone'")
        await c.execute("UPDATE share_links SET audience = 'organization'")
        await c.execute(
            "INSERT INTO org_library_items (org_id, kind, title, publisher_id, thread_id)"
            " VALUES ('o', 'thread', 'T', 'u-137', %s)",
            (thread,),
        )
        await c.execute(
            "INSERT INTO org_library_items (org_id, kind, title, publisher_id, artifact_id)"
            " VALUES ('o', 'file', 'F', 'u-137', %s)",
            (artifact,),
        )
        await c.execute(
            "INSERT INTO org_library_items (org_id, kind, title, publisher_id, skill_name,"
            " content_hash, archive_blob, archive_bytes)"
            " VALUES ('o', 'skill', 'S', 'u-137', 'dcf', 'sha256:x', 'zip', 3)"
        )
        # A kind's reference must match it: a thread item naming a file is refused.
        with pytest.raises(psycopg.errors.CheckViolation):
            await c.execute(
                "INSERT INTO org_library_items (org_id, kind, title, publisher_id, thread_id, artifact_id)"
                " VALUES ('o', 'thread', 'X', 'u-137', %s, %s)",
                (thread, artifact),
            )
        assert "org_library_item_id" in await _columns(c, "user_skills")

    await _alembic(scratch, "downgrade", "137")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "org_library_items" not in await _tables(c)
        assert "audience" not in await _columns(c, "share_links")
        assert "org_library_item_id" not in await _columns(c, "user_skills")
        assert await _keys(c) == set()
        row = await (await c.execute("SELECT code, shared_at IS NOT NULL FROM share_links")).fetchone()
        assert row == ("c126aaaaaaaa", True)

    await _alembic(scratch, "upgrade", "140")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "org_library_items" in await _tables(c)
        assert await _keys(c) == set(KEYS)
