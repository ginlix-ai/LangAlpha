"""149 against real PostgreSQL: a shared workspace's managers may keep
members' own context out.

Seeds a database at 148 with a shared workspace. After 149 it follows the
organization's grant (``member_context`` true), the column takes false and
refuses null, and the memory alias key is minted. The documented rollback
drops the column and the key; the upgrade then runs again.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest

from tests.integration.test_117_legacy_annotations_to_layers_migration import _alembic
from tests.integration.test_141_shared_workspaces_migration import scratch  # noqa: F401

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _has_column(c) -> bool:
    row = await (
        await c.execute(
            "SELECT count(*) FROM information_schema.columns"
            " WHERE table_name = 'shared_workspaces' AND column_name = 'member_context'"
        )
    ).fetchone()
    return row[0] == 1


async def _alias_keys(c) -> int:
    row = await (
        await c.execute("SELECT count(*) FROM server_keys WHERE name = 'member_memory_alias'")
    ).fetchone()
    return row[0]


async def test_149_up_down_up(scratch):  # noqa: F811
    await _alembic(scratch, "upgrade", "148")
    ws = uuid.uuid4()
    holder = f"shared-{ws}"
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        await c.execute("INSERT INTO users (user_id) VALUES ('u-149')")
        await c.execute("INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (holder,))
        await c.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, 'Desk', 'running')",
            (ws, holder),
        )
        await c.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, everyone, created_by)"
            " VALUES (%s, %s, 'org', false, 'u-149')",
            (ws, holder),
        )

    await _alembic(scratch, "upgrade", "149")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _has_column(c)
        assert await _alias_keys(c) == 1
        row = await (await c.execute("SELECT member_context FROM shared_workspaces")).fetchone()
        assert row == (True,)
        await c.execute("UPDATE shared_workspaces SET member_context = false")
        with pytest.raises(psycopg.errors.NotNullViolation):
            await c.execute("UPDATE shared_workspaces SET member_context = NULL")

    await _alembic(scratch, "downgrade", "148")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert not await _has_column(c)
        # The key outlives a downgrade, so a re-upgrade aliases as before.
        key = (await (await c.execute("SELECT secret FROM server_keys WHERE name = 'member_memory_alias'")).fetchone())[0]
        assert (await (await c.execute("SELECT count(*) FROM shared_workspaces")).fetchone())[0] == 1

    await _alembic(scratch, "upgrade", "149")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _has_column(c)
        assert await _alias_keys(c) == 1
        again = (await (await c.execute("SELECT secret FROM server_keys WHERE name = 'member_memory_alias'")).fetchone())[0]
        assert again == key
