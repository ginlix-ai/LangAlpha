"""144 against real PostgreSQL: who may share a shared workspace's work outside it.

Seeds a database at 143 with a shared workspace, upgrades to 144 and checks
the row reads ``managers`` and the column refuses any other value. The
documented rollback drops the column and keeps the row; the upgrade then runs
again.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest

from tests.integration.test_117_legacy_annotations_to_layers_migration import _alembic
from tests.integration.test_141_shared_workspaces_migration import scratch  # noqa: F401

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _columns(c) -> set[str]:
    rows = await (
        await c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'shared_workspaces'"
        )
    ).fetchall()
    return {r[0] for r in rows}


async def test_144_up_down_up(scratch):  # noqa: F811
    await _alembic(scratch, "upgrade", "143")
    ws = uuid.uuid4()
    holder = f"shared-{ws}"
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        await c.execute("INSERT INTO users (user_id) VALUES ('u-144')")
        await c.execute("INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (holder,))
        await c.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, 'W', 'running')",
            (ws, holder),
        )
        await c.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, everyone, created_by)"
            " VALUES (%s, %s, 'org', false, 'u-144')",
            (ws, holder),
        )

    await _alembic(scratch, "upgrade", "144")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        row = await (await c.execute("SELECT share_out FROM shared_workspaces")).fetchone()
        assert row == ("managers",)
        for value in ("members", "none", "managers"):
            await c.execute("UPDATE shared_workspaces SET share_out = %s", (value,))
        with pytest.raises(psycopg.errors.CheckViolation):
            await c.execute("UPDATE shared_workspaces SET share_out = 'anyone'")

    await _alembic(scratch, "downgrade", "143")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "share_out" not in await _columns(c)
        assert await (await c.execute("SELECT count(*) FROM shared_workspaces")).fetchone() == (1,)

    await _alembic(scratch, "upgrade", "144")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "share_out" in await _columns(c)
