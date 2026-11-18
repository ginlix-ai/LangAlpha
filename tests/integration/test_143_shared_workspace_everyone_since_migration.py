"""143 against real PostgreSQL: when a shared workspace opened to everyone.

Seeds a database at 142 with one shared workspace open to everyone and one
that is not, upgrades to 143 and checks the new column is filled from the
creation time for the first only. The documented rollback drops the column and
keeps the rows; the upgrade then runs again.
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


async def test_143_up_down_up(scratch):  # noqa: F811
    await _alembic(scratch, "upgrade", "142")
    open_ws, closed_ws = uuid.uuid4(), uuid.uuid4()
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        await c.execute("INSERT INTO users (user_id) VALUES ('u-143')")
        for i, (ws, everyone) in enumerate(((open_ws, True), (closed_ws, False))):
            holder = f"shared-{ws}"
            await c.execute(
                "INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (holder,)
            )
            await c.execute(
                "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, %s, 'running')",
                (ws, holder, f"W{i}"),
            )
            await c.execute(
                "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, everyone, created_by, created_at)"
                " VALUES (%s, %s, 'org', %s, 'u-143', NOW() - INTERVAL '1 day')",
                (ws, holder, everyone),
            )

    await _alembic(scratch, "upgrade", "143")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "everyone_since" in await _columns(c)
        rows = dict(
            await (
                await c.execute(
                    "SELECT workspace_id, everyone_since = created_at FROM shared_workspaces"
                )
            ).fetchall()
        )
        assert rows == {open_ws: True, closed_ws: None}

    await _alembic(scratch, "downgrade", "142")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "everyone_since" not in await _columns(c)
        n = await (await c.execute("SELECT count(*) FROM shared_workspaces")).fetchone()
        assert n == (2,)

    await _alembic(scratch, "upgrade", "143")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert "everyone_since" in await _columns(c)
