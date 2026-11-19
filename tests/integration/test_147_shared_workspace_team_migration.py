"""147 against real PostgreSQL: a shared workspace may name one team.

Seeds a database at 146 with a shared workspace open to everyone. After 147
it names no team, a team and its time are written together or not at all,
and a workspace open to everyone cannot also name a team. The documented
rollback drops the columns, their checks and the index; the upgrade then
runs again.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest

from tests.integration.test_117_legacy_annotations_to_layers_migration import _alembic
from tests.integration.test_141_shared_workspaces_migration import scratch  # noqa: F401

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _shape(c) -> tuple[set[str], set[str], int]:
    cols = await (
        await c.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'shared_workspaces' AND column_name LIKE 'team%%'"
        )
    ).fetchall()
    checks = await (
        await c.execute("SELECT conname FROM pg_constraint WHERE conname LIKE 'shared_workspaces_team%%'")
    ).fetchall()
    index = await (
        await c.execute("SELECT count(*) FROM pg_indexes WHERE indexname = 'ix_shared_workspaces_team'")
    ).fetchone()
    return {r[0] for r in cols}, {r[0] for r in checks}, index[0]


async def test_147_up_down_up(scratch):  # noqa: F811
    await _alembic(scratch, "upgrade", "146")
    ws = uuid.uuid4()
    holder = f"shared-{ws}"
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        await c.execute("INSERT INTO users (user_id) VALUES ('u-147')")
        await c.execute("INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (holder,))
        await c.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, 'Desk', 'running')",
            (ws, holder),
        )
        await c.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, everyone, everyone_since, created_by)"
            " VALUES (%s, %s, 'org', true, now(), 'u-147')",
            (ws, holder),
        )

    await _alembic(scratch, "upgrade", "147")
    shape = (
        {"team_id", "team_since"},
        {"shared_workspaces_team_since", "shared_workspaces_team_or_everyone"},
        1,
    )
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _shape(c) == shape
        row = await (await c.execute("SELECT team_id, team_since, everyone FROM shared_workspaces")).fetchone()
        assert row == (None, None, True)
        with pytest.raises(psycopg.errors.CheckViolation):
            await c.execute("UPDATE shared_workspaces SET team_id = 't', team_since = now()")
        await c.execute("UPDATE shared_workspaces SET everyone = false, everyone_since = NULL")
        with pytest.raises(psycopg.errors.CheckViolation):
            await c.execute("UPDATE shared_workspaces SET team_id = 't'")
        await c.execute("UPDATE shared_workspaces SET team_id = 't', team_since = now()")

    await _alembic(scratch, "downgrade", "146")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _shape(c) == (set(), set(), 0)
        assert (await (await c.execute("SELECT count(*) FROM shared_workspaces")).fetchone())[0] == 1

    await _alembic(scratch, "upgrade", "147")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await _shape(c) == shape
