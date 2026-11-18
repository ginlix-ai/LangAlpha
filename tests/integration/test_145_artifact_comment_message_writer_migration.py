"""145 against real PostgreSQL: which member wrote each comment message.

Upgrades to 145 and checks the column is nullable and lets go of a removed
account. The documented rollback drops the column; the upgrade then runs
again.
"""

from __future__ import annotations

import psycopg
import pytest

from tests.integration.test_117_legacy_annotations_to_layers_migration import _alembic
from tests.integration.test_141_shared_workspaces_migration import scratch  # noqa: F401

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_COLUMN = (
    "SELECT is_nullable FROM information_schema.columns"
    " WHERE table_name = 'artifact_comment_messages' AND column_name = 'written_by'"
)
_ON_DELETE = (
    "SELECT confdeltype FROM pg_constraint"
    " WHERE conrelid = 'artifact_comment_messages'::regclass AND contype = 'f'"
    " AND confrelid = 'users'::regclass"
)


async def test_145_up_down_up(scratch):  # noqa: F811
    await _alembic(scratch, "upgrade", "145")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await (await c.execute(_COLUMN)).fetchone() == ("YES",)
        assert await (await c.execute(_ON_DELETE)).fetchone() == ("n",)

    await _alembic(scratch, "downgrade", "144")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await (await c.execute(_COLUMN)).fetchone() is None

    await _alembic(scratch, "upgrade", "145")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as c:
        assert await (await c.execute(_COLUMN)).fetchone() == ("YES",)
