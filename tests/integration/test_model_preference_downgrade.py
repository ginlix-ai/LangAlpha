"""Rolling 034 back must not take the column's data with it.

The first cut of the downgrade carried keys back only for rows still holding one
of the keys 034 had moved. A row written by the per-model matrix holds none of
them, so it failed that filter, was never touched, and was dropped with the
column one statement later. The whole bug lived in two SQL predicates, so this
runs the real migration against real PostgreSQL: a fake would only prove that
the fake carries data back.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

#: Its own database, because this test downgrades the schema and the shared one
#: is session-scoped: every table the suite relies on would go with it.
SCRATCH_DB = "langalpha_migration_roundtrip"

#: The rows, keyed by what makes each one interesting. ``profiles`` and
#: ``prompt_guidance`` are the settings 034 did not lift out of the old column,
#: so a row holding only those is the shape the old filter passed over.
SEED = {
    "matrix-only": (
        {"profiles": {"m1": {"fast_mode": True}}},
        {"search_provider": "tavily"},
    ),
    "guidance-only": ({"prompt_guidance": "lean"}, {}),
    "matrix-and-guidance": (
        {"profiles": {"m1": {"fast_mode": True}}, "prompt_guidance": "lean"},
        {"search_depth": "basic"},
    ),
}


class RoundTrip(NamedTuple):
    seeded_at: dict[str, datetime]
    after_downgrade: dict[str, tuple[dict, datetime]]
    after_reupgrade: dict[str, tuple[dict, dict]]
    column: tuple[str, str]


def _uri(dbname: str) -> str:
    """The suite's own TEST_DB_* connection, pointed at another database."""
    from tests.integration.conftest import _build_db_uri

    parts = urlparse(_build_db_uri())
    return urlunparse(parts._replace(path=f"/{dbname}"))


async def _alembic(uri: str, action: str, revision: str) -> None:
    """Run alembic to ``revision``. Threaded for the same reason the suite's
    upgrade helper is: a migration calls ``asyncio.run`` internally."""
    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parent.parent.parent
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option(
        "sqlalchemy.url", uri.replace("postgresql://", "postgresql+psycopg://", 1)
    )
    run = command.upgrade if action == "upgrade" else command.downgrade
    await asyncio.to_thread(run, cfg, revision)


async def _rows(uri: str, columns: str) -> dict[str, Any]:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        cur = await conn.execute(
            f"SELECT user_id, {columns} FROM user_preferences ORDER BY user_id"
        )
        return {
            row[0]: row[1] if len(row) == 2 else row[1:] for row in await cur.fetchall()
        }


@pytest_asyncio.fixture(scope="module")
async def round_trip() -> RoundTrip:
    """Seed at 034, roll back to 033, roll forward again; snapshot each stop."""
    admin, uri = _uri("postgres"), _uri(SCRATCH_DB)

    async def _recreate(create: bool) -> None:
        async with await psycopg.AsyncConnection.connect(admin, autocommit=True) as c:
            await c.execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}"')
            if create:
                await c.execute(f'CREATE DATABASE "{SCRATCH_DB}"')

    await _recreate(create=True)
    try:
        await _alembic(uri, "upgrade", "034")

        async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
            for user_id, (model_pref, other_pref) in SEED.items():
                await conn.execute(
                    "INSERT INTO users (user_id, email) VALUES (%s, %s)",
                    (user_id, f"{user_id}@example.com"),
                )
                await conn.execute(
                    "INSERT INTO user_preferences (user_id, model_preference, other_preference)"
                    " VALUES (%s, %s::jsonb, %s::jsonb)",
                    (user_id, json.dumps(model_pref), json.dumps(other_pref)),
                )
        seeded_at = await _rows(uri, "updated_at")

        await _alembic(uri, "downgrade", "033")
        after_downgrade = await _rows(uri, "other_preference, updated_at")

        await _alembic(uri, "upgrade", "034")
        after_reupgrade = await _rows(uri, "model_preference, other_preference")

        async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
            cur = await conn.execute(
                "SELECT is_nullable, column_default FROM information_schema.columns"
                " WHERE table_name = 'user_preferences'"
                "   AND column_name = 'model_preference'"
            )
            column = await cur.fetchone()

        yield RoundTrip(seeded_at, after_downgrade, after_reupgrade, column)
    finally:
        await _recreate(create=False)


class TestDowngrade:
    async def test_a_matrix_only_row_is_carried_back_not_passed_over(self, round_trip):
        """The row the old filter skipped and the next statement then dropped.

        Its ``model_preference`` holds only ``profiles``, which the flat shape
        has nowhere to put, so the carry-back moves nothing for it and the stamp
        the statement writes is the only evidence it was in scope at all.
        """
        other_preference, updated_at = round_trip.after_downgrade["matrix-only"]

        assert other_preference == {"search_provider": "tavily"}
        assert updated_at > round_trip.seeded_at["matrix-only"]

    async def test_prompt_guidance_comes_back_and_goes_out_again(self, round_trip):
        """``profiles`` is the only setting a rollback is allowed to cost."""
        for user_id in ("guidance-only", "matrix-and-guidance"):
            assert round_trip.after_downgrade[user_id][0]["prompt_guidance"] == "lean"

            model_preference, _ = round_trip.after_reupgrade[user_id]
            assert model_preference["prompt_guidance"] == "lean"
            assert "profiles" not in model_preference

        assert round_trip.after_reupgrade["matrix-and-guidance"][1] == {
            "search_depth": "basic"
        }

    async def test_the_column_comes_back_not_null_with_a_default(self, round_trip):
        """A NULL here is an AttributeError at every read site that chains
        ``.get`` off it, so the column is never allowed to hold one."""
        assert round_trip.column == ("NO", "'{}'::jsonb")
