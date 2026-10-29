"""075 against real PostgreSQL: the per-provider key, the seeded sequence, the way back.

Seeds a database at 074 the way a deployment that switched providers holds it:
one kind's rollout rows, computers of both kinds stamped with versions, and a
workspace shadow stamped above them all. Upgrades, registers a second kind
beside the first, downgrades (the documented rollback) and upgrades again.
"""

from __future__ import annotations

import asyncio
import tempfile
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

#: Its own database: this test downgrades the schema, which the shared
#: session-scoped one cannot survive.
SCRATCH_DB = "langalpha_migration_075"

USER = "user-075-a"
WORKSPACE = str(uuid.uuid4())
DAYTONA_COMPUTER = str(uuid.uuid4())
E2B_COMPUTER = str(uuid.uuid4())
# The highest version anywhere: a workspace shadow stamped by an earlier build.
SHADOW_VERSION = 15


def _uri(dbname: str) -> str:
    from tests.integration.conftest import _build_db_uri

    parts = urlparse(_build_db_uri())
    return urlunparse(parts._replace(path=f"/{dbname}"))


async def _alembic(uri: str, action: str, revision: str) -> None:
    """Threaded, as the suite's upgrade helper is: a migration calls
    ``asyncio.run`` internally."""
    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parent.parent.parent
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option(
        "sqlalchemy.url", uri.replace("postgresql://", "postgresql+psycopg://", 1)
    )
    run = {"upgrade": command.upgrade, "downgrade": command.downgrade}[action]
    await asyncio.to_thread(run, cfg, revision)


async def _execute(uri: str, statements: list[tuple[str, tuple]]) -> None:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        for sql, params in statements:
            await conn.execute(sql, params)


async def _fetch(uri: str, sql: str, params: tuple = ()) -> list[tuple]:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


def _rollout(secret_key: str, provider: str, generation: int) -> tuple[str, tuple]:
    return (
        "INSERT INTO platform_secret_rollouts (secret_key, provider, secret_name,"
        " provider_secret_id, placeholder_sha256, current_credential_sha256,"
        " generation) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (secret_key, provider, f"ns-{secret_key.lower()}", f"{provider}-id",
         "a" * 64, "b" * 64, generation),
    )


def _seed() -> list[tuple[str, tuple]]:
    computer = (
        "INSERT INTO computers (computer_id, user_id, kind, provider_ref, status,"
        " platform_secret_version) VALUES (%s, %s, %s, %s, 'running', %s)"
    )
    return [
        ("INSERT INTO users (user_id, email) VALUES (%s, %s)", (USER, "a@example.com")),
        (
            "INSERT INTO workspaces (workspace_id, user_id, name, name_key, status,"
            " platform_secret_version)"
            " VALUES (%s, %s, 'Research', 'research', 'running', %s)",
            (WORKSPACE, USER, SHADOW_VERSION),
        ),
        _rollout("FMP_API_KEY", "daytona", 7),
        _rollout("EXTRA_API_KEY", "daytona", 7),
        (computer, (DAYTONA_COMPUTER, USER, "daytona", "dtn-1", 7)),
        # Kept from before a switch, stamped under an earlier identity.
        (computer, (E2B_COMPUTER, USER, "e2b", "e2b-1", 12)),
    ]


async def _read(uri: str) -> dict[str, Any]:
    pk = await _fetch(
        uri,
        "SELECT a.attname FROM pg_index i"
        " JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)"
        " WHERE i.indrelid = 'platform_secret_rollouts'::regclass AND i.indisprimary"
        " ORDER BY array_position(i.indkey::int2[], a.attnum)",
    )
    sequence = (await _fetch(
        uri, "SELECT to_regclass('platform_secret_generation_seq') IS NOT NULL",
    ))[0][0]
    state: dict[str, Any] = {
        "pk": [row[0] for row in pk],
        "sequence": None,
        "rows": sorted(await _fetch(
            uri,
            "SELECT provider, secret_key, generation, provider_secret_id"
            " FROM platform_secret_rollouts",
        )),
        "versions": dict(await _fetch(
            uri, "SELECT computer_id::text, platform_secret_version FROM computers",
        )),
    }
    if sequence:
        # Read without consuming: (next value, whether it was already handed out).
        state["sequence"] = (await _fetch(
            uri, "SELECT last_value, is_called FROM platform_secret_generation_seq",
        ))[0]
    return state


@pytest_asyncio.fixture(scope="module")
async def run() -> dict[str, Any]:
    admin, uri = _uri("postgres"), _uri(SCRATCH_DB)

    async def _recreate(create: bool) -> None:
        async with await psycopg.AsyncConnection.connect(admin, autocommit=True) as c:
            await c.execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}"')
            if create:
                await c.execute(f'CREATE DATABASE "{SCRATCH_DB}"')

    await _recreate(create=True)
    out: dict[str, Any] = {}
    try:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            # Earlier migrations read the bundled servers; keep them as shipped.
            config = Path(tmp) / "agent_config.yaml"
            config.write_text("{}\n")
            mp.setenv("PTC_CONFIG_FILE", str(config))
            await _alembic(uri, "upgrade", "074")
            await _execute(uri, _seed())
            out["before"] = await _read(uri)

            await _alembic(uri, "upgrade", "075")
            out["up"] = await _read(uri)

            # What a second kind's registration writes: the same secret key
            # beside the first kind's, at the generation the sequence issues.
            generation = (await _fetch(
                uri, "SELECT nextval('platform_secret_generation_seq')",
            ))[0][0]
            out["issued"] = generation
            await _execute(uri, [_rollout("FMP_API_KEY", "e2b", generation)])
            out["both"] = await _read(uri)

            await _alembic(uri, "downgrade", "074")
            out["down"] = await _read(uri)

            await _alembic(uri, "upgrade", "075")
            out["again"] = await _read(uri)
        yield out
    finally:
        await _recreate(create=False)


class TestUpgrade:
    async def test_rollouts_are_keyed_by_provider_and_secret(self, run):
        assert run["before"]["pk"] == ["secret_key"]
        assert run["up"]["pk"] == ["provider", "secret_key"]

    async def test_the_sequence_starts_above_every_generation_and_version(self, run):
        assert run["up"]["sequence"] == (SHADOW_VERSION + 1, False)
        assert run["issued"] == SHADOW_VERSION + 1

    async def test_no_row_or_stamped_version_changes(self, run):
        assert run["up"]["rows"] == run["before"]["rows"]
        assert run["up"]["versions"] == run["before"]["versions"]

    async def test_a_second_kind_registers_the_same_secret_beside_the_first(self, run):
        assert [row[:3] for row in run["both"]["rows"]] == [
            ("daytona", "EXTRA_API_KEY", 7),
            ("daytona", "FMP_API_KEY", 7),
            ("e2b", "FMP_API_KEY", SHADOW_VERSION + 1),
        ]


class TestRollback:
    async def test_the_downgrade_keeps_the_kind_registered_last(self, run):
        down = run["down"]
        assert down["pk"] == ["secret_key"]
        assert down["sequence"] is None
        assert [row[:3] for row in down["rows"]] == [
            ("e2b", "FMP_API_KEY", SHADOW_VERSION + 1),
        ]
        assert down["versions"] == run["before"]["versions"]

    async def test_upgrading_again_reseeds_above_what_was_issued(self, run):
        again = run["again"]
        assert again["pk"] == ["provider", "secret_key"]
        assert again["sequence"] == (SHADOW_VERSION + 2, False)
        assert again["rows"] == run["down"]["rows"]
