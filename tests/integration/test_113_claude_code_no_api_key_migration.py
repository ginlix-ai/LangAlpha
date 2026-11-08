"""113 against real PostgreSQL: a user who chose the API key reads as local, and the way back.

Seeds a database at 112 with one user per choice, the API key's among them
under an operator's block, upgrades it and reads back each row; then a write
of the API key is refused. The documented rollback allows it again.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

#: Its own database: this test downgrades the schema, which the shared
#: session-scoped one cannot survive.
SCRATCH_DB = "langalpha_migration_113"

CHOICES = {"user-113-key": "api_key", "user-113-blocked": "api_key", "user-113-stored": "stored", "user-113-local": "local"}


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
    cfg.set_main_option("sqlalchemy.url", uri.replace("postgresql://", "postgresql+psycopg://", 1))
    await asyncio.to_thread({"upgrade": command.upgrade, "downgrade": command.downgrade}[action], cfg, revision)


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
            # Earlier migrations read the bundled servers; keep them as shipped.
            config = Path(tmp) / "agent_config.yaml"
            config.write_text("{}\n")
            mp.setenv("PTC_CONFIG_FILE", str(config))
            yield _uri(SCRATCH_DB)
    finally:
        await _recreate(create=False)


async def _modes(uri: str) -> dict[str, tuple[str, bool]]:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        cur = await conn.execute("SELECT user_id, mode, blocked_at IS NOT NULL FROM claude_code_custody")
        return {user: (mode, blocked) for user, mode, blocked in await cur.fetchall()}


async def _choose(uri: str, user: str, mode: str) -> None:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        await conn.execute("UPDATE claude_code_custody SET mode = %s WHERE user_id = %s", (mode, user))


async def test_the_api_key_choice_reads_as_local_and_cannot_be_written_until_rolled_back(scratch):
    await _alembic(scratch, "upgrade", "112")
    async with await psycopg.AsyncConnection.connect(scratch, autocommit=True) as conn:
        for user, mode in CHOICES.items():
            await conn.execute("INSERT INTO users (user_id, email) VALUES (%s, %s)", (user, f"{user}@example.com"))
            await conn.execute(
                "INSERT INTO claude_code_custody (user_id, mode, blocked_at, blocked_reason) VALUES (%s, %s, %s, %s)",
                (user, mode, *(("2026-11-01T00:00:00+00:00", "terms") if user == "user-113-blocked" else (None, None))),
            )

    await _alembic(scratch, "upgrade", "113")
    assert await _modes(scratch) == {
        "user-113-key": ("local", False), "user-113-blocked": ("local", True),
        "user-113-stored": ("stored", False), "user-113-local": ("local", False),
    }
    with pytest.raises(psycopg.errors.CheckViolation):
        await _choose(scratch, "user-113-local", "api_key")

    await _alembic(scratch, "downgrade", "112")
    await _choose(scratch, "user-113-local", "api_key")
    assert (await _modes(scratch))["user-113-local"] == ("api_key", False)
    await _choose(scratch, "user-113-local", "local")
    await _alembic(scratch, "upgrade", "113")
    assert (await _modes(scratch))["user-113-stored"] == ("stored", False)
