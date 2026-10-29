"""059 against real PostgreSQL: which threads its backfill seeds with a model.

The suite's own database is upgraded while empty, so the seed never ran on
rows. This lays out turn histories at 058 the way the run lifecycle writes
them (one START stamping a turn's query and first attempt alike, a retry
chaining a later attempt, a regenerate replacing the attempt under a kept
query), upgrades, and reads back each thread's model.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

SCRATCH_DB = "langalpha_migration_059"
USER = "user-059"
WORKSPACE = str(uuid.uuid4())
A, C = "model-a", "model-c"


def _uri(dbname: str) -> str:
    from tests.integration.conftest import _build_db_uri

    parts = urlparse(_build_db_uri())
    return urlunparse(parts._replace(path=f"/{dbname}"))


async def _upgrade(uri: str, revision: str) -> None:
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
    await asyncio.to_thread(command.upgrade, cfg, revision)


async def _execute(uri: str, statements: list[tuple[str, tuple]]) -> None:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        for sql, params in statements:
            await conn.execute(sql, params)


async def _fetch(uri: str, sql: str, params: tuple = ()) -> list[tuple]:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


def _at(minute: int) -> str:
    # Microseconds, as START's Python timestamp carries them.
    return f"2026-08-01T10:{minute:02d}:00.123456+00:00"


class _Seed:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple]] = [
            (
                "INSERT INTO users (user_id, email) VALUES (%s, %s)",
                (USER, "user-059@example.com"),
            ),
            (
                "INSERT INTO workspaces (workspace_id, user_id, name, name_key, status) "
                "VALUES (%s, %s, 'W', 'w', 'running')",
                (WORKSPACE, USER),
            ),
        ]
        self.threads: dict[str, str] = {}

    def thread(self, label: str, platform: str = "web") -> str:
        thread_id = str(uuid.uuid4())
        self.threads[label] = thread_id
        self.statements.append(
            (
                "INSERT INTO conversation_threads "
                "(conversation_thread_id, workspace_id, msg_type, current_status, "
                "thread_index, platform) "
                "VALUES (%s, %s, 'ptc', 'completed', %s, %s)",
                (thread_id, WORKSPACE, len(self.threads), platform),
            )
        )
        return thread_id

    def query(self, thread_id: str, turn: int, model: str, at: str) -> None:
        self.statements.append(
            (
                "INSERT INTO conversation_queries "
                "(conversation_thread_id, turn_index, content, type, metadata, created_at) "
                "VALUES (%s, %s, 'hello', 'initial', %s::jsonb, %s)",
                (thread_id, turn, json.dumps({"llm_model": model}), at),
            )
        )

    def attempt(
        self,
        thread_id: str,
        turn: int,
        at: str,
        status: str,
        *,
        attempt_no: int = 1,
        retry_of: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Born in_progress and settled after, as the lifecycle guard requires."""
        run_id = str(uuid.uuid4())
        if metadata is None:
            metadata = {"msg_type": "ptc", "user_id": USER, "burst_slot_id": "slot"}
        self.statements += [
            (
                "INSERT INTO conversation_responses "
                "(conversation_response_id, conversation_thread_id, turn_index, "
                "status, metadata, created_at, attempt_no, retry_of_run_id) "
                "VALUES (%s, %s, %s, 'in_progress', %s::jsonb, %s, %s, %s)",
                (
                    run_id,
                    thread_id,
                    turn,
                    json.dumps(metadata),
                    at,
                    attempt_no,
                    retry_of,
                ),
            ),
            (
                "UPDATE conversation_responses SET status = %s "
                "WHERE conversation_response_id = %s",
                (status, run_id),
            ),
        ]
        return run_id

    def turn(self, thread_id: str, turn: int, model: str, at: str) -> None:
        """One START: the query and its first attempt share a timestamp."""
        self.query(thread_id, turn, model, at)
        self.attempt(thread_id, turn, at, "completed")

    def retried(
        self, thread_id: str, turn: int, model: str, at: str, later: str
    ) -> None:
        self.query(thread_id, turn, model, at)
        failed = self.attempt(thread_id, turn, at, "error")
        self.attempt(thread_id, turn, later, "completed", attempt_no=2, retry_of=failed)


def _seed() -> _Seed:
    seed = _Seed()
    seed.turn(seed.thread("plain"), 0, A, _at(0))

    seed.retried(seed.thread("retried"), 0, A, _at(0), _at(3))

    # The regenerate truncated the first attempt and started its own; the
    # marker is present even when no burst slot was held.
    regenerated = seed.thread("regenerated")
    seed.query(regenerated, 0, A, _at(0))
    seed.attempt(
        regenerated,
        0,
        _at(5),
        "completed",
        metadata={"msg_type": "ptc", "user_id": USER, "burst_slot_id": None},
    )

    # Before START existed, the response was written when its turn ended.
    pre_v4 = seed.thread("pre_v4")
    seed.query(pre_v4, 0, A, _at(0))
    seed.attempt(pre_v4, 0, _at(2), "completed", metadata={"msg_type": "ptc"})

    fallback = seed.thread("older_turn_unreplayed")
    seed.turn(fallback, 0, C, _at(0))
    seed.retried(fallback, 1, A, _at(10), _at(12))

    seed.turn(seed.thread("market_view", platform="market_view"), 0, A, _at(0))
    seed.turn(
        seed.thread("market_view_symbol", platform="market_view:AAPL"), 0, A, _at(0)
    )
    return seed


@pytest_asyncio.fixture(scope="module")
async def models() -> AsyncIterator[dict[str, str | None]]:
    admin, uri = _uri("postgres"), _uri(SCRATCH_DB)

    async def _recreate(create: bool) -> None:
        async with await psycopg.AsyncConnection.connect(admin, autocommit=True) as c:
            await c.execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}"')
            if create:
                await c.execute(f'CREATE DATABASE "{SCRATCH_DB}"')

    await _recreate(create=True)
    try:
        await _upgrade(uri, "058")
        seed = _seed()
        await _execute(uri, seed.statements)
        await _upgrade(uri, "059")
        label = {v: k for k, v in seed.threads.items()}
        rows = await _fetch(
            uri,
            "SELECT conversation_thread_id::text, llm_model FROM conversation_threads",
        )
        yield {label[thread_id]: model for thread_id, model in rows}
    finally:
        await _recreate(create=False)


class TestSeed:
    async def test_a_turn_that_ran_once_seeds_its_model(self, models):
        assert models["plain"] == A

    async def test_a_retried_turn_leaves_the_thread_null(self, models):
        """The retry may have run another model; its query row still names
        the first attempt's."""
        assert models["retried"] is None

    async def test_a_regenerated_turn_leaves_the_thread_null(self, models):
        assert models["regenerated"] is None

    async def test_a_turn_from_before_start_still_seeds(self, models):
        """A response later than its query marks a rerun only since START
        stamped both; before it, every response was written at turn end."""
        assert models["pre_v4"] == A

    async def test_a_rerun_latest_turn_never_falls_back_to_an_older_one(self, models):
        assert models["older_turn_unreplayed"] is None

    async def test_market_view_threads_seed_with_or_without_a_symbol(self, models):
        assert models["market_view"] == A
        assert models["market_view_symbol"] == A
