"""114/115 against real PostgreSQL: the bots backfill and the Home repair.

The suite upgrades an empty database, so 114's backfill never sees rows. This
lays out users/workspaces/threads/automations at 111, upgrades, and checks
what the bots layer made of them. It also simulates the first cut of 114 (a
Home analyst bot with bound thread/automation and a shared thread mapped as a
bot's private one) and proves 115 repairs it.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

SCRATCH_DB = "langalpha_migration_114"
USER = "user-114"
HOME = str(uuid.uuid5(uuid.UUID("f1a50000-0000-5000-e000-f1a500000000"), USER))
WS = str(uuid.uuid4())
HOME_THREAD = str(uuid.uuid4())
WS_THREAD = str(uuid.uuid4())
HOME_AUTOMATION = str(uuid.uuid4())
WS_AUTOMATION = str(uuid.uuid4())
OLD_HOME_BOT = str(uuid.uuid4())


def _uri(dbname: str) -> str:
    from tests.integration.conftest import _build_db_uri

    parts = urlparse(_build_db_uri())
    return urlunparse(parts._replace(path=f"/{dbname}"))


async def _upgrade(uri: str, revision: str) -> None:
    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parent.parent.parent
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option(
        "sqlalchemy.url", uri.replace("postgresql://", "postgresql+psycopg://", 1)
    )
    await asyncio.to_thread(command.upgrade, cfg, revision)


async def _downgrade(uri: str, revision: str) -> None:
    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parent.parent.parent
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option(
        "sqlalchemy.url", uri.replace("postgresql://", "postgresql+psycopg://", 1)
    )
    await asyncio.to_thread(command.downgrade, cfg, revision)


async def _execute(uri: str, statements: list[tuple[str, tuple]]) -> None:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        for sql, params in statements:
            await conn.execute(sql, params)


async def _fetch(uri: str, sql: str, params: tuple = ()) -> list[tuple]:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


async def _column(uri: str, table: str, column: str) -> bool:
    rows = await _fetch(
        uri,
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s AND column_name=%s",
        (table, column),
    )
    return bool(rows)


async def _index(uri: str, name: str) -> bool:
    rows = await _fetch(
        uri, "SELECT 1 FROM pg_indexes WHERE indexname = %s", (name,)
    )
    return bool(rows)


class _Db:
    def __init__(self) -> None:
        self.uri = ""
        self.admin = _uri("postgres")

    async def recreate(self, create: bool) -> None:
        async with await psycopg.AsyncConnection.connect(
            self.admin, autocommit=True
        ) as conn:
            await conn.execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}"')
            if create:
                await conn.execute(f'CREATE DATABASE "{SCRATCH_DB}"')
        self.uri = _uri(SCRATCH_DB)


@pytest_asyncio.fixture(scope="module")
async def db() -> _Db:
    database = _Db()
    await database.recreate(create=True)
    try:
        await _upgrade(database.uri, "111")
        await _execute(
            database.uri,
            [
                ("INSERT INTO users (user_id, email, name) VALUES (%s, %s, %s)",
                 (USER, "user-114@example.com", "User 114")),
                (
                    "INSERT INTO workspaces (workspace_id, user_id, name, name_key, status) "
                    "VALUES (%s, %s, 'Home', 'home', 'unbound')",
                    (HOME, USER),
                ),
                (
                    "INSERT INTO workspaces (workspace_id, user_id, name, name_key, status) "
                    "VALUES (%s, %s, 'Research', 'research', 'running')",
                    (WS, USER),
                ),
                (
                    "INSERT INTO conversation_threads (conversation_thread_id, "
                    "workspace_id, current_status, thread_index) "
                    "VALUES (%s, %s, 'completed', 1)",
                    (HOME_THREAD, HOME),
                ),
                (
                    "INSERT INTO conversation_threads (conversation_thread_id, "
                    "workspace_id, current_status, thread_index) "
                    "VALUES (%s, %s, 'completed', 1)",
                    (WS_THREAD, WS),
                ),
                (
                    "INSERT INTO automations (automation_id, user_id, name, "
                    "trigger_type, instruction, workspace_id) "
                    "VALUES (%s, %s, 'Home job', 'cron', 'do', %s)",
                    (HOME_AUTOMATION, USER, HOME),
                ),
                (
                    "INSERT INTO automations (automation_id, user_id, name, "
                    "trigger_type, instruction, workspace_id) "
                    "VALUES (%s, %s, 'Ws job', 'cron', 'do', %s)",
                    (WS_AUTOMATION, USER, WS),
                ),
            ],
        )
        await _upgrade(database.uri, "114")
        yield database
    finally:
        await database.recreate(create=False)


class TestFresh114:
    async def test_home_gets_no_backfilled_analyst(self, db):
        rows = await _fetch(
            db.uri,
            "SELECT count(*) FROM bots WHERE workspace_id = %s "
            "AND role = 'analyst' AND deleted_at IS NULL",
            (HOME,),
        )
        assert rows[0][0] == 0

    async def test_the_workspace_gets_one_and_its_rows_point_at_it(self, db):
        rows = await _fetch(
            db.uri,
            "SELECT b.role, "
            "(SELECT count(*) FROM conversation_threads "
            " WHERE conversation_thread_id = %s AND bot_id = b.bot_id), "
            "(SELECT count(*) FROM automations "
            " WHERE automation_id = %s AND bot_id = b.bot_id) "
            "FROM bots b WHERE b.workspace_id = %s AND b.deleted_at IS NULL",
            (WS_THREAD, WS_AUTOMATION, WS),
        )
        assert rows and rows[0][0] == "analyst" and rows[0][1] == 1 and rows[0][2] == 1

    async def test_home_rows_stay_unbound_until_the_resolver_stamps_the_cos(self, db):
        rows = await _fetch(
            db.uri,
            "SELECT "
            "(SELECT bot_id FROM conversation_threads WHERE conversation_thread_id = %s), "
            "(SELECT bot_id FROM automations WHERE automation_id = %s)",
            (HOME_THREAD, HOME_AUTOMATION),
        )
        assert rows[0] == (None, None)

    async def test_the_delivery_lease_and_idem_columns_wait_for_115(self, db):
        assert not await _column(db.uri, "bot_messages", "delivering_until")
        assert not await _column(db.uri, "assignments", "idem")


class Test115RepairsTheFirstCut:
    async def test_it_unlinks_and_soft_deletes_a_home_analyst(self, db):
        await _execute(
            db.uri,
            [
                (
                    "INSERT INTO bots (bot_id, user_id, workspace_id, name, role, created_by) "
                    "VALUES (%s, %s, %s, 'Home', 'analyst', %s)",
                    (OLD_HOME_BOT, USER, HOME, USER),
                ),
                (
                    "INSERT INTO bot_threads (bot_id, thread_id, kind) "
                    "VALUES (%s, %s, 'private')",
                    (OLD_HOME_BOT, HOME_THREAD),
                ),
                (
                    "UPDATE conversation_threads SET bot_id = %s "
                    "WHERE conversation_thread_id = %s",
                    (OLD_HOME_BOT, HOME_THREAD),
                ),
                (
                    "UPDATE automations SET bot_id = %s WHERE automation_id = %s",
                    (OLD_HOME_BOT, HOME_AUTOMATION),
                ),
                (
                    "UPDATE conversation_threads SET is_shared = true "
                    "WHERE conversation_thread_id = %s",
                    (WS_THREAD,),
                ),
            ],
        )
        await _upgrade(db.uri, "115")
        bot = await _fetch(
            db.uri,
            "SELECT deleted_at IS NOT NULL, status FROM bots WHERE bot_id = %s",
            (OLD_HOME_BOT,),
        )
        assert bot[0][0] is True
        cleared = await _fetch(
            db.uri,
            "SELECT "
            "(SELECT bot_id FROM conversation_threads WHERE conversation_thread_id = %s), "
            "(SELECT bot_id FROM automations WHERE automation_id = %s), "
            "(SELECT count(*) FROM bot_threads WHERE bot_id = %s)",
            (HOME_THREAD, HOME_AUTOMATION, OLD_HOME_BOT),
        )
        assert cleared[0] == (None, None, 0)

    async def test_it_drops_a_private_mapping_the_thread_does_not_belong_to(self, db):
        rows = await _fetch(
            db.uri,
            "SELECT count(*) FROM bot_threads bt "
            "JOIN conversation_threads t ON t.conversation_thread_id = bt.thread_id "
            "WHERE bt.kind = 'private' AND t.is_shared",
        )
        assert rows[0][0] == 0

    async def test_it_adds_the_columns_and_indexes_the_code_reads(self, db):
        assert await _column(db.uri, "bot_messages", "delivering_until")
        assert await _column(db.uri, "assignments", "idem")
        for index in (
            "ix_bots_live_name",
            "ix_bots_user",
            "ix_room_rounds_due",
            "ix_room_log_round_slot",
            "ix_room_log_request_key",
            "ix_assignments_idem",
        ):
            assert await _index(db.uri, index), index


class TestCycle:
    async def test_downgrade_to_111_and_upgrade_head_round_trips(self, db):
        # ``command.upgrade`` only goes forward; a lower target is a no-op.
        await _downgrade(db.uri, "111")
        assert not await _column(db.uri, "conversation_threads", "bot_id")
        await _upgrade(db.uri, "head")
        assert await _column(db.uri, "conversation_threads", "bot_id")
        assert await _index(db.uri, "ix_bots_live_name")
