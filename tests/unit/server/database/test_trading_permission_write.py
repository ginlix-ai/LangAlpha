"""Where the trading permission write takes the per-user lock.

The version fan-out is one UPDATE whose snapshot cannot see a workspace another
worker inserts while this write is open, and that workspace can resolve the old
level under a version nothing moves again. A workspace insert takes
``lock_user_writes`` first, so this write takes it before anything it writes.
A connection's approval switch takes the approval lock before its own bump, so
a level that asks takes it before this bump too, or the two writes deadlock.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from src.server.database import trading_permission as module
from src.server.services.trading_permission import TradingPermission


class _Db:
    def __init__(self) -> None:
        self.log: list[str] = []

    @asynccontextmanager
    async def transaction(self):
        self.log.append("BEGIN")
        yield
        self.log.append("COMMIT")

    @asynccontextmanager
    async def cursor(self, **_kwargs):
        yield self

    async def execute(self, sql, params=None):
        self.log.append(" ".join(sql.split()))

    async def fetchone(self):
        return {"level": "approve_each"}


@pytest.fixture
def db(monkeypatch) -> _Db:
    db = _Db()

    @asynccontextmanager
    async def _connection(conn=None):
        yield db

    async def _refuse(user_id, modes, *, conn):
        db.log.append("refuse")
        return []

    monkeypatch.setattr(module, "get_db_connection", _connection)
    monkeypatch.setattr(module, "refuse_unasked_attempts", _refuse)
    return db


def _bump(log: list[str]) -> int:
    return next(i for i, s in enumerate(log) if s.startswith("UPDATE workspaces"))


@pytest.mark.asyncio
async def test_the_write_takes_the_workspace_insert_lock_before_it_writes(db):
    await module.set_trading_permission("user-1", TradingPermission.APPROVE_EACH, None)

    assert db.log[0] == "BEGIN"
    assert db.log[1] == "SELECT pg_advisory_xact_lock(hashtext(%s::text))"
    assert db.log[2].startswith("INSERT INTO trading_permissions")
    assert db.log[-1] == "COMMIT"


@pytest.mark.asyncio
async def test_a_level_that_asks_refuses_before_it_bumps(db):
    await module.set_trading_permission("user-1", TradingPermission.APPROVE_EACH, None)

    assert db.log.index("refuse") < _bump(db.log)


@pytest.mark.asyncio
async def test_a_level_that_skips_approval_refuses_nothing(db):
    await module.set_trading_permission("user-1", TradingPermission.AUTONOMOUS, 1)

    assert "refuse" not in db.log
    assert db.log[-1] == "COMMIT"
