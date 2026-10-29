"""What turning approval on refuses in the order ledger, and what it leaves.

An order the user approved was asked, and one the relay already let out may be
held by the vendor, so both guards ride in the statement rather than in a
caller. The failure has to parse through the reader every order surface draws
from. The refusal is one statement and cannot see an attempt proposed after it,
so the consume of an unasked attempt judges it again under the lock the
refusal holds.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from src.server.database import order_attempts as module
from src.server.database.order_attempts import consume_attempt, refuse_unasked_attempts
from src.server.services.brokerage_orders import Failure
from src.server.services.trading_permission import TRADING_AGREEMENT_VERSION

ATTEMPT = "11111111-1111-4111-8111-111111111111"


class _Conn:
    """The caller's transaction: the helper writes on it and never opens one."""

    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple = ()
        self.statements: list[tuple[str, tuple]] = []

    @asynccontextmanager
    async def cursor(self, **_kwargs):
        yield self

    async def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = tuple(params or ())
        self.statements.append((self.sql, self.params))

    async def fetchall(self):
        return [{"attempt_id": ATTEMPT}]


@pytest.mark.asyncio
async def test_only_unasked_attempts_no_frame_has_carried_are_refused():
    conn = _Conn()

    refused = await refuse_unasked_attempts(
        "user-1", ["live", "staged"], server="moomoo", conn=conn
    )

    assert refused == [ATTEMPT]
    failure, user_id, modes, server, _ = (getattr(p, "obj", p) for p in conn.params)
    assert (user_id, modes, server) == ("user-1", ["live", "staged"], "moomoo")
    assert (
        "WHERE user_id = %s AND NOT approval_required AND mode = ANY(%s) "
        "AND (%s::text IS NULL OR server = %s) AND (status = 'approved' "
        "OR (status = 'submitting' AND dispatched_at IS NULL))"
    ) in conn.sql
    assert "SET status = 'refused'" in conn.sql
    assert "completed_at = NOW()" in conn.sql
    assert Failure.from_json(failure).code == "approval_changed"


@pytest.mark.asyncio
async def test_without_a_server_every_connection_is_refused():
    conn = _Conn()

    await refuse_unasked_attempts("user-1", ["live"], conn=conn)

    *_, server, also = conn.params
    assert server is None and also is None


LOCK = "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))"


@pytest.mark.asyncio
async def test_the_refusal_takes_the_approval_lock_before_it_reads():
    conn = _Conn()

    await refuse_unasked_attempts("user-1", ["live"], conn=conn)

    (first, key), (last, _) = conn.statements[0], conn.statements[-1]
    assert first == LOCK and key == ("orders:approval:user-1",)
    assert last.startswith("UPDATE order_attempts SET status = 'refused'")


class _Tx:
    """One pooled connection inside ``consume_attempt``'s transaction, logging
    every statement and the catalog read in the order they ran."""

    def __init__(self, found: dict | None, log: list) -> None:
        self.found = found
        self.log = log
        self.connection = self

    @asynccontextmanager
    async def transaction(self):
        self.log.append("BEGIN")
        yield
        self.log.append("COMMIT")

    @asynccontextmanager
    async def cursor(self, **_kwargs):
        yield self

    async def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = tuple(getattr(p, "obj", p) for p in params or ())
        self.log.append((self.sql, self.params))

    async def fetchone(self):
        if self.sql.startswith("SELECT user_id, server, vendor, tool"):
            return self.found
        if "SET status = 'submitting'" in self.sql:
            return {"attempt_id": ATTEMPT, "status": "submitting"}
        return None


def _consume_patches(monkeypatch, *, level: str, found: dict | None):
    log: list = []
    tx = _Tx(found, log)

    @asynccontextmanager
    async def _connection(conn=None):
        yield tx

    async def _catalog(user_id, name, *, conn=None):
        assert conn is tx, "the policy is read inside the consume's transaction"
        log.append(("catalog", user_id, name))
        return {
            "transport": "http",
            "order_approval": None,
            "trading_level": level,
            "trading_agreement_version": TRADING_AGREEMENT_VERSION,
        }

    monkeypatch.setattr(module, "get_db_connection", _connection)
    monkeypatch.setattr(module, "get_catalog_server", _catalog, raising=False)
    return log


UNASKED = {
    "user_id": "user-1",
    "server": "moomoo",
    "vendor": "moomoo",
    "tool": "trading_order_place",
}


def _statements(log: list) -> dict[str, tuple]:
    return {e[0]: e[1] for e in log if isinstance(e, tuple) and len(e) == 2}


def _sql(log: list) -> list[str]:
    return list(_statements(log))


@pytest.mark.asyncio
async def test_an_unasked_attempt_whose_order_asks_now_is_refused_not_consumed(
    monkeypatch,
):
    """Proposed after the step-down's refusal ran, so only the consume can
    catch it, and it reads the level only once it holds the refusal's lock."""
    log = _consume_patches(monkeypatch, level="approve_each", found=UNASKED)

    assert await consume_attempt(ATTEMPT) is None

    assert log[0] == "BEGIN" and log[-1] == "COMMIT"
    lock = (LOCK, ("orders:approval:user-1",))
    assert log.index(lock) < log.index(("catalog", "user-1", "moomoo"))
    statements = _statements(log)
    refusal = next(s for s in statements if "SET status = 'refused'" in s)
    assert "WHERE attempt_id = %s AND status = 'approved'" in refusal
    failure, attempt = statements[refusal]
    assert attempt == ATTEMPT
    assert Failure.from_json(failure).code == "approval_changed"
    assert not any("SET status = 'submitting'" in s for s in statements)


@pytest.mark.asyncio
async def test_an_unasked_attempt_that_still_does_not_ask_is_consumed(monkeypatch):
    log = _consume_patches(monkeypatch, level="autonomous", found=UNASKED)

    row = await consume_attempt(ATTEMPT)

    assert row["status"] == "submitting"
    sql = _sql(log)
    assert LOCK in sql
    assert not any("SET status = 'refused'" in s for s in sql)


@pytest.mark.asyncio
async def test_an_asked_attempt_is_consumed_without_the_approval_lock(monkeypatch):
    """The user answered this one, so no later answer can take it back, and
    the select that finds unasked attempts returns nothing for it."""
    log = _consume_patches(monkeypatch, level="approve_each", found=None)

    row = await consume_attempt(ATTEMPT)

    assert row["status"] == "submitting"
    assert LOCK not in _sql(log)
    assert ("catalog", "user-1", "moomoo") not in log
