"""The three statements that tie a confirm to the call that raised its question.

A vendor can hold a placement, a replace or a cancel for a confirmation, and the
call that answers names only the confirmation. These pin what the SQL itself
does, since the ledger's own tests run against a fake that mirrors it: the token
is kept on the row it was raised on, a confirm found by it resolves to the root
of its chain, and the order id a confirm learns is written up only where none
is held.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import patch
from uuid import UUID

import pytest

from src.server.database.order_attempts import (
    CONFIRM_TOKEN_KEY,
    complete_attempt,
    latest_attempt_for_confirm_token,
)
from src.server.services.brokerage_orders import AttemptStatus, OrderOutcome

ATTEMPT = "11111111-1111-4111-8111-111111111111"
ROOT = "22222222-2222-4222-8222-222222222222"


class _Cursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple = ()
        self.rowcount = 1
        self.row: object = (UUID(ROOT),)

    async def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = tuple(params or ())

    async def fetchone(self):
        return self.row


@pytest.fixture
def cursor():
    cur = _Cursor()

    class _Conn:
        @asynccontextmanager
        async def cursor(self, **_kwargs):
            yield cur

    @asynccontextmanager
    async def _db():
        yield _Conn()

    with patch("src.server.database.order_attempts.get_db_connection", new=_db):
        yield cur


@pytest.mark.asyncio
async def test_a_pending_confirmation_keeps_its_token_in_the_route(cursor):
    """Merged beside the adapter's own tokens, never in place of them."""
    cursor.row = {"attempt_id": ATTEMPT}
    await complete_attempt(
        ATTEMPT,
        OrderOutcome(
            status=AttemptStatus.PENDING_CONFIRM,
            route={"correlation_id": "c-8001"},
            confirm_token="q-1",
        ),
    )

    route = getattr(cursor.params[2], "obj", cursor.params[2])
    assert route == {"correlation_id": "c-8001", CONFIRM_TOKEN_KEY: "q-1"}
    assert "route = COALESCE(%s::jsonb, route)" in cursor.sql


@pytest.mark.asyncio
async def test_an_answer_with_no_token_writes_the_route_as_before(cursor):
    cursor.row = {"attempt_id": ATTEMPT}
    await complete_attempt(ATTEMPT, OrderOutcome(status=AttemptStatus.WORKING))

    assert cursor.params[2] is None


@pytest.mark.asyncio
async def test_a_confirm_found_by_its_token_resolves_to_the_root_of_its_chain(cursor):
    parent = await latest_attempt_for_confirm_token(
        "user-1", "ibkr", "q-2", account_ref=""
    )

    assert parent == ROOT
    # The row found may be the confirm that raised the next question; its
    # parent is then the call that started the chain.
    assert (
        "CASE WHEN action = %s AND parent_attempt_id IS NOT NULL "
        "THEN parent_attempt_id ELSE attempt_id END"
    ) in cursor.sql
    assert "AND route ->> %s = %s" in cursor.sql
    assert "ORDER BY created_at DESC, attempt_id DESC LIMIT 1" in cursor.sql
    # Any action may have raised it, so none is filtered on; the scope is the
    # user, the vendor and the account, as for an order id.
    assert "action = ANY" not in cursor.sql
    assert cursor.params == ("confirm", "user-1", "ibkr", "", CONFIRM_TOKEN_KEY, "q-2")


@pytest.mark.asyncio
async def test_a_blank_token_looks_nothing_up(cursor):
    assert await latest_attempt_for_confirm_token("u", "ibkr", " ", account_ref="") is None
    assert cursor.sql == ""


@pytest.mark.asyncio
async def test_the_hand_up_rides_the_completing_write(cursor):
    """One statement, so a confirm settled can never leave its order id behind.

    Written only onto the chain's root of the same user that holds no id, so a
    second worker finishing the same tail changes nothing, and never the
    root's status, which is the read's to settle.
    """
    cursor.row = {"attempt_id": ATTEMPT}
    await complete_attempt(
        ATTEMPT,
        OrderOutcome(status=AttemptStatus.WORKING, vendor_order_id="5001001"),
    )

    assert cursor.sql.startswith("WITH settled AS ( UPDATE order_attempts SET status = %s")
    hand_up = cursor.sql[cursor.sql.index("handed_up AS ("):]
    assert (
        "UPDATE order_attempts AS root SET vendor_order_id = settled.vendor_order_id, "
        "updated_at = NOW() FROM settled WHERE settled.action = 'confirm' "
        "AND root.attempt_id = settled.parent_attempt_id "
        "AND root.user_id = settled.user_id AND root.vendor_order_id IS NULL"
    ) in hand_up
    assert "status" not in hand_up.split("SELECT")[0].split("SET", 1)[1].split("FROM")[0]
    assert cursor.sql.endswith("FROM settled")
    assert cursor.params[-1] == ATTEMPT
