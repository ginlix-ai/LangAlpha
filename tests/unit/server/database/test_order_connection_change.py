"""What a connection change refuses in the order ledger, and what it leaves.

The guard is the contract: a row the relay already let out may be held by the
vendor, so ``dispatched_at`` rides in the statement rather than in a caller, and
the failure has to parse through the reader every order surface draws from.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from src.server.database.order_attempts import refuse_attempts_on_connection_change
from src.server.services.brokerage_orders import Failure

ATTEMPT = "11111111-1111-4111-8111-111111111111"


class _Conn:
    """The caller's transaction: the helper writes on it and never opens one."""

    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple = ()

    @asynccontextmanager
    async def cursor(self, **_kwargs):
        yield self

    async def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = tuple(params or ())

    async def fetchall(self):
        return [{"attempt_id": ATTEMPT}]


@pytest.mark.asyncio
async def test_only_attempts_no_frame_has_carried_are_refused():
    conn = _Conn()

    refused = await refuse_attempts_on_connection_change("user-1", "ibkr", conn=conn)

    assert refused == [ATTEMPT]
    failure, user_id, server = (getattr(p, "obj", p) for p in conn.params)
    assert (user_id, server) == ("user-1", "ibkr")
    assert (
        "WHERE user_id = %s AND server = %s AND (status IN ('proposed', 'approved') "
        "OR (status = 'submitting' AND dispatched_at IS NULL))"
    ) in conn.sql
    # Dated and settled the way the lapse is, under a code of its own.
    assert "SET status = 'refused'" in conn.sql
    assert "completed_at = NOW()" in conn.sql
    assert Failure.from_json(failure).code == "connection_changed"
