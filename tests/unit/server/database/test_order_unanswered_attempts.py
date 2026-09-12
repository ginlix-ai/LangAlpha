"""Which attempts compete for a listed order that no attempt owns yet.

The read decides whether the sweep may adopt an order at all, so its scope is
the contract: an attempt left out is one a listed order gets taken away from.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import patch
from uuid import UUID

import pytest

from src.server.database.order_reconciliation import unanswered_attempts

ATTEMPT = "11111111-1111-4111-8111-111111111111"


class _Conn:
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
        return [{"attempt_id": UUID(ATTEMPT), "order_json": {}, "executed_at": None}]


@pytest.mark.asyncio
async def test_every_placement_in_the_account_still_waiting_on_its_answer_competes():
    conn = _Conn()

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch(
        "src.server.database.order_reconciliation.get_db_connection", _connection
    ):
        rows = await unanswered_attempts("user-1", "moomoo", "1234567")

    # A string id, which is what the pass compares against its own row's.
    assert [row["attempt_id"] for row in rows] == [ATTEMPT]
    assert conn.params == ("user-1", "moomoo", "1234567", ["place", "stage"])
    assert "AND account_ref IS NOT DISTINCT FROM %s" in conn.sql
    assert "AND vendor_order_id IS NULL" in conn.sql
    assert "AND status IN ('submitting', 'unknown')" in conn.sql
    # No grace window: a call still inside it may be the one the vendor took.
    assert "updated_at" not in conn.sql
