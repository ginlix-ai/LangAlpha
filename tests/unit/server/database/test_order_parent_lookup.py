"""An amend stores the id of the order it names too, so only a placement may answer as that order's parent."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import patch
from uuid import UUID

import pytest

from src.server.database.order_attempts import latest_attempt_for_vendor_order

PLACEMENT = "11111111-1111-4111-8111-111111111111"


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

    async def fetchone(self):
        return (UUID(PLACEMENT),)


@pytest.mark.asyncio
async def test_a_cancel_after_a_replace_links_to_the_placement_not_the_replace():
    conn = _Conn()

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch("src.server.database.order_attempts.get_db_connection", _connection):
        parent = await latest_attempt_for_vendor_order(
            "user-1", "moomoo", "900101", account_ref="1234567"
        )

    # A string id, which is what the ledger writes as ``parent_attempt_id``.
    assert parent == PLACEMENT
    assert "AND action = ANY(%s)" in conn.sql
    assert conn.params[-2:] == ("900101", ["place", "stage"])
