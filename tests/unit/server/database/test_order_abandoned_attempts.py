"""What the ledger refuses once nothing is left that could answer an order.

Each refusal is final, so the guard is the contract: which statuses, which
rows, and a failure every order surface can read.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from src.server.database.order_attempts import (
    refuse_attempts_on_fork,
    refuse_attempts_on_thread_delete,
)
from src.server.database.order_reconciliation import refuse_abandoned_proposals
from src.server.services.brokerage_orders import Failure

ATTEMPT = "11111111-1111-4111-8111-111111111111"
THREAD = "22222222-2222-4222-8222-222222222222"


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
        return [{"attempt_id": ATTEMPT}]


@pytest.mark.asyncio
async def test_a_deleted_thread_refuses_only_what_still_waits_in_it():
    conn = _Conn()

    refused = await refuse_attempts_on_thread_delete(THREAD, conn=conn)

    assert refused == [ATTEMPT]
    failure, thread_id = (getattr(p, "obj", p) for p in conn.params)
    assert thread_id == THREAD
    # A row already on its way to the vendor is reconciliation's to settle.
    assert "WHERE thread_id = %s AND status IN ('proposed', 'approved')" in conn.sql
    assert "SET status = 'refused'" in conn.sql
    assert Failure.from_json(failure).code == "thread_deleted"


@pytest.mark.asyncio
async def test_a_fork_refuses_only_what_still_waits_under_the_turns_it_deletes():
    conn = _Conn()

    refused = await refuse_attempts_on_fork(THREAD, 3, conn=conn)

    assert refused == [ATTEMPT]
    failure, thread_id, from_turn = (getattr(p, "obj", p) for p in conn.params)
    assert (thread_id, from_turn) == (THREAD, 3)
    # A row already on its way to the vendor is reconciliation's to settle.
    assert "WHERE status IN ('proposed', 'approved')" in conn.sql
    # Exactly the responses truncate_thread_from_turn deletes, read before it runs.
    assert (
        "AND conversation_response_id IN ( SELECT conversation_response_id "
        "FROM conversation_responses "
        "WHERE conversation_thread_id = %s AND turn_index >= %s )"
    ) in conn.sql
    assert "SET status = 'refused'" in conn.sql
    assert Failure.from_json(failure).code == "turn_replaced"


@pytest.mark.asyncio
async def test_a_proposal_is_refused_only_under_a_run_that_ended_without_asking():
    conn = _Conn()

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch(
        "src.server.database.order_reconciliation.get_db_connection", _connection
    ):
        refused = await refuse_abandoned_proposals(grace_seconds=600)

    assert [row["attempt_id"] for row in refused] == [ATTEMPT]
    failure, grace = (getattr(p, "obj", p) for p in conn.params)
    assert grace == 600.0
    assert "WHERE a.status = 'proposed'" in conn.sql
    # An interrupted run is still waiting on the user, however long they take.
    assert "AND r.status IN ('cancelled', 'completed')" in conn.sql
    assert Failure.from_json(failure).code == "never_decided"
