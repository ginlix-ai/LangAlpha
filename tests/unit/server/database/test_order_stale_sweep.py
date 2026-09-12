"""Which attempts a vendor read could still settle, and which it never could.

An attempt whose action names an event no tool lists is left out of the sweep
itself rather than read and left unresolved: the read has nowhere to look, so
picking the row would only stamp it and ask the same empty question next pass.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import patch
from uuid import UUID

import pytest

from src.server.database.order_reconciliation import (
    fail_undispatched_attempts,
    list_stale_attempts,
)

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
        return [{"attempt_id": UUID(ATTEMPT), "status": "submitted"}]


@pytest.mark.asyncio
async def test_an_attempt_whose_event_no_tool_lists_is_never_picked():
    conn = _Conn()

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch(
        "src.server.database.order_reconciliation.get_db_connection", _connection
    ):
        rows = await list_stale_attempts(
            submitting_grace_seconds=120, open_after_seconds=60, limit=50
        )

    # A string id, which is what the pass carries through to the write.
    assert [row["attempt_id"] for row in rows] == [ATTEMPT]
    # Both actions, because the same fact covers an exercise and its cancel.
    assert conn.params[0] == ["exercise", "cancel_exercise"]
    # Outside the two populations rather than inside one of them: a lost answer
    # is no more readable than an answered one when nothing lists the event.
    assert "WHERE COALESCE(action, '') <> ALL(%s) AND (" in conn.sql
    # A row that recorded no action is not one of them.
    assert "COALESCE(action, '')" in conn.sql
    assert conn.params[1:] == (120.0, ["submitted", "pending_confirm", "working",
                                       "partially_filled", "unknown"], 60.0, 50)


@pytest.mark.asyncio
async def test_only_a_placement_no_frame_carried_is_failed_unread():
    """The predicate is the fix: the host's own record, not the vendor's list."""
    conn = _Conn()

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch(
        "src.server.database.order_reconciliation.get_db_connection", _connection
    ):
        await fail_undispatched_attempts(grace_seconds=120)

    assert "status = 'submitting'" in conn.sql
    # The marker that makes the read unnecessary: no frame ever left.
    assert "dispatched_at IS NULL" in conn.sql
    # Dated from the execution, because the bound is the token's life. From
    # ``updated_at`` the window would restart on every touch of the row.
    assert "executed_at < NOW() - make_interval(secs => %s)" in conn.sql
    assert "updated_at <" not in conn.sql
    # Failed, not refused: nothing decided against this order.
    assert "SET status = 'failed'" in conn.sql
    assert conn.params[1] == 120.0


@pytest.mark.asyncio
async def test_an_undispatched_row_is_left_to_the_undispatched_sweep():
    """A row that never left the host is not the vendor match's to settle.

    It has no order on any book to be found by, so the fingerprint match could
    only adopt somebody else's identical order. The two sweeps divide on
    ``dispatched_at``, and they had to: ``submitting_grace_seconds`` may be set
    as low as 90 while the undispatched sweep waits the longer of that and the
    token's 120s life, so a grace under 120 handed those rows here first.
    """
    conn = _Conn()

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch(
        "src.server.database.order_reconciliation.get_db_connection", _connection
    ):
        await list_stale_attempts(
            submitting_grace_seconds=90, open_after_seconds=60, limit=50
        )

    assert "status = 'submitting' AND dispatched_at IS NOT NULL" in conn.sql
    # The other arm is untouched. An ``unknown`` status means a call came back
    # with something, so that frame did leave, whatever it came back with.
    assert "(status = 'unknown' AND vendor_order_id IS NULL)" in conn.sql
    # No new parameter: the predicate is a column test, so the grace still lands
    # in the same slot and every existing caller is unchanged.
    assert conn.params[1] == 90.0
