"""What the two writes that record a fill bind, in the order the SQL reads them.

Both are positional, and the two columns most likely to be swapped hold the same
type and are almost always both NULL at the moment a placement is answered -- so
a reordered bind would put a price in a quantity column and stay invisible until
the first order that actually filled. Nothing else in the suite reaches this SQL.

Reconciliation's write binds the instrument twice: once to decide whether there
is anything to patch and once to patch with. It also derives its own guard, so
the states it names are asserted here rather than taken from a caller.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import patch

import pytest

from src.server.database.order_attempts import complete_attempt
from src.server.database.order_reconciliation import Observation, record_observation
from src.server.services.brokerage_orders import AttemptStatus, Money, OrderOutcome

ATTEMPT = "11111111-1111-4111-8111-111111111111"
INSTRUMENT = {"kind": "equity", "symbol": "AAPL", "venue": None}


class _Cursor:
    def __init__(self):
        self.sql = ""
        self.params: tuple = ()
        self.rowcount = 1

    async def execute(self, sql, params=None):
        self.sql = sql
        self.params = tuple(params or ())

    async def fetchone(self):
        return {"attempt_id": ATTEMPT}


@pytest.fixture
def cursor(request):
    cur = _Cursor()

    @asynccontextmanager
    async def _cursor(**_kwargs):
        yield cur

    class _Conn:
        cursor = staticmethod(_cursor)

    @asynccontextmanager
    async def _db():
        yield _Conn()

    module = getattr(request, "param", "order_reconciliation")
    with patch(f"src.server.database.{module}.get_db_connection", new=_db):
        yield cur


def _json_params(cursor: _Cursor) -> list:
    """The bound values, with psycopg's Json wrappers unwrapped."""
    return [getattr(p, "obj", p) for p in cursor.params]


@pytest.mark.asyncio
async def test_the_settling_write_binds_the_fill_and_the_instrument_twice(cursor):
    await record_observation(
        ATTEMPT,
        AttemptStatus.WORKING,
        Observation(
            status=AttemptStatus.FILLED,
            vendor_order_id="900001",
            filled_qty=Decimal("10"),
            avg_fill_price=Decimal("318.62"),
            fees={"amount": "0.35", "currency": "USD"},
            instrument=INSTRUMENT,
        ),
    )

    params = _json_params(cursor)
    assert params[:2] == ["filled", "900001"]
    # route, failure, then the learned facts, then completed / id / guard. The
    # quantity is bound three times: the floor it may not fall below, then the
    # gate ahead of the average and of the fees, both measured with it.
    assert params[4:11] == [
        Decimal("10"),
        Decimal("10"),
        Decimal("318.62"),
        Decimal("10"),
        {"amount": "0.35", "currency": "USD"},
        INSTRUMENT,
        INSTRUMENT,
    ]
    # Terminal, so the same statement dates the attempt, and the guard is the
    # set the write derived rather than one a caller chose.
    assert params[11] is True
    assert params[12] == ATTEMPT
    assert set(params[13]) == {
        "submitting", "unknown", "submitted", "pending_confirm",
        "working", "partially_filled",
    }
    assert "jsonb_set" in cursor.sql


@pytest.mark.asyncio
async def test_a_read_naming_the_same_state_only_enriches_and_never_completes(cursor):
    await record_observation(
        ATTEMPT,
        AttemptStatus.PARTIALLY_FILLED,
        Observation(status=AttemptStatus.PARTIALLY_FILLED, filled_qty=Decimal("7")),
    )

    params = _json_params(cursor)
    assert params[4:11] == [
        Decimal("7"), Decimal("7"), None, Decimal("7"), None, None, None
    ]
    # GREATEST, so a vendor snapshot older than the row cannot walk a recorded
    # fill backwards, and a NULL still leaves the column alone. The average and
    # the fees are gated on that same quantity, so a snapshot behind the row
    # cannot restate the larger fill at the smaller one's average.
    assert "filled_qty = GREATEST(%s, filled_qty)" in cursor.sql
    assert "WHEN %s < filled_qty THEN avg_fill_price" in cursor.sql
    assert "WHEN %s < filled_qty THEN fees" in cursor.sql
    # Not terminal, so nothing dates the attempt, and the guard is the state
    # the pass read: the row may only be enriched where it still stands.
    assert params[11] is False
    assert params[12] == ATTEMPT
    assert params[13] == ["partially_filled"]


@pytest.mark.asyncio
async def test_a_read_naming_a_state_clears_a_lost_answers_failure(cursor):
    """Assigned, not COALESCEd: an open row's failure is only ever a lost answer's."""
    await record_observation(
        ATTEMPT, AttemptStatus.UNKNOWN, Observation(status=AttemptStatus.WORKING)
    )

    assert _json_params(cursor)[3] is None
    assert "failure = %s::jsonb" in cursor.sql


@pytest.mark.asyncio
async def test_an_absence_may_only_settle_the_row_it_was_read_from(cursor):
    """``failed`` from a missing listing is held to one state, not to its rank.

    It shares the top rank with ``filled``, so rank alone would let it overtake
    every open state, including a ``submitted`` the attempt's own call wrote in
    the window between the sweep's select and this write. That would terminalize
    an order the brokerage accepted, and unrecoverably, since nothing overtakes a
    terminal in turn. So the guard names the status the sweep actually read.
    """
    await record_observation(
        ATTEMPT,
        AttemptStatus.SUBMITTING,
        Observation(
            status=AttemptStatus.FAILED,
            reason="not_found",
            from_absence=True,
            failure={"kind": "reconciliation", "code": "not_found"},
        ),
    )

    params = _json_params(cursor)
    # One state, the observed one. Without the narrowing this arrives as the
    # whole of ``forward_from(failed)``, which carries submitted and working.
    assert params[13] == ["submitting"]
    assert "WHERE attempt_id = %s AND status = ANY(%s)" in cursor.sql


@pytest.mark.asyncio
async def test_a_vendors_own_answer_still_overtakes_every_open_state(cursor):
    """The narrowing is on absence, not on terminals: a real fill still lands."""
    await record_observation(
        ATTEMPT, AttemptStatus.SUBMITTING, Observation(status=AttemptStatus.FILLED)
    )

    # A row that moved to working while the read was in flight is still settled
    # by it, because the vendor said so rather than failed to mention it.
    assert "working" in _json_params(cursor)[13]


@pytest.mark.parametrize("cursor", ["order_attempts"], indirect=True)
@pytest.mark.asyncio
async def test_the_completing_write_binds_the_fill_the_vendor_answered(cursor):
    await complete_attempt(
        ATTEMPT,
        OrderOutcome(
            status=AttemptStatus.FILLED,
            vendor_order_id="900001",
            filled_qty=Decimal("10"),
            avg_fill_price=Decimal("318.62"),
            fees=Money(amount=Decimal("0.35"), currency="USD"),
        ),
        "cafe",
    )

    params = _json_params(cursor)
    assert params[:2] == ["filled", "900001"]
    # route, action_url, then the fill, then the result hash.
    assert params[4:8] == [
        Decimal("10"),
        Decimal("318.62"),
        {"amount": "0.35", "currency": "USD"},
        "cafe",
    ]
    assert params[-1] == ATTEMPT
