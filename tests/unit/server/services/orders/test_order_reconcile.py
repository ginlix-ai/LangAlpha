"""Reconciliation pass: what it moves, what it refuses to move, and when it stops.

The ledger fake enforces the same guard the real UPDATE does -- a write lands
only when the row's current status is one ``record_observation`` would allow --
so "terminal rows are untouched" is asserted through the engine rather than by
reading the SQL.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.server.database.order_reconciliation import forward_from
from src.server.services.brokerage_orders import (
    AttemptStatus,
    EquityRef,
    ListingIncomplete,
    MatchKey,
    Money,
    OrderOutcome,
    StatusQuery,
    StatusReadError,
    VendorOrder,
)
from src.server.services.brokerage_orders.models import TERMINAL_STATUSES
from src.server.services.egress.execution_token import DEFAULT_TTL_SECONDS
from src.server.services.orders.reconcile import OrderReconciler

_MOD = "src.server.services.orders.reconcile"

EXECUTED_AT = datetime(2026, 9, 9, 6, 46, 16, tzinfo=UTC)
MATCH_KEY = MatchKey.of(
    acc="acct", symbol="AAPL", side="buy", qty="1", price="50", note="note"
)


def _config(**overrides):
    values = {
        "enabled": True,
        "interval_seconds": 60,
        "submitting_grace_seconds": 120,
        "approved_grace_seconds": 600,
        "open_after_seconds": 60,
        "batch_limit": 50,
        "match_window_seconds": 900,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _row(attempt_id: str, status: AttemptStatus, vendor_order_id: str | None = None):
    return {
        "attempt_id": attempt_id,
        "user_id": "u1",
        "server": "moomoo",
        "vendor": "moomoo",
        "tool": "sim_trade_input_order",
        "action": "place",
        "mode": "paper",
        "account_ref": "acct",
        "order_json": {"vendor": "moomoo", "account_ref": "acct", "mode": "paper"},
        "status": str(status),
        "vendor_order_id": vendor_order_id,
        "route": {},
        "executed_at": EXECUTED_AT,
        "created_at": EXECUTED_AT,
        "updated_at": EXECUTED_AT,
    }


def _listed(
    vendor_order_id: str,
    status: AttemptStatus,
    *,
    instrument=None,
    **outcome,
):
    return VendorOrder(
        vendor_order_id=vendor_order_id,
        outcome=OrderOutcome(
            status=status, vendor_order_id=vendor_order_id, **outcome
        ),
        match_key=MATCH_KEY,
        placed_at=EXECUTED_AT,
        instrument=instrument,
    )


def _learn(row, filled_qty, avg_fill_price, fees, instrument):
    """``_LEARNED_SET`` in Python: a null never erases, and a fill never shrinks.

    The average and the fees are measured against a quantity, so a snapshot
    behind the quantity already recorded leaves them where they are rather than
    restating the larger fill at the smaller one's average.
    """
    recorded = row.get("filled_qty")
    behind = filled_qty is not None and recorded is not None and filled_qty < recorded
    if filled_qty is not None:
        row["filled_qty"] = (
            filled_qty if recorded is None else max(filled_qty, recorded)
        )
    for key, value in (
        ("avg_fill_price", avg_fill_price),
        ("fees", fees),
    ):
        if value is not None and not behind:
            row[key] = value
    if instrument is not None:
        row["order_json"] = {
            **(row.get("order_json") or {}),
            "instrument": instrument,
        }


class FakeLedger:
    """The ledger rows, with the real write's guard and nothing else."""

    def __init__(
        self, rows, cancels=None, lapsed=None, abandoned=None, undispatched=None
    ):
        self.rows = {row["attempt_id"]: dict(row) for row in rows}
        self.recorded: list[str] = []
        # A confirmed cancel of the user's own, keyed by the order it ended.
        self.cancels = dict(cancels or {})
        # What the two lapse statements refuse this pass; the SQL decides which.
        self.lapsed = list(lapsed or ())
        self.abandoned = list(abandoned or ())
        # What the undispatched statement fails this pass, for the same reason.
        self.undispatched = list(undispatched or ())
        self.lapse_calls = 0
        self.sweeps: list[str] = []

    async def list_stale_attempts(self, **_kwargs):
        return [dict(row) for row in self.rows.values()]

    async def refuse_abandoned_proposals(self, **_kwargs):
        self.sweeps.append("proposals")
        return [dict(row) for row in self.abandoned]

    async def lapse_unspent_approvals(self, **_kwargs):
        self.lapse_calls += 1
        self.sweeps.append("approvals")
        return [dict(row) for row in self.lapsed]

    async def fail_undispatched_attempts(self, **_kwargs):
        self.sweeps.append("undispatched")
        self.grace = _kwargs.get("grace_seconds")
        return [dict(row) for row in self.undispatched]

    async def claimed_vendor_order_ids(self, _user, _vendor, _account):
        return {
            row["vendor_order_id"]
            for row in self.rows.values()
            if row["vendor_order_id"]
        }

    async def unanswered_attempts(self, _user, _vendor, _account):
        return [
            dict(row)
            for row in self.rows.values()
            if not row["vendor_order_id"]
            and row["status"] in ("submitting", "unknown")
        ]

    async def record_observation(self, attempt_id, current, observation):
        """The one write, with the guard the statement derives for itself."""
        row = self.rows[attempt_id]
        status = observation.status
        allowed = (current,) if status == current else forward_from(status)
        if AttemptStatus(row["status"]) not in allowed:
            return None
        self.recorded.append(attempt_id)
        row["status"] = str(status)
        # COALESCE, as every column but the failure does it: a silent answer
        # erases nothing, and a read naming a state is the answer a lost one
        # was waiting for.
        row["vendor_order_id"] = row["vendor_order_id"] or observation.vendor_order_id
        row["route"] = row["route"] or (observation.route or {})
        row["failure"] = observation.failure
        row["completed"] = status in TERMINAL_STATUSES
        _learn(
            row,
            observation.filled_qty,
            observation.avg_fill_price,
            observation.fees,
            observation.instrument,
        )
        return dict(row)

    async def cancelled_by_own_attempt(
        self, _user, _vendor, vendor_order_id, *, account_ref, exclude_attempt_id
    ):
        cancel = self.cancels.get(vendor_order_id)
        if (
            cancel is None
            or cancel["attempt_id"] == exclude_attempt_id
            or cancel["account_ref"] != account_ref
        ):
            return None
        return dict(cancel)


class StubAdapter:
    """One status read, and whatever the test wants it to have returned.

    ``pages`` is how many pages the vendor claims to have: the listed orders
    come back on the first, the rest are empty, and each one but the last
    names the next by a ``next_key`` argument.
    """

    def __init__(self, listed=None, error=None, key=MATCH_KEY, pages=1):
        self.listed = list(listed or ())
        self.error = error
        self.key = key
        self.pages = pages
        self.queried: list[StatusQuery] = []

    def status_query(self, order, *, vendor_order_id=None, route=None):
        return StatusQuery("sim_trade_history_order_list", {"acc_id": "acct"})

    def parse_status(self, query, body):
        self.queried.append(query)
        if self.error is not None:
            raise self.error
        return list(self.listed) if "next_key" not in query.args else []

    def next_page(self, query, body):
        turned = int(query.args.get("next_key", 0))
        if turned + 1 >= self.pages:
            return None
        return StatusQuery(query.tool, {**query.args, "next_key": str(turned + 1)})

    def match_key(self, order):
        return self.key


class StallingAdapter(StubAdapter):
    """A vendor that says it has more orders and names no page to ask for."""

    def next_page(self, query, body):
        raise ListingIncomplete("more orders, no next page")


class FakeClient:
    def __init__(self, payload="{}", raises=None):
        self.payload = payload
        self.raises = raises
        self.calls: list[tuple[str, dict]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def call_tool(self, name, args):
        self.calls.append((name, dict(args)))
        if self.raises is not None:
            raise self.raises
        return SimpleNamespace(
            content=[SimpleNamespace(text=self.payload)], structured_content=None
        )


@asynccontextmanager
async def _lock(held: bool):
    yield held


def _patches(
    ledger: FakeLedger,
    adapter: StubAdapter,
    client: FakeClient,
    *,
    held=True,
    group_held=True,
):
    """Everything outside the engine: the ledger, the vendor, and the relay."""
    connection = SimpleNamespace(
        status="active",
        server_url="https://mcp.moomoo.com/mcp",
        connection_id="conn-1",
    )
    return [
        patch(f"{_MOD}.reconcile_pass_lock", lambda: _lock(held)),
        patch(f"{_MOD}.reconcile_group_lock", lambda *_: _lock(group_held)),
        patch(f"{_MOD}.list_stale_attempts", ledger.list_stale_attempts),
        patch(f"{_MOD}.lapse_unspent_approvals", ledger.lapse_unspent_approvals),
        patch(f"{_MOD}.refuse_abandoned_proposals", ledger.refuse_abandoned_proposals),
        patch(f"{_MOD}.fail_undispatched_attempts", ledger.fail_undispatched_attempts),
        patch(f"{_MOD}.claimed_vendor_order_ids", ledger.claimed_vendor_order_ids),
        patch(f"{_MOD}.unanswered_attempts", ledger.unanswered_attempts),
        patch(f"{_MOD}.record_observation", ledger.record_observation),
        patch(
            f"{_MOD}.cancelled_by_own_attempt", ledger.cancelled_by_own_attempt
        ),
        patch(f"{_MOD}.SERVABLE", frozenset({"active"})),
        patch(f"{_MOD}.get_connection", _async(connection)),
        patch(f"{_MOD}.vendor_for_url", lambda _url: "moomoo"),
        patch(f"{_MOD}.adapter_for", lambda _vendor: adapter),
        patch(
            f"{_MOD}.active_grant_for_connection",
            _async({"grant_id": "g1", "workspace_id": "ws1"}),
        ),
        patch(
            f"{_MOD}.mint_relay_jwt",
            lambda *a, **k: SimpleNamespace(token="t", expires_at=0),
        ),
        patch(f"{_MOD}.relay_mcp_client", lambda *a, **k: client),
        patch(f"{_MOD}.EGRESS_RELAY_SECRET", "secret"),
    ]


def _async(value):
    async def _call(*_args, **_kwargs):
        return value

    return _call


async def _run(ledger, adapter, client, *, held=True, group_held=True, config=None):
    reconciler = OrderReconciler(config=config or _config())
    patches = _patches(ledger, adapter, client, held=held, group_held=group_held)
    for p in patches:
        p.start()
    try:
        return await reconciler.run_once()
    finally:
        for p in reversed(patches):
            p.stop()


# --------------------------------------------------------------- forward only


def test_forward_from_never_overtakes_a_terminal_row():
    for terminal in (
        AttemptStatus.FILLED,
        AttemptStatus.CANCELLED,
        AttemptStatus.FAILED,
        AttemptStatus.REJECTED_BY_VENDOR,
        AttemptStatus.REJECTED_BY_USER,
        AttemptStatus.REFUSED,
    ):
        for status in AttemptStatus:
            assert terminal not in forward_from(status)


def test_forward_from_does_not_walk_an_attempt_backwards():
    assert AttemptStatus.WORKING not in forward_from(AttemptStatus.SUBMITTED)
    assert AttemptStatus.PARTIALLY_FILLED not in forward_from(AttemptStatus.WORKING)
    assert AttemptStatus.SUBMITTED in forward_from(AttemptStatus.WORKING)
    assert forward_from(AttemptStatus.SUBMITTING) == ()


@pytest.mark.asyncio
async def test_open_row_moves_forward_to_working():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTED, "V1")])
    adapter = StubAdapter([_listed("V1", AttemptStatus.WORKING)])
    report = await _run(ledger, adapter, FakeClient())
    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "working"


@pytest.mark.asyncio
async def test_vendor_says_filled_becomes_filled():
    ledger = FakeLedger([_row("a1", AttemptStatus.WORKING, "V1")])
    adapter = StubAdapter([_listed("V1", AttemptStatus.FILLED)])
    report = await _run(ledger, adapter, FakeClient())
    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "filled"
    assert ledger.rows["a1"]["completed"] is True


@pytest.mark.asyncio
async def test_a_terminal_row_is_not_rewritten_by_a_late_listing():
    # The turn settled the attempt between the candidate read and the write,
    # which is the race the guard exists for: the pass still holds the old row.
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTED, "V1")])
    adapter = StubAdapter([_listed("V1", AttemptStatus.WORKING)])

    stale = [dict(ledger.rows["a1"])]

    async def _stale(**_kwargs):
        ledger.rows["a1"]["status"] = str(AttemptStatus.CANCELLED)
        return stale

    ledger.list_stale_attempts = _stale
    report = await _run(ledger, adapter, FakeClient())
    assert report.moved == 0
    assert ledger.rows["a1"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_an_unmapped_vendor_state_is_never_written_as_unknown():
    ledger = FakeLedger([_row("a1", AttemptStatus.WORKING, "V1")])
    adapter = StubAdapter([_listed("V1", AttemptStatus.UNKNOWN, raw_status="99")])
    report = await _run(ledger, adapter, FakeClient())
    assert report.moved == 0
    assert report.unresolved == 1
    assert ledger.rows["a1"]["status"] == "working"


@pytest.mark.asyncio
async def test_an_unchanged_state_is_recorded_without_moving_the_row():
    """The same write, guarded on the state it read: enrichment, not a move."""
    ledger = FakeLedger([_row("a1", AttemptStatus.WORKING, "V1")])
    adapter = StubAdapter([_listed("V1", AttemptStatus.WORKING)])
    report = await _run(ledger, adapter, FakeClient())
    assert report.moved == 0
    assert ledger.recorded == ["a1"]
    assert ledger.rows["a1"]["status"] == "working"


# ----------------------------------------------------------------- the fill


@pytest.mark.asyncio
async def test_a_partial_fill_then_a_full_one_moves_the_row_carrying_each_fill():
    """The whole point of the fill columns: they change while the order lives."""
    ledger = FakeLedger([_row("a1", AttemptStatus.WORKING, "V1")])

    partial = StubAdapter(
        [
            _listed(
                "V1",
                AttemptStatus.PARTIALLY_FILLED,
                filled_qty=Decimal("4"),
                avg_fill_price=Decimal("318.50"),
            )
        ]
    )
    assert (await _run(ledger, partial, FakeClient())).moved == 1
    assert ledger.rows["a1"]["status"] == "partially_filled"
    assert ledger.rows["a1"]["filled_qty"] == Decimal("4")
    assert ledger.rows["a1"]["avg_fill_price"] == Decimal("318.50")

    full = StubAdapter(
        [
            _listed(
                "V1",
                AttemptStatus.FILLED,
                filled_qty=Decimal("10"),
                avg_fill_price=Decimal("318.62"),
                fees=Money(amount=Decimal("0.35"), currency="USD"),
            )
        ]
    )
    assert (await _run(ledger, full, FakeClient())).moved == 1
    assert ledger.rows["a1"]["status"] == "filled"
    assert ledger.rows["a1"]["completed"] is True
    assert ledger.rows["a1"]["filled_qty"] == Decimal("10")
    assert ledger.rows["a1"]["avg_fill_price"] == Decimal("318.62")
    assert ledger.rows["a1"]["fees"] == {"amount": "0.35", "currency": "USD"}


@pytest.mark.asyncio
async def test_a_fill_that_grew_is_recorded_without_the_status_moving():
    """The usual way a partial fill changes: the same word, a bigger number."""
    ledger = FakeLedger([_row("a1", AttemptStatus.PARTIALLY_FILLED, "V1")])
    ledger.rows["a1"]["filled_qty"] = Decimal("4")
    adapter = StubAdapter(
        [
            _listed(
                "V1", AttemptStatus.PARTIALLY_FILLED, filled_qty=Decimal("7")
            )
        ]
    )

    report = await _run(ledger, adapter, FakeClient())

    assert report.moved == 0
    assert ledger.recorded == ["a1"]
    assert ledger.rows["a1"]["filled_qty"] == Decimal("7")


@pytest.mark.asyncio
async def test_a_listing_with_no_fill_does_not_erase_one_already_recorded():
    ledger = FakeLedger([_row("a1", AttemptStatus.PARTIALLY_FILLED, "V1")])
    ledger.rows["a1"]["filled_qty"] = Decimal("4")
    adapter = StubAdapter([_listed("V1", AttemptStatus.FILLED)])

    await _run(ledger, adapter, FakeClient())

    assert ledger.rows["a1"]["status"] == "filled"
    assert ledger.rows["a1"]["filled_qty"] == Decimal("4")


@pytest.mark.asyncio
async def test_a_stale_snapshot_cannot_shrink_a_fill_already_recorded():
    """A vendor list read before the fill the row already holds.

    Passes are serialized and the completing write is guarded on
    ``submitting``, so a smaller number can only come from the vendor answering
    out of an older snapshot. No venue reports a fill that went backwards, and
    a fill that shrank is what someone would read as shares they still own.
    """
    ledger = FakeLedger([_row("a1", AttemptStatus.PARTIALLY_FILLED, "V1")])
    ledger.rows["a1"]["filled_qty"] = Decimal("7")
    adapter = StubAdapter(
        [_listed("V1", AttemptStatus.PARTIALLY_FILLED, filled_qty=Decimal("4"))]
    )

    await _run(ledger, adapter, FakeClient())

    assert ledger.rows["a1"]["filled_qty"] == Decimal("7")


# ------------------------------------------------- the instrument write-back


@pytest.mark.asyncio
async def test_a_listed_symbol_replaces_an_opaque_vendor_id():
    """IBKR's case: the request could only name a contract id, the list names AAPL."""
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTED, "V1")])
    ledger.rows["a1"]["order_json"]["instrument"] = {
        "kind": "opaque",
        "raw_code": "265598",
    }
    adapter = StubAdapter(
        [_listed("V1", AttemptStatus.WORKING, instrument=EquityRef(symbol="AAPL"))]
    )

    await _run(ledger, adapter, FakeClient())

    assert ledger.rows["a1"]["order_json"]["instrument"] == {
        "kind": "equity",
        "symbol": "AAPL",
        "venue": None,
    }


@pytest.mark.asyncio
async def test_an_instrument_the_request_named_is_never_overwritten():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTED, "V1")])
    named = {"kind": "equity", "symbol": "AAPL", "venue": "US"}
    ledger.rows["a1"]["order_json"]["instrument"] = dict(named)
    adapter = StubAdapter(
        [_listed("V1", AttemptStatus.WORKING, instrument=EquityRef(symbol="AAPL"))]
    )

    await _run(ledger, adapter, FakeClient())

    assert ledger.rows["a1"]["order_json"]["instrument"] == named


# ------------------------------------------------------- the lost submission


@pytest.mark.asyncio
async def test_a_status_read_follows_every_page_the_vendor_offers():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    adapter = StubAdapter([_listed("V1", AttemptStatus.WORKING)], pages=3)
    client = FakeClient()
    report = await _run(ledger, adapter, client)

    assert [c[1].get("next_key") for c in client.calls] == [None, "1", "2"]
    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "working"


@pytest.mark.asyncio
async def test_absence_from_a_list_cut_short_settles_nothing(caplog):
    """Past the page bound the read is incomplete, and an absent call is left open."""
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    adapter = StubAdapter([], pages=5)
    with patch(f"{_MOD}.MAX_LISTING_PAGES", 2), caplog.at_level(logging.INFO):
        report = await _run(ledger, adapter, FakeClient())

    assert report.moved == 0
    assert report.unresolved == 1
    assert ledger.rows["a1"]["status"] == "submitting"
    assert "the vendor has more" in caplog.text


@pytest.mark.asyncio
async def test_absence_from_a_list_that_stalled_settles_nothing(caplog):
    """A vendor claiming more orders it will not name cannot fail an absent call."""
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    with caplog.at_level(logging.WARNING):
        report = await _run(ledger, StallingAdapter([]), FakeClient())

    assert report.moved == 0
    assert report.unresolved == 1
    assert ledger.rows["a1"]["status"] == "submitting"
    assert "absence from this list is not evidence" in caplog.text


@pytest.mark.asyncio
async def test_an_order_on_a_stalled_list_still_settles_its_attempt():
    """The pages read still count: only absence from them stops being evidence."""
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    adapter = StallingAdapter([_listed("V1", AttemptStatus.WORKING)])
    report = await _run(ledger, adapter, FakeClient())

    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "working"
    assert ledger.rows["a1"]["vendor_order_id"] == "V1"


@pytest.mark.asyncio
async def test_an_unknown_attempt_with_no_id_is_settled_from_the_full_list():
    """A lost answer after dispatch files as unknown; the list resolves it."""
    ledger = FakeLedger([_row("a1", AttemptStatus.UNKNOWN)])
    adapter = StubAdapter([_listed("V1", AttemptStatus.FILLED, filled_qty=Decimal("1"))])
    report = await _run(ledger, adapter, FakeClient())

    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "filled"
    assert ledger.rows["a1"]["vendor_order_id"] == "V1"


@pytest.mark.asyncio
async def test_a_lost_answers_failure_goes_once_the_vendor_lists_the_order():
    """A filled order must not still read as a call that failed in transit."""
    row = {
        **_row("a1", AttemptStatus.UNKNOWN),
        "failure": {"kind": "transport", "message": "read timed out"},
    }
    ledger = FakeLedger([row])
    adapter = StubAdapter([_listed("V1", AttemptStatus.FILLED, filled_qty=Decimal("1"))])
    await _run(ledger, adapter, FakeClient())

    assert ledger.rows["a1"]["status"] == "filled"
    assert ledger.rows["a1"]["failure"] is None


@pytest.mark.asyncio
async def test_an_unknown_attempt_with_no_id_absent_from_the_full_list_failed():
    ledger = FakeLedger([_row("a1", AttemptStatus.UNKNOWN)])
    report = await _run(ledger, StubAdapter([]), FakeClient())

    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "failed"
    assert ledger.rows["a1"]["failure"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_an_unknown_attempt_with_an_id_the_vendor_dropped_stays_open():
    ledger = FakeLedger([_row("a1", AttemptStatus.UNKNOWN, "V1")])
    report = await _run(ledger, StubAdapter([]), FakeClient())

    assert report.moved == 0
    assert report.unresolved == 1
    assert ledger.rows["a1"]["status"] == "unknown"


@pytest.mark.asyncio
async def test_submitting_the_vendor_does_not_know_becomes_failed():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    adapter = StubAdapter([_listed("OTHER", AttemptStatus.WORKING)])
    adapter.listed[0] = VendorOrder(
        vendor_order_id="OTHER",
        outcome=OrderOutcome(status=AttemptStatus.WORKING),
        match_key=MatchKey.of(
            acc="acct", symbol="ORCL", side="buy", qty="1", price="50", note="note"
        ),
        placed_at=EXECUTED_AT,
    )
    report = await _run(ledger, adapter, FakeClient())
    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "failed"
    assert ledger.rows["a1"]["failure"]["kind"] == "reconciliation"
    assert ledger.rows["a1"]["failure"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_submitting_the_vendor_does_know_takes_the_vendors_answer():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    adapter = StubAdapter([_listed("V9", AttemptStatus.CANCELLED)])
    report = await _run(ledger, adapter, FakeClient())
    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "cancelled"
    assert ledger.rows["a1"]["vendor_order_id"] == "V9"


@pytest.mark.asyncio
async def test_submitting_we_cannot_identify_is_left_alone():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    adapter = StubAdapter([], key=None)
    report = await _run(ledger, adapter, FakeClient())
    assert report.moved == 0
    assert report.unresolved == 1
    assert ledger.rows["a1"]["status"] == "submitting"


@pytest.mark.asyncio
async def test_two_identical_orders_leave_the_row_rather_than_guess(caplog):
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    adapter = StubAdapter(
        [_listed("V1", AttemptStatus.WORKING), _listed("V2", AttemptStatus.WORKING)]
    )
    with caplog.at_level(logging.WARNING):
        report = await _run(ledger, adapter, FakeClient())
    # Ambiguity is not absence: the vendor has an order, we cannot say which,
    # so the row stays open instead of being failed or paired with a guess. It
    # is also the one unresolved case a person has to settle, so it warns.
    assert report.moved == 0
    assert report.unresolved == 1
    assert ledger.rows["a1"]["status"] == "submitting"
    assert any("listed orders match it" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_a_listing_outside_the_match_window_is_not_this_order():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    far = VendorOrder(
        vendor_order_id="V1",
        outcome=OrderOutcome(status=AttemptStatus.WORKING),
        match_key=MATCH_KEY,
        placed_at=EXECUTED_AT + timedelta(hours=4),
    )
    report = await _run(ledger, StubAdapter([far]), FakeClient())
    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "failed"


@pytest.mark.asyncio
async def test_an_identical_order_placed_before_the_attempt_is_not_this_order():
    """One placed by hand earlier, while this call never reached the book."""
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    earlier = VendorOrder(
        vendor_order_id="MANUAL",
        outcome=OrderOutcome(status=AttemptStatus.FILLED),
        match_key=MATCH_KEY,
        placed_at=EXECUTED_AT - timedelta(minutes=5),
    )
    await _run(ledger, StubAdapter([earlier]), FakeClient())
    assert ledger.rows["a1"]["status"] == "failed"
    assert ledger.rows["a1"]["vendor_order_id"] is None


@pytest.mark.asyncio
async def test_a_listing_seconds_before_the_attempt_is_clock_skew_not_another_order():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    skewed = VendorOrder(
        vendor_order_id="V1",
        outcome=OrderOutcome(status=AttemptStatus.WORKING),
        match_key=MATCH_KEY,
        placed_at=EXECUTED_AT - timedelta(seconds=5),
    )
    await _run(ledger, StubAdapter([skewed]), FakeClient())
    assert ledger.rows["a1"]["status"] == "working"
    assert ledger.rows["a1"]["vendor_order_id"] == "V1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("placed_at", "executed_at"),
    [(None, EXECUTED_AT), (EXECUTED_AT, None)],
    ids=["undated-listing", "undated-attempt"],
)
async def test_a_lone_match_with_no_time_to_compare_is_left_for_a_person(
    placed_at, executed_at, caplog
):
    """Neither adopted nor failed, since an older identical order looks the same."""
    ledger = FakeLedger(
        [{**_row("a1", AttemptStatus.SUBMITTING), "executed_at": executed_at}]
    )
    undated = VendorOrder(
        vendor_order_id="V1",
        outcome=OrderOutcome(status=AttemptStatus.FILLED),
        match_key=MATCH_KEY,
        placed_at=placed_at,
    )
    with caplog.at_level(logging.WARNING):
        report = await _run(ledger, StubAdapter([undated]), FakeClient())

    # Still a candidate, so absence cannot fail the row, but one no window can
    # place after this call, so it is a person's to settle and it warns.
    assert report.moved == 0
    assert report.unresolved == 1
    assert ledger.rows["a1"]["status"] == "submitting"
    assert ledger.rows["a1"]["vendor_order_id"] is None
    assert any(
        r.levelno == logging.WARNING and "V1" in r.message for r in caplog.records
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "swept",
    [("a1", "a2"), ("a1",)],
    ids=["both-in-one-pass", "rival-not-swept-yet"],
)
async def test_one_order_two_identical_calls_leaves_both_rather_than_guess(
    swept, caplog
):
    """Whichever row a pass reaches first is no evidence of which call the vendor took."""
    ledger = FakeLedger(
        [_row("a1", AttemptStatus.SUBMITTING), _row("a2", AttemptStatus.UNKNOWN)]
    )
    ledger.list_stale_attempts = _async([dict(ledger.rows[a]) for a in swept])
    adapter = StubAdapter([_listed("V1", AttemptStatus.WORKING)])
    with caplog.at_level(logging.WARNING):
        report = await _run(ledger, adapter, FakeClient())

    # Adopting it for either would fail the other as absent, and a failed row
    # is what a person reads as leave to place the order again.
    assert report.moved == 0
    assert report.unresolved == len(swept)
    assert ledger.recorded == []
    assert ledger.rows["a1"]["status"] == "submitting"
    assert ledger.rows["a2"]["status"] == "unknown"
    assert any(
        r.levelno == logging.WARNING
        and "2 unanswered attempts match listed order V1" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_an_identical_call_made_after_the_order_was_placed_is_no_rival():
    """Listed minutes before the second call left, so it was only ever the first's."""
    later = {
        **_row("a2", AttemptStatus.SUBMITTING),
        "executed_at": EXECUTED_AT + timedelta(minutes=5),
    }
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING), later])
    adapter = StubAdapter([_listed("V1", AttemptStatus.WORKING)])
    report = await _run(ledger, adapter, FakeClient())

    assert report.moved == 2
    assert ledger.rows["a1"]["status"] == "working"
    assert ledger.rows["a1"]["vendor_order_id"] == "V1"
    assert ledger.rows["a2"]["status"] == "failed"


@pytest.mark.asyncio
async def test_an_unanswered_call_for_a_different_order_is_no_rival():
    class NotedAdapter(StubAdapter):
        """A key carrying the remark, so two calls differ by it and nothing else."""

        def match_key(self, order):
            note = getattr(order, "note", None)
            if not note:
                return self.key
            return MatchKey.of(**{**dict(self.key.fields), "note": note})

    other = _row("a2", AttemptStatus.SUBMITTING)
    other["order_json"] = {**other["order_json"], "note": "a different order"}
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING), other])
    adapter = NotedAdapter([_listed("V1", AttemptStatus.WORKING)])
    await _run(ledger, adapter, FakeClient())

    assert ledger.rows["a1"]["status"] == "working"
    assert ledger.rows["a1"]["vendor_order_id"] == "V1"
    assert ledger.rows["a2"]["vendor_order_id"] is None


# --------------------------------------------------------- errors and the lock


@pytest.mark.asyncio
async def test_a_vendor_error_leaves_the_row_and_is_logged(caplog):
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING, "V1")])
    adapter = StubAdapter(error=StatusReadError("ret_code -5"))
    with caplog.at_level(logging.WARNING):
        report = await _run(ledger, adapter, FakeClient())
    assert ledger.rows["a1"]["status"] == "submitting"
    assert report.moved == 0
    assert report.errors["moomoo"] == 1
    assert any("status read" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_a_dead_relay_session_leaves_every_row(caplog):
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING, "V1")])
    client = FakeClient(raises=RuntimeError("relay refused"))
    with caplog.at_level(logging.WARNING):
        report = await _run(ledger, StubAdapter(), client)
    assert ledger.rows["a1"]["status"] == "submitting"
    assert report.errors["moomoo"] == 1


@pytest.mark.asyncio
async def test_lock_contention_skips_the_pass_without_reading_anything():
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    client = FakeClient()
    report = await _run(ledger, StubAdapter(), client, held=False)
    assert report is None
    assert client.calls == []
    assert ledger.lapse_calls == 0
    assert ledger.rows["a1"]["status"] == "submitting"


@pytest.mark.asyncio
async def test_a_lapsed_approval_is_reported_without_a_vendor_read():
    ledger = FakeLedger([], lapsed=[{"attempt_id": "a9", "user_id": "u1"}])
    client = FakeClient()
    report = await _run(ledger, StubAdapter(), client)
    assert report.lapsed == 1
    assert report.examined == 0
    assert client.calls == []


@pytest.mark.asyncio
async def test_abandoned_proposals_are_refused_before_approvals_lapse():
    ledger = FakeLedger(
        [],
        lapsed=[{"attempt_id": "a9", "user_id": "u1"}],
        abandoned=[{"attempt_id": "a8", "user_id": "u1"}],
    )
    client = FakeClient()
    report = await _run(ledger, StubAdapter(), client)
    # An approval waits while a sibling is still proposed, so order is the point.
    # The undispatched statement runs last of the three and still before the
    # listing, so a row it settles is out of the population the sweep reads.
    assert ledger.sweeps == ["proposals", "approvals", "undispatched"]
    assert report.lapsed == 2
    assert client.calls == []


@pytest.mark.asyncio
async def test_an_attempt_that_never_left_the_host_is_failed_without_a_vendor_read():
    """No listing can speak to a frame the relay never claimed."""
    ledger = FakeLedger([], undispatched=[{"attempt_id": "a7", "user_id": "u1"}])
    client = FakeClient()
    report = await _run(ledger, StubAdapter(), client)
    assert report.undispatched == 1
    # Counted apart from the refusals: nothing decided against this order.
    assert report.lapsed == 0
    assert report.examined == 0
    assert client.calls == []


@pytest.mark.asyncio
async def test_the_undispatched_window_outlasts_the_execution_token():
    """The grace floor is below the token's life, and the longer bound wins.

    Failing a row while its token is still live would have the relay refuse a
    frame that was only slow, so the window cannot be the grace alone.
    """
    ledger = FakeLedger([])
    await _run(
        ledger,
        StubAdapter(),
        FakeClient(),
        config=_config(submitting_grace_seconds=90),
    )
    assert ledger.grace == float(DEFAULT_TTL_SECONDS)
    assert ledger.grace > 90


@pytest.mark.asyncio
async def test_an_order_tool_is_never_called_as_a_status_read(caplog):
    class OrderToolAdapter(StubAdapter):
        def status_query(self, order, *, vendor_order_id=None, route=None):
            return StatusQuery("sim_trade_cancel_order", {"acc_id": "acct"})

    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTING)])
    client = FakeClient()
    with caplog.at_level(logging.ERROR):
        report = await _run(ledger, OrderToolAdapter(), client)
    assert client.calls == []
    assert report.errors["moomoo"] == 1
    assert ledger.rows["a1"]["status"] == "submitting"


@pytest.mark.asyncio
async def test_one_read_serves_every_attempt_it_covers():
    ledger = FakeLedger(
        [
            _row("a1", AttemptStatus.SUBMITTED, "V1"),
            _row("a2", AttemptStatus.SUBMITTED, "V2"),
        ]
    )
    adapter = StubAdapter(
        [_listed("V1", AttemptStatus.FILLED), _listed("V2", AttemptStatus.CANCELLED)]
    )
    client = FakeClient()
    report = await _run(ledger, adapter, client)
    assert len(client.calls) == 1
    assert report.moved == 2


@pytest.mark.asyncio
async def test_an_account_a_sibling_worker_is_reading_is_skipped_not_waited_on():
    """The vendor reads are held per account, not per fleet.

    One brokerage stalling used to stop reconciliation for everyone, because the
    pass held a single fleet-wide key across every vendor call. A group already
    being read elsewhere is now passed over, and its rows keep their sweep stamp
    so the next pass finds them at the head of the queue.
    """
    ledger = FakeLedger([_row("a1", AttemptStatus.SUBMITTED, "V1")])
    adapter = StubAdapter([_listed("V1", AttemptStatus.FILLED)])
    client = FakeClient()

    report = await _run(ledger, adapter, client, group_held=False)

    # Not read at all: no call went out, and nothing was written.
    assert client.calls == []
    assert report.moved == 0
    assert report.skipped == 1
    # The pass itself still ran its SQL-only head, which is what the pass lock
    # is for now, so this is a skipped group rather than a skipped pass.
    assert report is not None
    assert report.examined == 1


# --------------------------------------------------------- the moomoo mapping


def test_moomoo_paper_history_maps_the_observed_cancelled_state():
    from src.server.services.brokerage_orders import adapter_for

    adapter = adapter_for("moomoo")
    query = StatusQuery(
        "sim_trade_history_order_list", {"acc_id": "acct", "market": 100}
    )
    body = json.dumps(
        {
            "ret_code": 0,
            "ret_msg": "success",
            "data": {
                "orders": [
                    {
                        "order_id": "1",
                        "status": 5,
                        "symbol": "AAPL",
                        "side": 1,
                        "qty": "1",
                        "price": "50",
                        "text": "note",
                        "market": 2,
                        "create_time": "1788936376000000",
                    }
                ]
            },
        }
    )
    listed = adapter.parse_status(query, body)
    assert [order.vendor_order_id for order in listed] == ["1"]
    assert listed[0].outcome.status is AttemptStatus.CANCELLED
    # The market a cancel has to send back, not the one the answer is stamped with.
    assert listed[0].outcome.route == {"market": "100"}


def test_moomoo_paper_fingerprint_survives_the_round_trip():
    from src.server.services.brokerage_orders import adapter_for

    adapter = adapter_for("moomoo")
    order = adapter.parse_request(
        "sim_trade_input_order",
        {
            "acc_id": "acct",
            "market": 100,
            "symbol": "AAPL",
            "order_side": 1,
            "order_type": 1,
            "qty": "1",
            "price": "50.00",
            "text": "note",
        },
    )
    query = adapter.status_query(order)
    body = json.dumps(
        {
            "ret_code": 0,
            "data": {
                "orders": [
                    {
                        "order_id": "1",
                        "status": 2,
                        "symbol": "AAPL",
                        "side": 1,
                        "order_type": 1,
                        "qty": "1",
                        "price": "50",
                        "text": "note",
                    }
                ]
            },
        }
    )
    assert adapter.parse_status(query, body)[0].match_key == adapter.match_key(order)


def test_moomoo_status_read_refuses_a_vendor_error():
    from src.server.services.brokerage_orders import adapter_for

    adapter = adapter_for("moomoo")
    query = StatusQuery("sim_trade_history_order_list", {"acc_id": "acct", "market": 1})
    with pytest.raises(StatusReadError):
        adapter.parse_status(query, json.dumps({"ret_code": -5, "ret_msg": "nope"}))


# ------------------------------------------------- an order we cancelled ourselves


def _cancel_row(
    attempt_id: str, vendor_order_id: str, *, account_ref: str = "acct"
) -> dict:
    """A cancel of this user's that the vendor answered ``cancelled``."""
    return {
        "attempt_id": attempt_id,
        "tool": "sim_trade_cancel_order",
        "action": "cancel",
        "account_ref": account_ref,
        "completed_at": EXECUTED_AT,
        "vendor_order_id": vendor_order_id,
    }


@pytest.mark.asyncio
async def test_an_order_our_own_cancel_ended_settles_when_the_vendor_drops_it():
    """The case a vendor states by silence rather than by a status word.

    IBKR answers a delete by dropping the instruction from the list, so the
    placement is read forever with the cancel that ended it one row away.
    """
    ledger = FakeLedger(
        [_row("a1", AttemptStatus.SUBMITTED, "V1")],
        cancels={"V1": _cancel_row("c1", "V1")},
    )
    adapter = StubAdapter([])

    report = await _run(ledger, adapter, FakeClient())

    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "cancelled"
    assert ledger.rows["a1"]["completed"] is True


@pytest.mark.asyncio
async def test_a_vendor_that_still_lists_the_order_outranks_our_cancel_record():
    """A cancel we sent is not a cancel the venue accepted."""
    ledger = FakeLedger(
        [_row("a1", AttemptStatus.SUBMITTED, "V1")],
        cancels={"V1": _cancel_row("c1", "V1")},
    )
    adapter = StubAdapter([_listed("V1", AttemptStatus.WORKING)])

    report = await _run(ledger, adapter, FakeClient())

    assert report.moved == 1
    assert ledger.rows["a1"]["status"] == "working"


@pytest.mark.asyncio
async def test_a_cancel_never_settles_itself():
    ledger = FakeLedger(
        [_row("c1", AttemptStatus.SUBMITTED, "V1")],
        cancels={"V1": _cancel_row("c1", "V1")},
    )
    adapter = StubAdapter([])

    report = await _run(ledger, adapter, FakeClient())

    assert report.moved == 0
    assert ledger.rows["c1"]["status"] == "submitted"


@pytest.mark.asyncio
async def test_a_cancel_in_another_account_does_not_settle_this_order():
    """An order id is unique only within an account, so another's cancel is no evidence."""
    ledger = FakeLedger(
        [_row("a1", AttemptStatus.SUBMITTED, "V1")],
        cancels={"V1": _cancel_row("c1", "V1", account_ref="other")},
    )

    report = await _run(ledger, StubAdapter([]), FakeClient())

    assert report.moved == 0
    assert ledger.rows["a1"]["status"] == "submitted"
