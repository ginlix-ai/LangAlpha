"""The line a placed order's result ends with, and the order wakeup placing it sets."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from src.server.services.brokerage_orders.models import BrokerOrder
from src.server.services.wakeups import placed
from src.tools.wakeups import orders as order_wakeups
from src.tools.wakeups.wording import PLACED_WATCH

THREAD = "11111111-1111-1111-1111-111111111111"
# Tue 2026-10-20 09:57 EDT.
NOW = datetime(2026, 10, 20, 13, 57, tzinfo=timezone.utc)


def _attempt(**over):
    order = BrokerOrder(
        vendor="moomoo",
        account_ref="acct_test",
        asset_class="equity",
        instrument={"kind": "equity", "symbol": "NVDA", "venue": "NASDAQ"},
        side="buy",
        qty=100,
        order_type="limit",
        limit_price=200,
    )
    return {
        "attempt_id": "22222222-2222-2222-2222-222222222222",
        "vendor": "moomoo",
        "mode": "paper",
        "action": "place",
        "status": "submitted",
        "vendor_order_id": "7712045",
        "filled_qty": None,
        "avg_fill_price": None,
        "failure": None,
        "order_json": order.to_json(),
        "route": {},
        **over,
    }


@pytest.fixture
def arm(monkeypatch):
    """Wakeups on, order status read back, and an arm that records its specs."""
    calls = []

    async def arm_wakeups(thread_id, user_id, specs, *, cap=20):
        calls.append((thread_id, user_id, specs, cap))
        spec = specs[0]
        return [
            {
                "short_id": "w_3f2a",
                "kind": "order",
                "timezone": spec.timezone,
                "expires_at": spec.expires_at,
            }
        ]

    monkeypatch.setattr(placed.arm_db, "arm_wakeups", arm_wakeups)
    monkeypatch.setattr(order_wakeups, "readback_on", lambda: True)
    monkeypatch.setattr(placed, "user_feature_enabled_or", AsyncMock(return_value=True))
    monkeypatch.setattr(placed, "user_timezone", AsyncMock(return_value="Asia/Shanghai"))
    return calls


async def _note(attempt, thread_id=THREAD):
    return await placed.placed_note(attempt, user_id="u_test", thread_id=thread_id, now=NOW)


@pytest.mark.asyncio
async def test_a_working_order_gets_a_wakeup_that_watches_it(arm):
    note, row = await _note(_attempt())

    assert note == (
        "Order ledger: order 7712045 is working and has not filled yet. "
        "Order wakeup w_3f2a was set for you: you hear here when this order fills, even "
        "partly, or ends unfilled, so don't set another for it; it lapses Tue 2026-10-27 "
        "09:57 EDT if it does neither. Wakeups on price or time, such as at the trade's "
        "exit levels, are still yours to set."
    )
    assert row["short_id"] == "w_3f2a"
    [(thread_id, user_id, [spec], cap)] = arm
    assert (thread_id, user_id, cap) == (THREAD, "u_test", 20)
    assert spec.kind == "order"
    assert spec.attempt_id == "22222222-2222-2222-2222-222222222222"
    assert spec.vendor_order_id == "7712045"
    assert spec.baseline_filled_qty == 0.0
    # The lapse reads on the clock the order trades on, not the user's.
    assert spec.timezone == "America/New_York"


@pytest.mark.asyncio
async def test_a_partial_fill_counts_from_what_already_filled(arm):
    note, _ = await _note(
        _attempt(status="partially_filled", filled_qty="40", avg_fill_price="199.5")
    )

    assert note.startswith(
        "Order ledger: order 7712045 has partly filled, 40 of 100 at an average $199.50 "
        "so far; the rest is still working. Order wakeup w_3f2a was set for you:"
    )
    assert arm[0][2][0].baseline_filled_qty == 40.0


@pytest.mark.asyncio
async def test_a_fill_needs_no_wakeup(arm):
    note, row = await _note(
        _attempt(status="filled", filled_qty="100", avg_fill_price="199.8")
    )

    assert note == "Order ledger: order 7712045 filled, 100 at an average $199.80."
    assert row is None
    assert arm == []


@pytest.mark.asyncio
async def test_a_rejection_says_the_brokerages_reason(arm):
    note, row = await _note(
        _attempt(
            status="rejected_by_vendor",
            vendor_order_id=None,
            failure={"kind": "vendor", "message": "insufficient buying power"},
        )
    )

    assert note == (
        "Order ledger: the brokerage rejected this order: insufficient buying power."
    )
    assert row is None
    assert arm == []


@pytest.mark.asyncio
async def test_an_answer_lost_after_sending_warns_against_sending_it_again(arm):
    note, row = await _note(_attempt(status="unknown", vendor_order_id=None))

    assert note.startswith(
        "Order ledger: the brokerage's answer was lost after this order was sent, so it "
        "may exist. Read the open orders before you place it again;"
    )
    assert row is None


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["cancel", "replace", "stage"])
async def test_only_a_placement_gets_a_note(arm, action):
    assert await _note(_attempt(action=action)) == (None, None)
    assert arm == []


@pytest.mark.asyncio
async def test_with_wakeups_off_the_note_states_only_the_order(arm, monkeypatch):
    monkeypatch.setattr(placed, "user_feature_enabled_or", AsyncMock(return_value=False))

    note, row = await _note(_attempt())

    assert note == "Order ledger: order 7712045 is working and has not filled yet."
    assert row is None
    assert arm == []


@pytest.mark.asyncio
async def test_with_order_wakeups_off_the_note_says_none_was_set(arm, monkeypatch):
    async def on(user_id, key, *, on_error):
        return key == "wakeups"

    monkeypatch.setattr(placed, "user_feature_enabled_or", on)

    note, row = await _note(_attempt())

    assert note == (
        "Order ledger: order 7712045 is working and has not filled yet. "
        + PLACED_WATCH["auto_off"]
    )
    assert row is None
    assert arm == []


@pytest.mark.asyncio
async def test_a_full_conversation_says_no_wakeup_was_set(arm, monkeypatch):
    monkeypatch.setattr(
        placed.arm_db, "arm_wakeups", AsyncMock(return_value=[None])
    )

    note, row = await _note(_attempt())

    assert note.endswith(PLACED_WATCH["full"])
    assert row is None


@pytest.mark.asyncio
async def test_a_server_that_reads_nothing_back_says_so(arm, monkeypatch):
    monkeypatch.setattr(order_wakeups, "readback_on", lambda: False)

    note, row = await _note(_attempt())

    assert note.endswith(PLACED_WATCH["readback_off"])
    assert row is None
    assert arm == []


@pytest.mark.asyncio
async def test_no_thread_no_wakeup(arm):
    note, row = await _note(_attempt(), thread_id=None)

    assert note == "Order ledger: order 7712045 is working and has not filled yet."
    assert row is None
    assert arm == []


@pytest.mark.asyncio
async def test_a_confirmation_that_releases_the_order_arms_it_on_the_placement(arm, monkeypatch):
    # The placement waited on a confirmation; the confirmation's answer carries
    # the order, which the ledger hands up to the placement the reconciler follows.
    root = _attempt(status="pending_confirm", vendor_order_id=None, user_id="u_test")
    monkeypatch.setattr(placed.attempts_db, "get_attempt", AsyncMock(return_value=root))
    confirm = {
        "attempt_id": "33333333-3333-3333-3333-333333333333",
        "parent_attempt_id": root["attempt_id"],
        "user_id": "u_test",
        "action": "confirm",
        "status": "submitted",
        "vendor_order_id": "7712045",
        "filled_qty": None,
        "avg_fill_price": None,
        "failure": None,
    }

    note, row = await _note(confirm)

    assert note.startswith(
        "Order ledger: order 7712045 is working and has not filled yet. "
        "Order wakeup w_3f2a was set for you:"
    )
    assert row["short_id"] == "w_3f2a"
    [(_thread, _user, [spec], _cap)] = arm
    assert spec.attempt_id == root["attempt_id"]
    assert spec.vendor_order_id == "7712045"


@pytest.mark.asyncio
async def test_a_confirmation_that_releases_no_order_ends_with_no_line(arm, monkeypatch):
    get = AsyncMock()
    monkeypatch.setattr(placed.attempts_db, "get_attempt", get)
    note, row = await _note(
        {"action": "confirm", "parent_attempt_id": "p", "user_id": "u_test", "status": "pending_confirm"}
    )
    assert (note, row) == (None, None)
    get.assert_not_awaited()
    assert arm == []


@pytest.mark.asyncio
async def test_a_coin_fill_keeps_its_price_precision(arm):
    order = BrokerOrder(
        vendor="robinhood",
        account_ref="acct_test",
        asset_class="crypto",
        instrument={"kind": "crypto", "symbol": "SHIB"},
        side="buy",
        qty=1000000,
        order_type="market",
    )
    note, _ = await _note(
        _attempt(
            vendor="robinhood",
            status="filled",
            filled_qty="1000000",
            avg_fill_price="0.00000464",
            order_json=order.to_json(),
        )
    )
    assert "at an average $0.00000464" in note
