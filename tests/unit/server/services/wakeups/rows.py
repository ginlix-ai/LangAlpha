"""Wakeup rows and order attempts shaped as the database returns them."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.server.services.brokerage_orders.models import BrokerOrder

UTC = timezone.utc
# Tue 2026-10-20 09:57 EDT.
SET_AT = datetime(2026, 10, 20, 13, 57, tzinfo=UTC)
WAKEUP_ID = "33333333-3333-3333-3333-333333333333"
THREAD_ID = "11111111-1111-1111-1111-111111111111"


def base_row(**over) -> dict:
    return {
        "wakeup_id": WAKEUP_ID,
        "short_id": "w_3f2a",
        "conversation_thread_id": THREAD_ID,
        "user_id": "u_test",
        "label": None,
        "status": "armed",
        "fires_done": 0,
        "last_fired_seq": 0,
        "delivered_seq": 0,
        "created_at": SET_AT,
        "schedule": [],
        **over,
    }


def cron_schedule(n: int = 24, *, every: int = 5) -> list[datetime]:
    """``n`` checks from 10:00 EDT, ``every`` minutes apart."""
    first = datetime(2026, 10, 20, 14, 0, tzinfo=UTC)
    return [first + timedelta(minutes=every * i) for i in range(n)]


def cron_row(**over) -> dict:
    return base_row(
        kind="cron",
        timezone="America/New_York",
        cron="*/5 10-15 * * 1-5",
        schedule=cron_schedule(),
        stop_reason="24",
        **over,
    )


def order_attempt(**over) -> dict:
    """An ``order_attempts`` row as the ledger writes one: ``order_json`` is a
    BrokerOrder's ``to_json``, beside the vendor, mode and ids."""
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
        "mode": "live",
        "action": "place",
        "status": "submitted",
        "vendor_order_id": "7712045",
        "filled_qty": 0,
        "route": {},
        "order_json": order.to_json(),
        **over,
    }
