"""Watching an order placed elsewhere, such as in the user's own account.

An order wakeup follows a ledger row, and the reconciler reads only rows it
holds, so an order this thread did not place first has to become one. A watch
reads the order once through the user's own grant, to prove the account lists
it and it is still open, and records it as a ``watch`` attempt. From then on
it is read back, and its wakeup fires, exactly as for an order the agent
placed. Nothing is ever sent for it: a watch has no approval, no execution and
no tool that changes an order.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from src.server.database.order_attempts import insert_watch
from src.server.services.brokerage_orders import (
    OPEN_STATUSES,
    BrokerOrder,
    OrderAdapter,
    StatusQuery,
    VendorOrder,
)
from src.server.services.orders.status_read import (
    READER_WATCH,
    StatusReader,
    Unreachable,
    is_status_read,
    status_reader,
)

logger = logging.getLogger(__name__)

__all__ = ["WatchRefused", "watch_order"]

# Each way the reader can be out of reach, as the refusal the tool words.
_UNREACHABLE = {
    "no_connection": "no_connection",
    "not_connected": "not_connected",
    "unsupported": "vendor_unwatchable",
    "no_grant": "no_grant",
}


@dataclass
class WatchRefused(Exception):
    """Why the order cannot be watched, as a key the tool words and its fields."""

    reason: str
    fields: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.reason


def _reads(
    vendor: str, adapter: OrderAdapter, account: str, order_id: str
) -> list[StatusQuery]:
    """The reads that could list ``order_id``, without any that is not a read."""
    return [
        q for q in adapter.watch_queries(account, order_id) if is_status_read(vendor, q)
    ]


async def _find(
    reader: StatusReader, queries: list[StatusQuery], order_id: str
) -> tuple[StatusQuery, VendorOrder] | None:
    """The first read that lists ``order_id``, and the order as it lists it.

    A read the vendor refuses only means the id is not of that kind, or the
    account is spelled another way for it, so it is passed over. When no read
    lists the order and none was complete, nothing was looked at in full,
    which is a failure, not an absence.
    """
    answered = False
    client = reader.client()
    async with client:
        for query in queries:
            try:
                listing = await reader.read(client, query)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.info("[OrderWatch] read %r found nothing: %s", query.tool, e)
                continue
            found = next(
                (o for o in listing.orders if o.vendor_order_id == order_id), None
            )
            if found is not None:
                return query, found
            # A list cut short is no evidence the order is missing from it.
            answered = answered or listing.complete
    if not answered:
        raise WatchRefused("failed", {})
    return None


async def watch_order(
    *,
    user_id: str,
    thread_id: str,
    brokerage: str,
    account: str,
    order_id: str,
) -> dict[str, Any]:
    """The ``watch`` attempt for ``order_id`` in ``account``, recorded open.

    Raises :class:`WatchRefused` when the order cannot be read or has ended.
    """
    reader = await status_reader(
        user_id,
        brokerage,
        reader=READER_WATCH,
        by_vendor=True,
        serves=lambda vendor, adapter: bool(_reads(vendor, adapter, account, order_id)),
    )
    if isinstance(reader, Unreachable):
        named = reader.server
        if reader.reason == "unsupported":
            named = reader.vendor or reader.server
        raise WatchRefused(_UNREACHABLE[reader.reason], {"brokerage": named})

    queries = _reads(reader.vendor, reader.adapter, account, order_id)
    hit = await _find(reader, queries, order_id)
    if hit is None:
        raise WatchRefused("not_found", {"account": account})
    query, listed = hit
    outcome = listed.outcome
    if outcome.status not in OPEN_STATUSES:
        raise WatchRefused("ended", {"status": str(outcome.status)})

    order = listed.order or BrokerOrder(vendor=reader.vendor, instrument=listed.instrument)
    # The account the read was made in is what every later read is built from,
    # whatever spelling the listed row itself carried.
    order = order.model_copy(update={"account_ref": account})
    row = await insert_watch(
        user_id=user_id,
        thread_id=thread_id,
        server=reader.server,
        vendor=reader.vendor,
        tool=query.tool,
        account_ref=account,
        order=order.to_json(),
        outcome=outcome.model_copy(update={"vendor_order_id": order_id}),
    )
    if row.get("status") not in OPEN_STATUSES:
        # The thread watched this order before and the ledger saw it end;
        # the row is the order's, so its end stands.
        raise WatchRefused("ended", {"status": str(row.get("status"))})
    # A row this thread watched before keeps the fill it last read, which a
    # watch nothing waited on stopped refreshing; the wakeup counts from what
    # this read saw, so a fill before the ask is not its fire.
    # The larger of the two, as the reconciler keeps fills: a read can lag the
    # fill the row already recorded.
    if outcome.filled_qty is not None:
        stored = row.get("filled_qty")
        filled = max(float(outcome.filled_qty), float(stored)) if stored is not None else outcome.filled_qty
        row = {**row, "filled_qty": filled}
    return row
