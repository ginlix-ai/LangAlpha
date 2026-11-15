"""Charge what the relay metered to the thread its call ran for.

The relay records paid vendor calls in ``metered_vendor_calls``
(``services/egress/metering.py``) and charges each as it settles, on a usage
row of its own on the thread the call's command ran for, in the transaction
that settles it. A call with no thread known, and a hold no settle ever
reached, is charged when one of the user's turns next starts, to a live
thread of the workspace the call ran in, else to that turn's thread. The
vendor's credits are the count, priced per unit in the web manifest's
``auxiliary_tools``.

These rows are billed the way between-turn work is: a turn's credit gate does
not count them, and the agent sees them only on the command's result, or on
its end notice when it was handed off.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.server.services.egress.metering import Meter

logger = logging.getLogger(__name__)

#: The ``between_turns`` marker of a paid-call usage row.
PAID_DATA_CALLS = "paid_data_calls"

# The relay settles a call after its last byte went out, so a command that
# read the reply can return first. Settling is a few writes; past this a
# call is still running (a process the command left behind) and is charged
# when it ends, unlisted.
_SETTLE_WAIT_S = 3.0
_SETTLE_POLL_S = 0.2


def _per_unit(name: str) -> float:
    from src.utils.tracking.infrastructure_costs import INFRASTRUCTURE_PRICING

    return float((INFRASTRUCTURE_PRICING.get(name) or {}).get("credits_per_use") or 0)


def _meter(vendor: str) -> Meter | None:
    # Imported here: the platform accounts' meters import the module that
    # imports this one.
    from src.server.services.egress.platform_accounts import meter_for_vendor

    return meter_for_vendor(vendor)


def user_credits(meter: Meter, vendor_credits: float) -> float:
    """What ``vendor_credits`` of ``meter``'s vendor costs the user in our credits."""
    return math.ceil(vendor_credits) * _per_unit(meter.tracking)


def _units(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Tracking name -> whole vendor credits, each call rounded up as it was
    charged."""
    units: dict[str, int] = {}
    for row in rows:
        if (meter := _meter(row["vendor"])) is not None:
            units[meter.tracking] = units.get(meter.tracking, 0) + math.ceil(
                row["vendor_credits"]
            )
    return units


async def _charge(conn: Any, user_id: str, thread_id: str, workspace_id: str, rows: list) -> None:
    """Write the usage row charging ``rows`` to a thread, on ``conn``."""
    from src.server.services.persistence.usage import persist_between_turns

    units = {name: n for name, n in _units(rows).items() if n > 0}
    if units:
        await persist_between_turns(
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            verb=PAID_DATA_CALLS,
            tool_usage=units,
            conn=conn,
        )


async def settle_call(
    user_id: str, hold_id: int, *, credits: float | None, job_ref: str | None
) -> None:
    """Settle a paid call and charge it to its thread in one transaction.
    Raises on failure, which leaves the hold to be taken as stale."""
    from src.server.database.metered_vendor_calls import settle, stamp_collected
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn, conn.transaction():
        row = await settle(conn, hold_id, credits=credits, job_ref=job_ref)
        if row is not None and row["thread_workspace_id"]:
            await _charge(conn, user_id, row["thread_id"], row["thread_workspace_id"], [row])
            await stamp_collected(conn, [row["id"]])


async def charge_owed(user_id: str, thread_id: str, workspace_id: str) -> None:
    """Charge, as a turn starts, what ``user_id`` spent that no settle
    charged, in one transaction: a stale hold on its own thread, and a call
    whose thread is unknown or gone on the most recently active thread of the
    workspace it ran in, else on the starting one. The agent is not told: the
    turn did not make these calls.

    Run whenever anything is owed, not only while a server is metered, so
    calls made before an operator removed the server are still charged; a
    turn with nothing owed pays one indexed probe.
    """
    from src.server.database.metered_vendor_calls import (
        has_owed,
        stamp_collected,
        take_owed,
    )
    from src.server.database.pool import get_db_connection

    try:
        if not await has_owed(user_id):
            return
        async with get_db_connection() as conn, conn.transaction():
            rows = await take_owed(conn, user_id)
            by_thread: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for row in rows:
                if row["thread_workspace_id"]:
                    key = (row["thread_id"], row["thread_workspace_id"])
                elif row.get("workspace_thread_id"):
                    key = (row["workspace_thread_id"], row["workspace_id"])
                else:
                    key = (thread_id, workspace_id)
                by_thread.setdefault(key, []).append(row)
            for (thread, workspace), taken in by_thread.items():
                await _charge(conn, user_id, thread, workspace, taken)
            await stamp_collected(conn, [row["id"] for row in rows])
    except Exception:  # noqa: BLE001 - rolled back, the rows stay owed
        logger.exception("[metered_charges] could not charge what %s owes", user_id)
        return
    if rows:
        logger.info(
            "[metered_charges] charged %d owed call(s) user_id=%s: %s",
            len(rows),
            user_id,
            _units(rows),
        )


async def command_charges(user_id: str, call_id: str) -> str:
    """The line ``user_id``'s command's result carries naming what its paid
    calls cost, or "" when there were none. A plain read: each call was
    charged as it settled. Calls still held are waited for briefly (see
    ``_SETTLE_WAIT_S``).
    """
    from src.server.database.metered_vendor_calls import calls_of

    deadline = time.monotonic() + _SETTLE_WAIT_S
    try:
        while True:
            rows = await calls_of(user_id, call_id)
            if not any(r["status"] == "held" for r in rows) or time.monotonic() >= deadline:
                break
            await asyncio.sleep(_SETTLE_POLL_S)
    except Exception:  # noqa: BLE001 - the line is a report; the charge stands
        logger.warning("[metered_charges] could not read the calls of %s", call_id, exc_info=True)
        return ""
    charged_rows = [r for r in rows if r["status"] != "held"]
    totals: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in charged_rows:
        totals.setdefault((row["vendor"], row["tool"]), []).append(row)
    parts: list[str] = []
    charged = 0.0
    for (vendor, tool), taken in sorted(totals.items()):
        meter = _meter(vendor)
        units = sum(math.ceil(r["vendor_credits"]) for r in taken)
        if meter is None or units <= 0:
            continue
        charged += units * _per_unit(meter.tracking)
        parts.append(f"{tool} x{len(taken)} = {units:g} {meter.label} credits")
    if not parts:
        return ""
    return f"Paid data calls in this run: {'; '.join(parts)} ({charged:g} credits charged)."
