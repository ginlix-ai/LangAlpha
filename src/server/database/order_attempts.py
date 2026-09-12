"""The order attempt ledger: the authorization state of every order call.

Every transition is a guarded ``UPDATE ... WHERE status = <from> RETURNING``,
so two workers racing on one attempt cannot both win: the loser gets no row
back and reads that as the refusal it is. Nothing here caches, and no caller
may keep an attempt's state in process memory -- the row is the only truth
about whether a call has already been sent to a vendor.
"""

from __future__ import annotations

import logging
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime
from typing import Any, Sequence
from uuid import UUID

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.services.brokerage_orders import AttemptStatus, OrderOutcome
from src.server.services.brokerage_orders.models import (
    CREATING_ACTIONS,
    TERMINAL_STATUSES,
)
from src.server.utils.pg_sanitize import SafeJson, strip_pg_nul_str

logger = logging.getLogger(__name__)

# Every column, in one place: the reads all select the whole row and the
# middleware's port shapes it, so a new column reaches every surface at once.
_COLUMNS = """
    attempt_id, user_id, workspace_id, thread_id, conversation_response_id,
    turn_index, message_id, tool_call_id, server, vendor, tool, action, mode,
    account_ref, args, args_sha256, order_json, approval_required, status,
    decided_at,
    decision_message, executed_at, dispatched_at, completed_at, vendor_order_id, route,
    action_url, filled_qty, avg_fill_price, fees, parent_attempt_id,
    result_sha256, failure, created_at, updated_at
"""

# What a decision means in the ledger's vocabulary.
_DECISIONS = {
    "approve": AttemptStatus.APPROVED,
    "reject": AttemptStatus.REJECTED_BY_USER,
}

# One page bound, here rather than at the router: the ceiling protects the
# index this list is served from, so it belongs to the statement that uses it.
PAGE_SIZE = 50
_MAX_PAGE_SIZE = 200


def row_as_json(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """UUIDs as strings at the boundary, so a row is JSON-serializable as read.

    Shared with ``order_reconciliation``: the two modules write different
    statements against the same table and hand their rows to the same readers.
    """
    if row is None:
        return None
    return {k: (str(v) if isinstance(v, UUID) else v) for k, v in row.items()}


async def insert_attempt(
    *,
    user_id: str,
    message_id: str,
    tool_call_id: str,
    vendor: str,
    workspace_id: str | None = None,
    thread_id: str | None = None,
    conversation_response_id: str | None = None,
    turn_index: int | None = None,
    server: str | None = None,
    tool: str | None = None,
    action: str | None = None,
    mode: str | None = None,
    account_ref: str | None = None,
    args: dict[str, Any] | None = None,
    args_sha256: str | None = None,
    order: dict[str, Any] | None = None,
    approval_required: bool = False,
    status: AttemptStatus = AttemptStatus.PROPOSED,
    parent_attempt_id: str | None = None,
) -> dict[str, Any]:
    """Write the attempt for one tool call, or return the one already there.

    Proposing is idempotent by the call's message and id because the node that
    proposes re-runs on every resume: the conflict arm returns the existing row
    without touching its status, so a resume can never walk a consumed attempt
    back to ``proposed``. It leaves ``updated_at`` alone too, because that
    column is the reconciliation sweep's clock and a resume is not a state
    change.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                INSERT INTO order_attempts (
                    user_id, workspace_id, thread_id, conversation_response_id,
                    turn_index, message_id, tool_call_id, server, vendor, tool,
                    action, mode, account_ref, args, args_sha256, order_json,
                    approval_required, status, parent_attempt_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (user_id, message_id, tool_call_id)
                DO UPDATE SET updated_at = order_attempts.updated_at
                RETURNING {_COLUMNS}
                """,
                (
                    user_id,
                    workspace_id,
                    thread_id,
                    conversation_response_id,
                    turn_index,
                    strip_pg_nul_str(message_id),
                    strip_pg_nul_str(tool_call_id),
                    strip_pg_nul_str(server),
                    strip_pg_nul_str(vendor),
                    strip_pg_nul_str(tool),
                    strip_pg_nul_str(action),
                    strip_pg_nul_str(mode),
                    strip_pg_nul_str(account_ref),
                    SafeJson(args) if args is not None else None,
                    strip_pg_nul_str(args_sha256),
                    SafeJson(order) if order is not None else None,
                    approval_required,
                    str(status),
                    parent_attempt_id,
                ),
            )
            return row_as_json(await cur.fetchone())  # type: ignore[return-value]


async def decide_attempt(
    attempt_id: str, decision: str, message: str | None = None
) -> dict[str, Any] | None:
    """Record the user's verdict, or None when the row was not still proposed.

    Guarded on ``proposed`` so a stale resume, a duplicated answer or a second
    worker cannot re-decide an attempt that has already moved.
    """
    status = _DECISIONS.get(decision)
    if status is None:
        raise ValueError(f"unknown order decision {decision!r}")
    # A rejection ends the order here: no vendor will ever answer for it, so
    # the verdict is also the completion and the receipt has to say when.
    completed = status is AttemptStatus.REJECTED_BY_USER
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE order_attempts
                   SET status = %s,
                       decided_at = NOW(),
                       decision_message = %s,
                       completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END,
                       updated_at = NOW()
                 WHERE attempt_id = %s AND status = 'proposed'
                RETURNING {_COLUMNS}
                """,
                (str(status), strip_pg_nul_str(message), completed, attempt_id),
            )
            return row_as_json(await cur.fetchone())


async def consume_attempt(attempt_id: str) -> dict[str, Any] | None:
    """Take the single execution an approval buys, or None if it is spent.

    This is the consume-once. A retry after a worker loss, a replayed frame and
    a second concurrent handler all reach this statement, and exactly one of
    them finds the row ``approved``.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE order_attempts
                   SET status = 'submitting',
                       executed_at = NOW(),
                       updated_at = NOW()
                 WHERE attempt_id = %s AND status = 'approved'
                RETURNING {_COLUMNS}
                """,
                (attempt_id,),
            )
            return row_as_json(await cur.fetchone())


async def claim_dispatch(attempt_id: str) -> str | None:
    """Mark the frame as leaving the host, once; None if it already has.

    The relay calls this between verifying the execution token and sending.
    ``consume_attempt`` spends the approval before the frame is built; this
    spends the frame itself, so a duplicate of one signed request inside the
    token's lifetime finds the claim taken and never reaches the vendor.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE order_attempts
                   SET dispatched_at = NOW(),
                       updated_at = NOW()
                 WHERE attempt_id = %s
                   AND status = 'submitting'
                   AND dispatched_at IS NULL
                RETURNING attempt_id
                """,
                (attempt_id,),
            )
            row = await cur.fetchone()
            return str(row["attempt_id"]) if row else None


async def complete_attempt(
    attempt_id: str, outcome: OrderOutcome, result_sha256: str | None = None
) -> dict[str, Any] | None:
    """Settle a consumed attempt with the vendor's answer.

    Guarded on ``submitting``: only the worker that consumed the attempt can
    complete it, and a late second answer for the same call is dropped rather
    than allowed to overwrite the first.

    The fill columns COALESCE rather than assign: a vendor that answers a
    placement with no fill yet must not blank a number some later read already
    put on the row.
    """
    failure = outcome.failure.to_json() if outcome.failure is not None else None
    # Only a terminal answer dates the attempt: a state the vendor can still
    # move, ``unknown`` included, would date the order to the answer we could
    # not read rather than to the one that settled it.
    completed = outcome.status in TERMINAL_STATUSES
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE order_attempts
                   SET status = %s,
                       vendor_order_id = COALESCE(%s, vendor_order_id),
                       -- Cast: psycopg binds a Json wrapper as ``json`` and
                       -- COALESCE has no common type with a ``jsonb`` column.
                       route = COALESCE(%s::jsonb, route),
                       action_url = COALESCE(%s, action_url),
                       filled_qty = COALESCE(%s, filled_qty),
                       avg_fill_price = COALESCE(%s, avg_fill_price),
                       fees = COALESCE(%s::jsonb, fees),
                       result_sha256 = COALESCE(%s, result_sha256),
                       failure = %s,
                       completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END,
                       updated_at = NOW()
                 WHERE attempt_id = %s AND status = 'submitting'
                RETURNING {_COLUMNS}
                """,
                (
                    str(outcome.status),
                    strip_pg_nul_str(outcome.vendor_order_id),
                    SafeJson(dict(outcome.route)) if outcome.route else None,
                    strip_pg_nul_str(outcome.action_url),
                    outcome.filled_qty,
                    outcome.avg_fill_price,
                    SafeJson(outcome.fees.to_json()) if outcome.fees else None,
                    strip_pg_nul_str(result_sha256),
                    SafeJson(failure) if failure is not None else None,
                    completed,
                    attempt_id,
                ),
            )
            return row_as_json(await cur.fetchone())


async def refuse_attempt(attempt_id: str, reason: str) -> dict[str, Any] | None:
    """End an attempt that never reached a vendor, naming why.

    The absence of an answer is one of these: an interrupt that came back
    without a decision for this attempt is refused, never approved.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE order_attempts
                   SET status = 'refused',
                       failure = %s,
                       decided_at = COALESCE(decided_at, NOW()),
                       completed_at = NOW(),
                       updated_at = NOW()
                 WHERE attempt_id = %s AND status IN ('proposed', 'approved')
                RETURNING {_COLUMNS}
                """,
                (
                    SafeJson({"kind": "policy", "message": reason}),
                    attempt_id,
                ),
            )
            return row_as_json(await cur.fetchone())


_CONNECTION_CHANGED = {
    "kind": "policy",
    "code": "connection_changed",
    "message": (
        "the brokerage connection changed after this order was proposed, so it "
        "was not sent; propose it again"
    ),
}


async def refuse_attempts_on_connection_change(
    user_id: str, server: str, *, conn
) -> list[str]:
    """Refuse this user's orders at ``server`` that no frame has carried yet.

    A reconnect can put another brokerage login behind the same row, and the
    relay matches an order by its arguments, not by the account it was shown
    for. Run inside the connection write's transaction: the relay reads the
    bearer before it claims the dispatch, so a bearer from the new login always
    meets a refused row. A dispatched row may be held by the vendor and is
    reconciliation's to settle.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE order_attempts
               SET status = 'refused',
                   failure = %s,
                   decided_at = COALESCE(decided_at, NOW()),
                   completed_at = NOW(),
                   updated_at = NOW()
             WHERE user_id = %s AND server = %s
               AND (status IN ('proposed', 'approved')
                    OR (status = 'submitting' AND dispatched_at IS NULL))
            RETURNING attempt_id
            """,
            (SafeJson(_CONNECTION_CHANGED), user_id, server),
        )
        refused = [str(row["attempt_id"]) for row in await cur.fetchall()]
    if refused:
        logger.info(
            "[ORDERS] user=%s server=%s: connection changed, refused attempts %s",
            user_id, server, refused,
        )
    return refused


_THREAD_DELETED = {
    "kind": "policy",
    "code": "thread_deleted",
    "message": "the thread this order was proposed in was deleted, so it was not sent",
}


async def refuse_attempts_on_thread_delete(thread_id: str, *, conn=None) -> list[str]:
    """Refuse the orders a deleted thread still held for a verdict or a call.

    With the thread gone no card can answer a proposal and no run can spend an
    approval, and the sweep never lapses a proposal whose run is waiting on the
    user. Run on the delete's fenced session, so no run on the thread can be
    admitted between this and the delete.
    """

    async def _execute(conn) -> list[str]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE order_attempts
                   SET status = 'refused',
                       failure = %s,
                       decided_at = COALESCE(decided_at, NOW()),
                       completed_at = NOW(),
                       updated_at = NOW()
                 WHERE thread_id = %s AND status IN ('proposed', 'approved')
                RETURNING attempt_id
                """,
                (SafeJson(_THREAD_DELETED), thread_id),
            )
            return [str(row["attempt_id"]) for row in await cur.fetchall()]

    if conn is not None:
        refused = await _execute(conn)
    else:
        async with get_db_connection() as own:
            refused = await _execute(own)
    if refused:
        logger.info(
            "[ORDERS] thread=%s: deleted, refused attempts %s", thread_id, refused
        )
    return refused


_TURN_REPLACED = {
    "kind": "policy",
    "code": "turn_replaced",
    "message": (
        "the turn this order was proposed in was edited or regenerated, so it "
        "was not sent"
    ),
}


async def refuse_attempts_on_fork(
    thread_id: str, from_turn: int, *, conn
) -> list[str]:
    """Refuse the orders still waiting under the turns a fork is about to delete.

    The ledger keeps no foreign key to the response row, so these rows outlive
    it where no sweep reaches them: an abandoned proposal is found through its
    run's row, and an approval never lapses while a proposal shares its run.
    Run in the fork's transaction and before its truncation: the subquery reads
    the rows the truncation deletes, and a fork that rolls back takes the
    refusal with it.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE order_attempts
               SET status = 'refused',
                   failure = %s,
                   decided_at = COALESCE(decided_at, NOW()),
                   completed_at = NOW(),
                   updated_at = NOW()
             WHERE status IN ('proposed', 'approved')
               AND conversation_response_id IN (
                     SELECT conversation_response_id FROM conversation_responses
                      WHERE conversation_thread_id = %s AND turn_index >= %s
                   )
            RETURNING attempt_id
            """,
            (SafeJson(_TURN_REPLACED), thread_id, from_turn),
        )
        refused = [str(row["attempt_id"]) for row in await cur.fetchall()]
    if refused:
        logger.info(
            "[ORDERS] thread=%s turn>=%s: turn replaced, refused attempts %s",
            thread_id, from_turn, refused,
        )
    return refused


async def get_attempt(attempt_id: str) -> dict[str, Any] | None:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM order_attempts WHERE attempt_id = %s",
                (attempt_id,),
            )
            return row_as_json(await cur.fetchone())


async def fetch_attempt_status(attempt_id: str) -> dict[str, Any] | None:
    """The narrow read the relay makes on every order frame.

    Deliberately not ``get_attempt``: the relay decides on the status and the
    call's identity, and reading the whole row would put the redacted arguments
    and the normalized order on a path that has no use for either.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT attempt_id, user_id, status, tool, tool_call_id,
                       vendor, args_sha256
                  FROM order_attempts
                 WHERE attempt_id = %s
                """,
                (attempt_id,),
            )
            return row_as_json(await cur.fetchone())


async def latest_attempt_for_vendor_order(
    user_id: str, vendor: str, vendor_order_id: str, *, account_ref: str | None
) -> str | None:
    """The newest attempt of this user's that owns a vendor order id, or None.

    An amend names an order somebody already placed, and this is what turns
    that name back into the attempt that placed it, so a cancel and its
    placement read as one order's history rather than two unrelated rows. The
    account is part of that name: an order id is unique only within one.
    """
    if not (vendor_order_id or "").strip() or not (vendor or "").strip():
        return None
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT attempt_id FROM order_attempts
                 WHERE user_id = %s AND vendor = %s
                   AND account_ref IS NOT DISTINCT FROM %s
                   AND vendor_order_id = %s
                   AND action = ANY(%s)
                 ORDER BY created_at DESC, attempt_id DESC
                 LIMIT 1
                """,
                (
                    user_id,
                    vendor,
                    account_ref,
                    vendor_order_id,
                    [str(a) for a in CREATING_ACTIONS],
                ),
            )
            row = await cur.fetchone()
    return None if row is None else str(row[0])


def decode_cursor(cursor: str | None) -> tuple[datetime, str] | None:
    """The ``(created_at, attempt_id)`` keyset a cursor names, or None for none.

    The token is base64url rather than the bare ``<created_at>|<attempt_id>``
    it wraps, because that timestamp carries a ``+`` for its UTC offset and an
    unencoded ``+`` in a query string arrives as a space. Malformed raises
    instead of falling back to no keyset: serving page one again for a cursor
    the caller believed in is an infinite loop, not an error they can see.
    """
    if not cursor:
        return None
    try:
        raw = urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("malformed cursor") from exc
    created, _, attempt_id = raw.partition("|")
    try:
        return datetime.fromisoformat(created), str(UUID(attempt_id))
    except ValueError as exc:
        raise ValueError("malformed cursor") from exc


def encode_cursor(row: dict[str, Any]) -> str:
    created = row.get("created_at")
    stamp = created.isoformat() if hasattr(created, "isoformat") else str(created)
    raw = f"{stamp}|{row.get('attempt_id')}"
    return urlsafe_b64encode(raw.encode()).decode().rstrip("=")


async def list_attempts(
    user_id: str,
    *,
    vendor: str | None = None,
    mode: str | None = None,
    status: Sequence[str] | None = None,
    asset_class: str | None = None,
    thread_id: str | None = None,
    cursor: str | None = None,
    limit: int = PAGE_SIZE,
) -> tuple[list[dict[str, Any]], str | None]:
    """One page of a user's attempts, newest first, plus the next cursor.

    Keyset rather than OFFSET: the list is ordered by the same
    ``(user_id, created_at DESC)`` index it filters on, and a row inserted
    mid-scroll would otherwise shift every page after it.
    """
    limit = max(1, min(int(limit or PAGE_SIZE), _MAX_PAGE_SIZE))
    where = ["user_id = %s"]
    params: list[Any] = [user_id]
    if vendor:
        where.append("vendor = %s")
        params.append(vendor)
    if mode:
        where.append("mode = %s")
        params.append(mode)
    if status:
        where.append("status = ANY(%s)")
        params.append(list(status))
    if asset_class:
        where.append("order_json ->> 'asset_class' = %s")
        params.append(asset_class)
    if thread_id:
        where.append("thread_id = %s")
        params.append(thread_id)
    keyset = decode_cursor(cursor)
    if keyset is not None:
        where.append("(created_at, attempt_id) < (%s, %s)")
        params.extend(keyset)
    params.append(limit + 1)

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_COLUMNS} FROM order_attempts
                 WHERE {" AND ".join(where)}
                 ORDER BY created_at DESC, attempt_id DESC
                 LIMIT %s
                """,
                params,
            )
            rows = [row_as_json(row) for row in await cur.fetchall()]
    if len(rows) > limit:
        return rows[:limit], encode_cursor(rows[limit - 1])  # type: ignore[arg-type]
    return rows, None  # type: ignore[return-value]
