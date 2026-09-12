"""The reads and the guarded writes reconciliation makes on the order ledger.

Separate from ``order_attempts`` because the two ask opposite questions. That
module is the authorization path: one attempt at a time, addressed by id, each
transition guarded on the state the caller believes it is in. This one sweeps:
it looks across users for rows nobody is holding, and it writes what a vendor
said long after the turn that placed the order has gone.

The write is forward-only by construction. ``record_observation`` derives the
states a row may currently be in from the state being written, and a terminal
state is in nobody's set -- so a settled attempt cannot be reopened by a late
list read, and a row that moved on between the read and the write keeps
whichever answer got further.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row

from src.server.database.order_attempts import row_as_json
from src.server.database.pool import get_db_connection
from src.server.database.session_lock import release_session_lock
from src.server.services.brokerage_capabilities import OrderAction
from src.server.services.brokerage_orders import OPEN_STATUSES, AttemptStatus
from src.server.services.brokerage_orders.models import (
    CREATING_ACTIONS,
    TERMINAL_STATUSES,
    UNLISTED_ACTIONS,
)
from src.server.utils.pg_sanitize import SafeJson, strip_pg_nul_str

logger = logging.getLogger(__name__)

# The pass lock covers the SQL-only head of a pass, where two workers would
# otherwise double-refuse the same row or hand each other the same candidates.
# It is deliberately not held across the vendor calls: those are per account and
# slow, and one brokerage stalling under a fleet-wide key stopped reconciliation
# for every other user until it answered.
PASS_LOCK_KEY = "order_attempts:reconcile"


def group_lock_key(user_id: str, server: str) -> str:
    """The lock one account's vendor reads are held under.

    What the fleet key was actually protecting is a pair of workers asking one
    brokerage the same question and racing to write the two answers, and that
    question is only ever the same within a single ``(user, server)``.
    """
    return f"{PASS_LOCK_KEY}:{user_id}:{server}"

_CANDIDATE_COLUMNS = """
    attempt_id, user_id, workspace_id, thread_id, server, vendor, tool, action,
    mode, account_ref, order_json, status, vendor_order_id, route,
    executed_at, created_at, updated_at
"""

# ``OPEN_STATUSES`` is the sweep's candidate set: a row in one of those states
# is one the host has stopped watching, and only a read tells us what the order
# did next. It is defined beside the lifecycle itself because the ledger reads
# the same set to decide that such a row is not finished.

# The actions that end an order they did not create. One of these that the
# vendor answered ``cancelled`` is the host's own evidence about the order it
# named, and it is evidence no later list read can produce.
CANCELLING_ACTIONS: tuple[OrderAction, ...] = (OrderAction.CANCEL, OrderAction.UNSTAGE)

# How far an attempt can travel, so a write can be refused unless it moves the
# row forward. Every terminal state shares the top rank: they do not order
# among themselves, and the only thing that matters is that nothing overtakes
# one. ``submitting`` is the bottom because it is the state that knows least.
_RANK: dict[AttemptStatus, int] = {
    AttemptStatus.SUBMITTING: 0,
    AttemptStatus.UNKNOWN: 1,
    AttemptStatus.SUBMITTED: 2,
    AttemptStatus.PENDING_CONFIRM: 2,
    AttemptStatus.WORKING: 3,
    AttemptStatus.PARTIALLY_FILLED: 4,
    **dict.fromkeys(TERMINAL_STATUSES, 5),
}

# What a status read teaches a row that is not its state: how much of it has
# filled and at what price, and the instrument the request could only name by
# an opaque vendor id. The write carries this whether or not the state moved,
# because a read can learn any of it without the row moving at all -- a partial
# fill that grew, or a staged instruction the vendor lists under a symbol the
# proposal never had.
#
# COALESCE, never assignment: a listing that omits a fill is silent about it,
# not evidence that the fill was undone. GREATEST for the quantity itself,
# which is the same silence one step further on: a vendor snapshot taken before
# the last fill would otherwise walk a recorded fill backwards, and no venue
# ever reports a fill that shrank. It keeps the COALESCE it replaces, because
# GREATEST ignores a NULL and returns one only when every argument is NULL.
#
# The average price and the fees are measured against a quantity, so they move
# only when the incoming quantity is not behind the one already recorded.
# Keeping the larger quantity while taking the smaller fill's average would
# state a fill that never happened: seven shares at the average of four. Either
# side being NULL makes the comparison NULL and falls through to the COALESCE,
# so a first fill still lands and a listing silent about one still erases
# nothing.
# Indented to sit under the ``SET`` at the depth the statement below uses.
_LEARNED_SET = """filled_qty = GREATEST(%s, filled_qty),
                       avg_fill_price = CASE
                           WHEN %s < filled_qty THEN avg_fill_price
                           ELSE COALESCE(%s, avg_fill_price)
                       END,
                       fees = CASE
                           WHEN %s < filled_qty THEN fees
                           ELSE COALESCE(%s::jsonb, fees)
                       END,
                       order_json = CASE
                           WHEN %s::jsonb IS NULL THEN order_json
                           ELSE jsonb_set(
                               COALESCE(order_json, '{}'::jsonb),
                               '{instrument}', %s::jsonb, true
                           )
                       END"""


def forward_from(status: AttemptStatus) -> tuple[AttemptStatus, ...]:
    """The states a write of ``status`` may overtake, and no others.

    Terminal states have the top rank and so appear in nobody's set: an attempt
    that has settled is never rewritten by a list read that arrived after it.
    Neither is a row that has already gone further on its own, because a vendor
    list a second old can still describe a working order that has since filled.
    """
    rank = _RANK.get(status)
    if rank is None:
        return ()
    return tuple(
        s for s, r in _RANK.items() if r < rank and s not in TERMINAL_STATUSES
    )


@dataclass(frozen=True, slots=True)
class Observation:
    """What one vendor read learned about an attempt, or why it learned nothing.

    ``status`` is None when there is nothing to record, and ``reason`` then says
    why; otherwise ``reason`` names the evidence behind the status, which is
    what the pass logs. ``ambiguous`` marks the single unresolved case a person
    has to settle by hand, so it is the one that is logged louder.

    ``from_absence`` marks a status founded on the order not being in a listing
    rather than on anything the vendor said about it. That is the one kind of
    evidence a later write can invalidate, so it is the one kind the write has
    to check it still holds.
    """

    status: AttemptStatus | None = None
    reason: str = ""
    ambiguous: bool = False
    from_absence: bool = False
    vendor_order_id: str | None = None
    route: dict[str, str] | None = None
    failure: dict[str, Any] | None = None
    filled_qty: Decimal | None = None
    avg_fill_price: Decimal | None = None
    fees: dict[str, Any] | None = None
    instrument: dict[str, Any] | None = None


async def record_observation(
    attempt_id: str, current: AttemptStatus, observation: Observation
) -> dict[str, Any] | None:
    """Write one vendor reading onto an attempt, or None when the row moved on.

    The guard is derived here rather than passed in, which is what makes the
    invariant a property of the write: a read naming the state the row is
    already in may only enrich it, any other read may overtake only the states
    ``forward_from`` allows, and a terminal state is in no such set. The same
    statement dates the attempt, so a completion time can never land on a state
    the vendor can still move.

    An absence is held to the row it was read from, which is stricter than rank
    alone. ``failed`` from a missing listing shares the top rank with ``filled``
    and so may overtake every open state, but it was only ever evidence about
    the row the sweep selected: the attempt's own call can land its answer in
    the window between that select and this write, and a ``failed`` laid over
    the ``submitted`` it just wrote would terminalize an order the brokerage
    accepted, unrecoverably, since nothing overtakes a terminal in turn. So such
    a write must find the row in the state it was read in, or do nothing.
    """
    status = observation.status
    if status is None:
        return None
    allowed = (
        (current,)
        if status == current or observation.from_absence
        else forward_from(status)
    )
    if not allowed:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE order_attempts
                   SET status = %s,
                       vendor_order_id = COALESCE(vendor_order_id, %s),
                       -- Cast: psycopg binds a Json wrapper as ``json`` and
                       -- COALESCE has no common type with a ``jsonb`` column.
                       route = COALESCE(NULLIF(route, '{{}}'::jsonb), %s::jsonb),
                       -- Assigned, as the completing write does: the only
                       -- failure an open row holds is a lost answer's, and a
                       -- read that names the order's state is that answer.
                       failure = %s::jsonb,
                       {_LEARNED_SET},
                       -- NOW(), not COALESCE: the row this overtakes was open,
                       -- so any completion time already on it predates the
                       -- answer that actually settled the order.
                       completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END,
                       updated_at = NOW()
                 WHERE attempt_id = %s AND status = ANY(%s)
                RETURNING {_CANDIDATE_COLUMNS}
                """,
                (
                    str(status),
                    strip_pg_nul_str(observation.vendor_order_id),
                    SafeJson(dict(observation.route)) if observation.route else None,
                    (
                        SafeJson(observation.failure)
                        if observation.failure is not None
                        else None
                    ),
                    # The quantity three times: once to keep it from shrinking,
                    # then once ahead of each figure that was measured with it.
                    observation.filled_qty,
                    observation.filled_qty,
                    observation.avg_fill_price,
                    observation.filled_qty,
                    SafeJson(observation.fees) if observation.fees else None,
                    # The instrument twice: once to decide whether there is
                    # anything to write and once to write it, because
                    # ``jsonb_set`` on a NULL patch blanks the key.
                    *((SafeJson(observation.instrument),) * 2
                      if observation.instrument else (None, None)),
                    status in TERMINAL_STATUSES,
                    attempt_id,
                    [str(s) for s in allowed],
                ),
            )
            return row_as_json(await cur.fetchone())


@asynccontextmanager
async def _try_session_lock(key: str) -> AsyncIterator[bool]:
    """Hold a session advisory lock, or yield False when someone else has it.

    Session-scoped rather than transaction-scoped: a holder makes network calls
    between its statements, and wrapping those in a transaction would pin a
    connection inside an open snapshot for the length of a brokerage's latency.
    """
    async with get_db_connection() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                    (key,),
                )
                row = await cur.fetchone()
        except BaseException:
            # The lock may already have been granted: a session lock survives a
            # cancelled fetch, so release it rather than pool a locked session.
            await release_session_lock(conn, key)
            raise
        if not (row and row[0]):
            yield False
            return
        try:
            yield True
        finally:
            await release_session_lock(conn, key)


@asynccontextmanager
async def reconcile_pass_lock() -> AsyncIterator[bool]:
    """Hold the single-runner lock for a pass head, or False when it is held."""
    async with _try_session_lock(PASS_LOCK_KEY) as held:
        yield held


@asynccontextmanager
async def reconcile_group_lock(user_id: str, server: str) -> AsyncIterator[bool]:
    """Hold one account's read lock, or False when a sibling worker has it.

    A group another worker is already reading is skipped rather than waited on,
    the same way a pass is. Its rows keep their ``swept_at`` stamp, so the next
    pass finds them at the head of the queue rather than losing them.
    """
    async with _try_session_lock(group_lock_key(user_id, server)) as held:
        yield held


async def list_stale_attempts(
    *,
    submitting_grace_seconds: float,
    open_after_seconds: float,
    limit: int,
) -> list[dict[str, Any]]:
    """The next batch of attempts a vendor read could still settle.

    Two populations, and they are stale for different reasons. A ``submitting``
    row means the call left the relay and the worker never wrote the answer, so
    it is stale once the grace window says no answer is coming. An open row was
    answered, and is stale once it has gone longer than a poll interval without
    anyone asking the vendor what happened next -- which is how a working order
    becomes filled with no user in the loop.

    An ``unknown`` row with no vendor order id joins the first population: the
    call was answered with nothing this build could read, or its answer was
    lost after the frame left the host, and either way the vendor's own list
    is the only place the truth is. Any other open row with no vendor order id
    is skipped: there is nothing to look it up by, and its own turn already
    recorded whatever the vendor said. So is any row whose action names an
    event no tool lists: the answer its own call got is all there will ever be.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH picked AS (
                    SELECT attempt_id FROM order_attempts
                     -- COALESCE, because a row that recorded no action at all
                     -- is not one of those and is still the sweep's to settle.
                     WHERE COALESCE(action, '') <> ALL(%s)
                       AND (
                             (
                               (
                                 -- Dispatched, because a ``submitting`` row that
                                 -- never left the host has no order on any book
                                 -- to be found by, and fingerprinting it against
                                 -- the vendor would adopt somebody else's. Those
                                 -- rows are ``fail_undispatched_attempts``, whose
                                 -- window is the longer of this grace and the
                                 -- token's life, so a shorter grace used to hand
                                 -- them here first.
                                 (status = 'submitting' AND dispatched_at IS NOT NULL)
                                 OR (status = 'unknown' AND vendor_order_id IS NULL)
                               )
                               AND updated_at < NOW() - make_interval(secs => %s)
                             )
                             OR (
                                  status = ANY(%s)
                                  AND vendor_order_id IS NOT NULL
                                  AND updated_at < NOW() - make_interval(secs => %s)
                                )
                           )
                     -- Least recently swept first, stamped as taken. A row no
                     -- read can settle is skipped without a write, so ordered
                     -- by updated_at alone it held the head of every pass, and
                     -- a batch of them left every row behind it unread.
                     ORDER BY swept_at ASC NULLS FIRST, updated_at ASC
                     LIMIT %s
                ), swept AS (
                    UPDATE order_attempts a
                       SET swept_at = NOW()
                      FROM picked
                     WHERE a.attempt_id = picked.attempt_id
                    RETURNING a.*
                )
                SELECT {_CANDIDATE_COLUMNS} FROM swept ORDER BY updated_at ASC
                """,
                (
                    [str(a) for a in UNLISTED_ACTIONS],
                    float(submitting_grace_seconds),
                    [str(s) for s in OPEN_STATUSES],
                    float(open_after_seconds),
                    int(limit),
                ),
            )
            return [row_as_json(row) for row in await cur.fetchall()]  # type: ignore[misc]


# Kind ``reconciliation``, like the sweep's other finding: it is this job's
# word about the row, not a vendor's.
_LAPSED = {
    "kind": "reconciliation",
    "code": "never_executed",
    "message": (
        "approved, but the call that would have placed it never ran; nothing "
        "reached the brokerage, and a retry is a new attempt"
    ),
}


async def lapse_unspent_approvals(*, grace_seconds: float) -> list[dict[str, Any]]:
    """Refuse the approvals no call will ever spend, and return the rows refused.

    An approval is spent by its own run's tool call seconds after the answer,
    so one still unspent past the grace window belongs to a run that stopped in
    between, and nothing else would ever consume or settle it. A row whose run
    still has a proposal waiting is left alone: that card holds the whole tool
    step, and answering it spends this approval too.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE order_attempts a
                   SET status = 'refused',
                       failure = %s,
                       decided_at = COALESCE(decided_at, NOW()),
                       completed_at = NOW(),
                       updated_at = NOW()
                 WHERE a.status = 'approved'
                   AND a.updated_at < NOW() - make_interval(secs => %s)
                   -- Without a run there is no telling whether a card still
                   -- holds the step, and a refusal is never undone.
                   AND a.conversation_response_id IS NOT NULL
                   AND NOT EXISTS (
                         SELECT 1 FROM order_attempts p
                          WHERE p.conversation_response_id = a.conversation_response_id
                            AND p.status = 'proposed'
                       )
                RETURNING a.attempt_id, a.user_id, a.vendor, a.tool
                """,
                (SafeJson(_LAPSED), float(grace_seconds)),
            )
            return [row_as_json(row) for row in await cur.fetchall()]  # type: ignore[misc]


# Not ``refused``: nothing decided against this order. The call ran, the host
# lost the worker between consuming the attempt and the relay claiming the
# dispatch, and ``failed`` is the same verdict the sweep writes for a row the
# vendor's book does not hold -- only founded on the host's own record instead
# of on absence from a list.
_UNDISPATCHED = {
    "kind": "reconciliation",
    "code": "never_dispatched",
    "message": (
        "the call that would have placed it never left this host; nothing "
        "reached the brokerage, and a retry is a new attempt"
    ),
}


async def fail_undispatched_attempts(*, grace_seconds: float) -> list[dict[str, Any]]:
    """Settle the placements no frame ever carried, without asking a vendor.

    ``dispatched_at`` is the host's own record that the relay claimed the frame,
    so a ``submitting`` row still missing it never reached the brokerage and no
    listing can say otherwise. Settling it here is what keeps it out of the
    sweep, where it has no vendor order id and would be matched by what was
    ordered -- and an order the person placed by hand after seeing nothing
    happen fingerprints the same as the one this attempt would have placed.

    ``executed_at`` dates the window rather than ``updated_at``, because the
    bound that matters is the execution token's life: fail a row before its
    token expires and the relay would refuse a frame that was merely slow.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE order_attempts
                   SET status = 'failed',
                       failure = %s,
                       completed_at = NOW(),
                       updated_at = NOW()
                 WHERE status = 'submitting'
                   AND dispatched_at IS NULL
                   AND executed_at IS NOT NULL
                   AND executed_at < NOW() - make_interval(secs => %s)
                RETURNING attempt_id, user_id, vendor, tool
                """,
                (SafeJson(_UNDISPATCHED), float(grace_seconds)),
            )
            return [row_as_json(row) for row in await cur.fetchall()]  # type: ignore[misc]


_ABANDONED = {
    "kind": "reconciliation",
    "code": "never_decided",
    "message": (
        "proposed, but its turn ended before anyone approved or rejected it; "
        "nothing reached the brokerage, and a retry is a new attempt"
    ),
}


async def refuse_abandoned_proposals(*, grace_seconds: float) -> list[dict[str, Any]]:
    """Refuse the proposals whose run ended without putting them to the user.

    A run that asks for a verdict ends ``interrupted`` and waits however long
    the user takes, so only a run stopped or finished first abandons one, as a
    Stop between the write and the card does. An errored run keeps its
    proposals: ``/retry`` resumes it from its last checkpoint, which proposes
    the same attempt again and has to find it still waiting.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE order_attempts a
                   SET status = 'refused',
                       failure = %s,
                       decided_at = COALESCE(a.decided_at, NOW()),
                       completed_at = NOW(),
                       updated_at = NOW()
                  FROM conversation_responses r
                 WHERE a.status = 'proposed'
                   AND a.updated_at < NOW() - make_interval(secs => %s)
                   AND r.conversation_response_id = a.conversation_response_id
                   AND r.status IN ('cancelled', 'completed')
                RETURNING a.attempt_id, a.user_id, a.vendor, a.tool
                """,
                (SafeJson(_ABANDONED), float(grace_seconds)),
            )
            return [row_as_json(row) for row in await cur.fetchall()]  # type: ignore[misc]


async def claimed_vendor_order_ids(
    user_id: str, vendor: str, account_ref: str | None
) -> set[str]:
    """Vendor order ids already bound to one of this account's attempts.

    An attempt with no id has to be recognized by what was ordered, and two
    identical orders minutes apart are indistinguishable that way. Excluding
    the ids a placement already owns is what keeps a re-placed order from being
    matched to the earlier attempt that is still open.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT vendor_order_id FROM order_attempts
                 WHERE user_id = %s AND vendor = %s
                   AND account_ref IS NOT DISTINCT FROM %s
                   AND vendor_order_id IS NOT NULL
                   AND action = ANY(%s)
                """,
                (user_id, vendor, account_ref, [str(a) for a in CREATING_ACTIONS]),
            )
            return {row[0] for row in await cur.fetchall()}


async def unanswered_attempts(
    user_id: str, vendor: str, account_ref: str | None
) -> list[dict[str, Any]]:
    """This account's placements that never heard back, any of which a listed order may be.

    The statuses the sweep settles by fingerprint, without its grace window: an
    identical call still waiting on its answer may be the one the vendor took,
    and adopting that order for another row would leave two attempts holding
    one order id once the answer lands.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT attempt_id, order_json, executed_at FROM order_attempts
                 WHERE user_id = %s AND vendor = %s
                   AND account_ref IS NOT DISTINCT FROM %s
                   AND vendor_order_id IS NULL
                   AND action = ANY(%s)
                   AND status IN ('submitting', 'unknown')
                """,
                (user_id, vendor, account_ref, [str(a) for a in CREATING_ACTIONS]),
            )
            return [row_as_json(row) for row in await cur.fetchall()]  # type: ignore[misc]


async def cancelled_by_own_attempt(
    user_id: str,
    vendor: str,
    vendor_order_id: str,
    *,
    account_ref: str | None,
    exclude_attempt_id: str,
) -> dict[str, Any] | None:
    """A confirmed cancel of this user's that named this vendor order, or None.

    Vendor-neutral because the fact is the host's rather than a brokerage's: an
    order the user cancelled and the vendor answered ``cancelled`` to is gone
    even at a vendor whose only word for it afterwards is to stop listing it.
    Scoped to the account, as the claimed ids are: an order id is unique only
    within one, so a cancel in another account says nothing about this order.
    """
    if not (vendor_order_id or "").strip() or not (vendor or "").strip():
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT attempt_id, tool, action, completed_at
                  FROM order_attempts
                 WHERE user_id = %s AND vendor = %s
                   AND account_ref IS NOT DISTINCT FROM %s
                   AND attempt_id <> %s::uuid
                   AND action = ANY(%s)
                   AND status = 'cancelled'
                   AND (
                         vendor_order_id = %s
                         OR order_json ->> 'target_ref' = %s
                       )
                 ORDER BY COALESCE(completed_at, updated_at) DESC
                 LIMIT 1
                """,
                (
                    user_id,
                    vendor,
                    account_ref,
                    exclude_attempt_id,
                    [str(a) for a in CANCELLING_ACTIONS],
                    vendor_order_id,
                    vendor_order_id,
                ),
            )
            return row_as_json(await cur.fetchone())


async def active_grant_for_connection(
    user_id: str, connection_id: str
) -> dict[str, str] | None:
    """An active egress grant on this connection, with the workspace it belongs to.

    Reconciliation borrows a grant rather than minting one: a grant is the
    user's consent pinned to the address they gave it for, and the sync that
    writes them replaces a whole workspace's set at once. Creating one here to
    read an order status would put a background job into that replacement race
    and could retire a grant a live turn is holding.

    None means the user has no reachable connection right now, which is a
    reason to skip the pass for them, never to guess.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT g.grant_id, g.workspace_id
                  FROM sandbox_egress_grants g
                 WHERE g.user_id = %s AND g.connection_id = %s::uuid
                   AND g.status = 'active'
                 ORDER BY g.updated_at DESC
                 LIMIT 1
                """,
                (user_id, connection_id),
            )
            row = await cur.fetchone()
    if row is None:
        return None
    return {
        "grant_id": str(row["grant_id"]),
        "workspace_id": str(row["workspace_id"]),
    }
