"""What a wakeup's notice is in the outbox, and when it is still owed.

A wakeup is armed until it fires (``fired`` for a one-shot, a price or an
order; ``finished`` once a series' last slot is consumed), its window passes
(``expired``), or someone cancels it. Every one of those transitions is a
guarded UPDATE, so the sweep and the price monitor running in each worker can
race on one row and exactly one wins.

A fire's notice is a ``wakeup`` job in ``hook_outbox``, enqueued on the same
transaction as the UPDATE that makes it due and keyed by the slot it reports,
so a claimed fire can never lose its notice to a crash and two claims of one
slot can never send two. Whoever puts a notice in front of the model, the
running turn or a wake turn, stamps it delivered on the row, and a job whose
notice is delivered, or whose wakeup a forced cancel ended, is not owed
(``WAKEUP_OWED_SQL``).

Lock order, everywhere a statement takes more than one: wakeup rows in id
order, then their notice jobs, heads before riders and each in id order
(``ORDER BY (payload ? 'rides'), hook_outbox_id``). A statement that holds,
settles or frees a wake's riders takes its one head first and the riders
after it, and gathering skips a rider someone else holds rather than wait on
it, so a cancel racing a wake turn queues behind it instead of deadlocking.

The one module of this package ``runs/outbox.py`` imports, which is why it
opens no connection and reaches no service.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, get_args

from src.server.database.runs.hook_jobs import HookJob

HOOK_TYPE = "wakeup"

Kind = Literal["at", "cron", "price", "order"]
Notice = Literal["fire", "lapse"]
CancelledBy = Literal["agent", "user", "setting_off", "disabled", "archive", "workspace"]
Outcome = Literal["cancelled", "cancelled_in_flight", "too_late", "not_pending"]

CANCELLERS = frozenset(get_args(CancelledBy))
# The cancels that end a wakeup outright, a notice past recall included,
# since nothing should arrive after them: a thread archived or a workspace
# deleted gets no turn, and ``disabled`` is delivery's own check on the
# notice it is holding, finding the setting off.
FORCED_CANCELLERS: Tuple[CancelledBy, ...] = ("disabled", "archive", "workspace")

_COLUMN_NAMES = (
    "wakeup_id", "short_id", "conversation_thread_id", "user_id", "label", "kind",
    "timezone", "cron", "until_at", "schedule", "stop_reason", "next_fire_at",
    "fires_done", "last_fired_seq", "delivered_seq", "last_fired_at",
    "delivered_at", "delivered_run_id", "symbol", "market", "price_kind",
    "price_value", "pct_from", "armed_price", "fired_price", "warn_at",
    "warned_at", "lapse_delivered_at", "lapse_delivered_run_id", "expires_at",
    "attempt_id", "vendor_order_id", "baseline_filled_qty", "order_outcome",
    "status", "cancelled_by", "cancelled_at", "created_at", "updated_at",
)


def cols(alias: str = "") -> str:
    """The row's columns, qualified when the statement joins a table that
    shares some of their names (``order_attempts`` has ``status``)."""
    prefix = f"{alias}." if alias else ""
    return ", ".join(prefix + name for name in _COLUMN_NAMES)


WAKEUP_COLUMNS = cols()

_UUID_PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
_FORCED_SQL = ", ".join(f"'{by}'" for by in FORCED_CANCELLERS)


def _seq_sql(o: str) -> str:
    """The slot the job ``o`` reports, NULL when its payload is malformed."""
    return (
        f"CASE WHEN {o}.payload->>'seq' ~ '^[0-9]{{1,9}}$'"
        f" THEN ({o}.payload->>'seq')::int END"
    )


# Whether the notice of the job ``o`` is still owed: a fire until its slot is
# delivered, unless a forced cancel ended its wakeup; a lapse reminder while
# its price can still hit. Any other cancel closes every notice job it can
# still reach, so a fire job left behind one is past recall and arrives as the
# cancel promised. The casts are guarded so a malformed payload reads as not
# owed rather than failing the statement it sits in.
WAKEUP_OWED_SQL = f"""EXISTS (
    SELECT 1 FROM wakeups wk
    WHERE wk.wakeup_id = CASE
            WHEN o.payload->>'wakeup_id' ~* '{_UUID_PATTERN}'
            THEN (o.payload->>'wakeup_id')::uuid
        END
      AND CASE o.payload->>'notice'
            WHEN 'fire' THEN wk.delivered_seq < {_seq_sql("o")}
                AND (wk.cancelled_by IS NULL
                     OR wk.cancelled_by NOT IN ({_FORCED_SQL}))
            WHEN 'lapse' THEN wk.status = 'armed'
                AND wk.lapse_delivered_at IS NULL
                AND wk.expires_at > NOW()
            ELSE false
        END
)"""

# A notice job past recall: claimed by an executor, held by a running turn
# that is about to show it, riding a wake turn already dispatched, or carried
# by a wake POST that got no answer, which may yet be admitted with it. That
# mark (``unanswered``) stays on the head, and a rider a refusal held reads it
# through the head it rode.
TOO_LATE_SQL = (
    "(o.status = 'claimed' OR o.payload ? 'steered_run_id' OR o.payload ? 'rides'"
    " OR o.payload ? 'unanswered'"
    " OR EXISTS (SELECT 1 FROM hook_outbox h"
    " WHERE h.hook_outbox_id = (o.payload->>'rode')::uuid"
    " AND h.payload ? 'unanswered'))"
)


def open_sql(a: str) -> str:
    """Open, for the listing and a cancel of everything: still able to fire,
    or fired with a notice the agent has not read."""
    return (
        f"({a}.status = 'armed' OR ({a}.last_fired_seq > {a}.delivered_seq"
        f" AND {a}.status <> 'cancelled'))"
    )


def pending_sql(a: str = "") -> str:
    """Still able to fire, which is what the cap counts. A price or an order
    past its window reads as closed before the sweep closes it, as every
    reader shows it."""
    p = f"{a}." if a else ""
    return f"({p}status = 'armed' AND ({p}expires_at IS NULL OR {p}expires_at > NOW()))"


def jobs_of_sql(o: str, w: str) -> str:
    """The job ``o`` is one of the wakeup ``w``'s notices."""
    return (
        f"({o}.conversation_thread_id = {w}.conversation_thread_id"
        f" AND {o}.ordering_key = '{HOOK_TYPE}:' || {w}.conversation_thread_id::text"
        f" AND {o}.hook_type = '{HOOK_TYPE}'"
        f" AND {o}.payload->>'wakeup_id' = {w}.wakeup_id::text)"
    )


def unread_fire_sql(o: str, w: str) -> str:
    """The job ``o`` reports a fire of ``w`` the agent has not read."""
    return f"({o}.payload->>'notice' = 'fire' AND {_seq_sql(o)} > {w}.delivered_seq)"


def attempt_sql(w: str) -> str:
    """The order attempt the wakeup ``w`` watches, as JSON, aliased ``attempt``:
    every reader of an order wakeup describes the order it watches, so each
    read carries it rather than reading it after."""
    return (
        "(SELECT jsonb_build_object("
        "'attempt_id', a.attempt_id, 'vendor', a.vendor, 'mode', a.mode,"
        " 'server', a.server, 'tool', a.tool, 'order_json', a.order_json,"
        " 'status', a.status, 'vendor_order_id', a.vendor_order_id,"
        " 'filled_qty', a.filled_qty, 'avg_fill_price', a.avg_fill_price)"
        f" FROM order_attempts a WHERE a.attempt_id = {w}.attempt_id) AS attempt"
    )


def ordering_key(thread_id: str) -> str:
    """A thread's notices run in order, apart from its other jobs, so a parked
    notice never holds up a subagent's report-back."""
    return f"{HOOK_TYPE}:{thread_id}"


def fire_key(wakeup_id: str, seq: int) -> str:
    return f"{HOOK_TYPE}:{wakeup_id}:fire:{seq}"


def lapse_key(wakeup_id: str) -> str:
    return f"{HOOK_TYPE}:{wakeup_id}:lapse"


def notice_job(row: Dict[str, Any], notice: Notice, seq: int) -> HookJob:
    thread_id = str(row["conversation_thread_id"])
    wakeup_id = str(row["wakeup_id"])
    return HookJob(
        hook_type=HOOK_TYPE,
        idempotency_key=(
            fire_key(wakeup_id, seq) if notice == "fire" else lapse_key(wakeup_id)
        ),
        payload={
            "wakeup_id": wakeup_id,
            "notice": notice,
            "seq": seq,
            "thread_id": thread_id,
            "user_id": row["user_id"],
        },
        ordering_key=ordering_key(thread_id),
    )


def canonical_uuid(value: Any) -> Optional[str]:
    """``value`` as Postgres spells a uuid, or None. Python reads forms
    Postgres refuses (``urn:uuid:``, odd hyphen layouts), so an id is passed
    on and matched in this spelling only."""
    try:
        return str(uuid.UUID(str(value).strip()))
    except (TypeError, ValueError, AttributeError):
        return None


def is_uuid(value: Any) -> bool:
    return canonical_uuid(value) is not None


def uuids(values: Sequence[Any]) -> List[str]:
    return [u for u in map(canonical_uuid, values) if u is not None]


def normalize(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """String ids and float prices: rows cross into JSON payloads and tool
    text, where a UUID or a Decimal is a trap."""
    if row is None:
        return None
    out = dict(row)
    for key in ("wakeup_id", "conversation_thread_id", "delivered_run_id",
                "lapse_delivered_run_id", "attempt_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    for key in ("price_value", "armed_price", "fired_price", "baseline_filled_qty"):
        if out.get(key) is not None:
            out[key] = float(out[key])
    if out.get("schedule") is not None:
        out["schedule"] = list(out["schedule"])
    return out


# Readers for a value off a row or out of a JSON payload, where a time can
# arrive as ISO text and a number as a Decimal or a string.


def as_datetime(value: Any) -> Optional[datetime]:
    """Aware; a naive time is UTC."""
    if isinstance(value, str) and value:
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return None


def as_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
