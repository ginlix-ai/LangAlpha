"""The order attempt ledger: one durable row per order a model tries to place.

An approval used to authorize a graph transition. This table makes it
authorize one execution of one call with one set of arguments: the row is
written before the user is asked, decided against by id, consumed exactly once
at execution time, and completed with the vendor's answer. The relay reads it
before it forwards an order frame, so a replayed frame or a retry after a
worker loss finds the row already consumed and is refused.

No foreign keys, deliberately. The row is written mid-turn, before the
response row it names exists, and it is the record that an order was placed
with someone's money: deleting a thread must not delete the evidence.

``(user_id, message_id, tool_call_id)`` is unique because together they name one
call, and the consume-once guard is meaningless if one call can own two rows.
The call id alone does not: it is the model provider's, and some providers
number it by its position in the conversation, so a new thread or a regenerated
answer repeats it. The id of the message that made the call is new on every
model response and reads back unchanged when a run resumes after an approval,
the one repeat this key exists to recognize. The user is part of the key so a
collision across two users never hands one of them the other's row.

``filled_qty`` and ``avg_fill_price`` are NUMERIC, not float: a quantity and an
average price come off a wire as exact decimal text, and binary rounding here
would report a different fill than the brokerage did. ``fees`` is JSONB because
it is an amount and a currency, and a bare number would silently mix them.
``action_url`` keeps the vendor's own confirmation page for a staged
instruction, which arrives once, in the answer to the call that staged it.

``executed_at`` and ``dispatched_at`` are two marks, not one: the first is the
worker taking the single execution an approval buys, the second is the relay
letting the frame leave the host. The relay sets it once, so a second copy of
the same signed frame is refused, and the ledger reads it when the answer
never comes back: an attempt with the mark may have reached the vendor and is
``unknown`` until the vendor's list says otherwise, one without it never left.

``swept_at`` is the reconciliation sweep's clock, apart from ``updated_at``: a
pass stamps every row it takes whether or not the read moved it, so a row no
read can settle goes to the back of the queue rather than holding its front.

Revision ID: 043
Revises: 042
"""

from alembic import op


revision = "043"
down_revision = "042"
branch_labels = None
depends_on = None

# ``AttemptStatus`` in src/server/services/brokerage_orders/models.py. Written
# out rather than derived so the constraint is readable in psql and does not
# move when the enum is imported from somewhere else.
_STATUSES = (
    "proposed",
    "approved",
    "rejected_by_user",
    "refused",
    "submitting",
    "submitted",
    "pending_confirm",
    "working",
    "partially_filled",
    "filled",
    "cancelled",
    "rejected_by_vendor",
    "failed",
    "unknown",
)


def upgrade() -> None:
    statuses = ", ".join(f"'{status}'" for status in _STATUSES)
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS order_attempts (
            attempt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            workspace_id UUID,
            thread_id UUID,
            conversation_response_id UUID,
            turn_index INTEGER,
            message_id TEXT NOT NULL,
            tool_call_id TEXT NOT NULL,
            server TEXT,
            vendor TEXT NOT NULL,
            tool TEXT,
            action TEXT,
            mode TEXT,
            account_ref TEXT,
            args JSONB,
            args_sha256 TEXT,
            order_json JSONB,
            approval_required BOOLEAN NOT NULL DEFAULT FALSE,
            status TEXT NOT NULL CHECK (status IN ({statuses})),
            decided_at TIMESTAMPTZ,
            decision_message TEXT,
            executed_at TIMESTAMPTZ,
            dispatched_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            vendor_order_id TEXT,
            route JSONB,
            parent_attempt_id UUID,
            action_url TEXT,
            filled_qty NUMERIC,
            avg_fill_price NUMERIC,
            fees JSONB,
            result_sha256 TEXT,
            failure JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            swept_at TIMESTAMPTZ,
            UNIQUE (user_id, message_id, tool_call_id)
        )
    """)

    # The orders list: one user's attempts, newest first, across every thread.
    # ``attempt_id`` is in the key so the keyset page predicate
    # ``(created_at, attempt_id) < (%s, %s)`` is an index condition, not a
    # filter over everything ahead of the page.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_order_attempts_user
        ON order_attempts (user_id, created_at DESC, attempt_id DESC)
    """)
    # The thread's own orders, for the receipt and the turn view.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_order_attempts_thread
        ON order_attempts (thread_id, created_at DESC, attempt_id DESC)
    """)
    # Reconciliation's join: a vendor order list answers with ids, and this is
    # what turns one of those back into one of this user's attempts.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_order_attempts_vendor_order
        ON order_attempts (user_id, vendor, vendor_order_id)
    """)
    # The reconciliation sweep runs every minute over the whole ledger and
    # wants only the rows still moving, least recently swept first; the partial
    # predicate keeps it that size however many settled rows accumulate
    # underneath.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_order_attempts_stale
        ON order_attempts (swept_at ASC NULLS FIRST, updated_at)
        WHERE status IN (
            'submitting', 'submitted', 'pending_confirm', 'working',
            'partially_filled', 'unknown'
        )
    """)
    # The sweep's lapse check: approvals nothing spent, and whether their run
    # still has a card waiting. A row leaves both states once it is answered
    # and spent, so the index holds only what is still in flight.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_order_attempts_undecided
        ON order_attempts (conversation_response_id)
        WHERE status IN ('proposed', 'approved')
    """)


def downgrade() -> None:
    """Drop the ledger.

    DESTRUCTIVE: these rows are the only record of which orders were approved
    and by whom. Nothing reconstructs them, since the tool call they describe
    survives only as a checkpoint message.
    """
    op.execute("DROP TABLE IF EXISTS order_attempts CASCADE")
