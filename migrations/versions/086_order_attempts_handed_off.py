"""Migration 086: ``handed_off``, the end of a staged instruction its vendor stopped listing.

IBKR lists a staged instruction until a person sends or deletes it in IBKR's
own client, or it expires, and then drops it without saying which; a sent one
becomes a live order under a new id with no link back. Only a person there can
trade one, so reconciliation settles the attempt ``handed_off`` once a
complete list stops carrying it, rather than leaving it ``submitted`` and
re-read on every pass.

``unlisted_at`` is when a complete vendor listing first lacked an open order
that its adapter ends on absence, such as an IBKR staged instruction. The
reconciler settles the order ``handed_off`` only when the next read, a poll
interval later, agrees, and any read that lists it again clears the column, so
one transient empty or short list cannot end every staged instruction at once.

The status CHECK, declared inline by 043 and so named
``order_attempts_status_check``, is replaced whole by one that also allows the
new state. A plain ADD CONSTRAINT would scan every row under ACCESS EXCLUSIVE,
so it goes in NOT VALID, a catalog change under that short lock, and is
validated in an autocommit block under SHARE UPDATE EXCLUSIVE, which blocks no
reads or writes. The 5s lock timeout bounds only the wait for the ACCESS
EXCLUSIVE lock, failing the upgrade rather than queueing every order write
behind a long transaction on the ledger. Neither partial index changes: both
list states a row is still moving through, and this one ends it.

Rollback is a redeploy of the previous build with the schema left at 086: the
CHECK only widened, and that build never selects ``handed_off`` and ignores
``unlisted_at``. ``alembic downgrade 085`` is needed only to remove the state
itself, after stopping this build, which writes what the restored CHECK refuses.
"""

from alembic import op

revision = "086"
down_revision = "085"
branch_labels = None
depends_on = None

_CHECK = "order_attempts_status_check"

# 043's list, written out for the same reason it was: readable in psql, and
# fixed however ``AttemptStatus`` moves.
_BEFORE = (
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
_STATUSES = (*_BEFORE, "handed_off")


def _swap_check(statuses: tuple[str, ...]) -> str:
    # ADD CONSTRAINT has no IF NOT EXISTS; the DROP makes the ALTER re-runnable
    # after an abort in the autocommit block that validates it.
    values = ", ".join(f"'{status}'" for status in statuses)
    return f"""
        DROP CONSTRAINT IF EXISTS {_CHECK},
        ADD CONSTRAINT {_CHECK} CHECK (status IN ({values})) NOT VALID
    """


def _validate_check() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"ALTER TABLE order_attempts VALIDATE CONSTRAINT {_CHECK}")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"""
        ALTER TABLE order_attempts
            ADD COLUMN IF NOT EXISTS unlisted_at TIMESTAMPTZ,
            {_swap_check(_STATUSES)}
    """)
    # Every existing row already satisfies the narrower CHECK it replaces.
    _validate_check()


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Back to ``submitted``, which each of these rows was before it settled and
    # which the previous build says of an instruction it can no longer find:
    # any end state would claim an outcome nobody reported. An open row carries
    # no completion time, so that goes too. Upgraded again, this build settles
    # them anew from the vendor's list.
    op.execute(
        """
        UPDATE order_attempts
           SET status = 'submitted', completed_at = NULL
         WHERE status = 'handed_off'
        """
    )
    op.execute(f"""
        ALTER TABLE order_attempts
            DROP COLUMN IF EXISTS unlisted_at,
            {_swap_check(_BEFORE)}
    """)
    # The UPDATE above left no row outside the old vocabulary.
    _validate_check()
