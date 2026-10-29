"""Migration 094: ``expired``, an order its vendor says lapsed on its own.

A vendor that names how an order ended can say it expired: a time in force ran
out, or a staged instruction was never confirmed in time. Until now the ledger
had no word for that and wrote ``cancelled``, which tells a person that they
or the agent cancelled something nobody did.

The status CHECK is replaced whole by one that also allows the new state, the
way 086 replaced it: NOT VALID under a short lock, then validated in an
autocommit block that blocks no reads or writes.

Rollback is a redeploy of the previous build with the schema left at 094: the
CHECK only widened, and that build never selects ``expired``. ``alembic
downgrade 093`` is needed only to remove the state itself, after stopping
this build, which writes what the restored CHECK refuses.

Revision ID: 094
Revises: 093
"""

from alembic import op

revision = "094"
down_revision = "093"
branch_labels = None
depends_on = None

_CHECK = "order_attempts_status_check"

# 086's list, written out for the same reason it was: readable in psql, and
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
    "handed_off",
)
_STATUSES = (*_BEFORE, "expired")


def _swap_check(statuses: tuple[str, ...]) -> str:
    values = ", ".join(f"'{status}'" for status in statuses)
    return f"""
        ALTER TABLE order_attempts
            DROP CONSTRAINT IF EXISTS {_CHECK},
            ADD CONSTRAINT {_CHECK} CHECK (status IN ({values})) NOT VALID
    """


def _validate_check() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"ALTER TABLE order_attempts VALIDATE CONSTRAINT {_CHECK}")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(_swap_check(_STATUSES))
    # Every existing row already satisfies the narrower CHECK it replaces.
    _validate_check()


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Back to ``submitted`` with no completion time, as 086's downgrade does for
    # its state: the previous build reads the vendor's list again and settles
    # the row in its own words, rather than keeping an end it cannot name.
    op.execute(
        """
        UPDATE order_attempts
           SET status = 'submitted', completed_at = NULL
         WHERE status = 'expired'
        """
    )
    op.execute(_swap_check(_BEFORE))
    _validate_check()
