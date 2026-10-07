"""Migration 062: when a computer's sandbox host started refusing to start it.

``computers.host_unavailable_since`` is stamped by the first start the
provider refuses because the sandbox's host is recovering, and kept by every
refusal after it, so it measures the whole outage whichever worker tried and
however often the server restarted. A column rather than a key in ``config``
because several workers read and stamp it on the start path, and only for the
sandbox the row still names. Once it is older than
``daytona.host_unavailable_rebuild_after_minutes`` the next start rebuilds the
computer from its backed-up files and leaves the old sandbox in place. A start
that succeeds clears it, and so does binding another sandbox.

Existing rows start NULL, a computer with no outage, so they need no backfill.
A nullable column with no default rewrites no rows, so the statement holds its
lock only for the catalog change.

Rollback: ``alembic stamp 061`` first, because the previous build's
``upgrade head`` cannot start from a revision it does not ship, then redeploy
it; it never reads the column. ``downgrade`` drops the column, and with it the
start of any outage in progress, which the next refused start stamps afresh.
Upgrading again clears every stamp the column kept: the previous build starts
sandboxes without clearing them, so a stamp it outlived would cut the next
outage short.
"""

from alembic import op

revision = "062"
down_revision = "061"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Own transaction, so the ACCESS EXCLUSIVE lock is released at once.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            "ALTER TABLE computers "
            "ADD COLUMN IF NOT EXISTS host_unavailable_since TIMESTAMPTZ"
        )
        op.execute("RESET lock_timeout")
        # Matches nothing unless this is a re-upgrade after a stamp-only rollback.
        op.execute(
            "UPDATE computers SET host_unavailable_since = NULL "
            "WHERE host_unavailable_since IS NOT NULL"
        )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE computers DROP COLUMN IF EXISTS host_unavailable_since"
    )
