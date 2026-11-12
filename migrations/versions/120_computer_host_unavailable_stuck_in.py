"""Migration 120: the transition a computer's outage clock caught its sandbox in.

``computers.host_unavailable_since`` measures how long the host has failed to
bring the sandbox up. Refused starts fed it until now; a sandbox stuck
mid-start, mid-stop or mid-archive feeds it too, and this column names that
transition. A slow transition also gets sighted, so such a clock runs only
while every sighting finds the sandbox in the same transition, and a read that
finds it anywhere else ends it: two slow moments hours apart are not an outage.
NULL is a clock refused starts run, which reads cannot end, since a refused
sandbox reads as stopped between tries.

A nullable column with no default rewrites no rows, and every clock already
running was started by refused starts, which NULL already says. The previous
build never reads the column and runs against it untouched.

Rollback: ``alembic downgrade 119`` drops the column.
"""

from alembic import op

revision = "120"
down_revision = "119"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE computers ADD COLUMN IF NOT EXISTS host_unavailable_stuck_in TEXT"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE computers DROP COLUMN IF EXISTS host_unavailable_stuck_in")
