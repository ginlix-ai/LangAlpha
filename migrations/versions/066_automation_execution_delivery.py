"""Migration 066: keep a firing's delivery on its execution row.

An automation run whose results the messaging service delivers is handed to
the service before its turn starts, and how that start left the delivery is
stamped on the turn's run row at START, so the settle on any worker ends the
run the same way. A process that dies between the start and START leaves no
run row, and the sweep that settles its firing would otherwise fall back to
the webhook and never end the run the service holds. ``delivery`` keeps the
same stamp, ``{"id", "targets"}``, from the moment the start answers; a start
made again under a new id after a wait replaces it. NULL means the firing's
start asked nothing (no delivery entries, or no messaging service), or the
firing ran before the column existed.

Rollback: ``downgrade`` drops the column; the previous build never reads it.
"""

from alembic import op

revision = "066"
down_revision = "065"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The ALTER waits for an ACCESS EXCLUSIVE lock on a table every firing
    # writes; fail fast rather than queue its writers behind it.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE automation_executions ADD COLUMN IF NOT EXISTS delivery JSONB"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE automation_executions DROP COLUMN IF EXISTS delivery")
