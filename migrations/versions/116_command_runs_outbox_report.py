"""Migration 116: what a handed-off command's outbox came to, for its notice.

A Bash or ExecuteCode job publishes chart layers through its outbox. One that
ends inside its call has them published there and read back in the call's
result; one handed off has them published when the watcher settles it, and
``outbox_report`` keeps the read-back, and ``outbox_layers`` the layers it
stored (each one's address, title and summary), both written by the same
guarded move that ends the row: the read-back for the notice the agent reads,
the layers for the chart-layer card the notice's caption shows the user,
whichever worker renders it. Nullable columns, nothing existing touched.

Rollback: ``alembic downgrade 115``. The previous build never reads the
columns, so it can also run beside them.

Revision ID: 116
Revises: 115
"""

from alembic import op

revision = "116"
down_revision = "115"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE command_runs ADD COLUMN IF NOT EXISTS outbox_report TEXT, "
        "ADD COLUMN IF NOT EXISTS outbox_layers JSONB"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE command_runs DROP COLUMN IF EXISTS outbox_layers, "
        "DROP COLUMN IF EXISTS outbox_report"
    )
