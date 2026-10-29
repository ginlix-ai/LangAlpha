"""Migration 068: when each provenance record reached the client.

A record's ``source_timestamp`` is when its data was fetched. A source that
code fetched in the sandbox is reported only when the code returns, which can
be after a steer opened a new reply, and the reply showing it live is the one
open when it arrived. ``emitted_at`` keeps that arrival, which replay places a
subagent's record by. NULL means a row written before the column existed, and
replay places it by its fetch time as before.

Rollback: ``downgrade`` drops the column; the previous build never reads it.
"""

from alembic import op

revision = "068"
down_revision = "067"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The ALTER waits for an ACCESS EXCLUSIVE lock on a table every turn's
    # persist writes; fail fast rather than queue every writer behind it.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE provenance_records ADD COLUMN IF NOT EXISTS emitted_at TIMESTAMPTZ"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE provenance_records DROP COLUMN IF EXISTS emitted_at")
