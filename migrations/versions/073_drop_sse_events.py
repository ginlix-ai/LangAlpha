"""Migration 073: drop ``conversation_responses.sse_events``.

The column held each turn's live stream, archived when the turn settled, and
replay once rebuilt a turn from it. Nothing writes it since turns settle from
their checkpoints alone, and nothing reads it since replay projects every turn
from its checkpoints and ``replay_facts``: an older row's stream was resolved
into ``replay_facts.legacy``, or kept whole in ``replay_facts.archive`` where
its checkpoints could not cover the turn (069). What the column still held
beyond that is output no checkpoint kept (a failed or stopped step, an attempt
a retry replaced), which replay does not show for any turn.

The drop is a catalog change and rewrites nothing, so the column's storage is
freed only when the table is next rewritten (``VACUUM FULL``).

Rollback: ``downgrade`` restores the column empty; the previous build neither
reads nor writes it. The streams themselves come back only from a backup taken
before the upgrade.
"""

from alembic import op

revision = "073"
down_revision = "072"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The ALTER waits for an ACCESS EXCLUSIVE lock on a table every turn's
    # persist writes; fail fast rather than queue every writer behind it.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE conversation_responses DROP COLUMN IF EXISTS sse_events")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_responses ADD COLUMN IF NOT EXISTS sse_events JSONB"
    )
