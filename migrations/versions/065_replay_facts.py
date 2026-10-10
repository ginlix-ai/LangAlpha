"""Migration 065: replay facts on settled runs.

Some of what a turn's replay shows was only ever in the stream it emitted:
how long each reasoning block took, which sandbox images were captured to
which URLs, and the steering a subagent run handed back unread. ``replay_facts``
keeps those few values on the run row itself, so replay reads them next to the
checkpoint instead of from the stored event log. NULL means a run settled
before the column existed, and replay keeps reading the stored events for it.

Rollback: ``downgrade`` drops both columns; the previous build never reads them.
"""

from alembic import op

revision = "065"
down_revision = "064"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Each ALTER waits for an ACCESS EXCLUSIVE lock on a table live turns
    # write; fail fast rather than queue every writer behind it.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_responses "
        "ADD COLUMN IF NOT EXISTS replay_facts JSONB"
    )
    op.execute(
        "ALTER TABLE subagent_runs ADD COLUMN IF NOT EXISTS replay_facts JSONB"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE subagent_runs DROP COLUMN IF EXISTS replay_facts")
    op.execute(
        "ALTER TABLE conversation_responses DROP COLUMN IF EXISTS replay_facts"
    )
