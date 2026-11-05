"""Migration 108: idle compactions armed after a completed turn.

One new table, nothing existing touched.

``idle_compactions`` holds at most one firing per thread: the summary a
thread idle after a completed turn gets while its prompt cache is still warm.
A completed turn's outbox job writes or replaces the row, and every worker's
sweeper claims the rows that came due by deleting them, so a firing runs once
and is never retried. ``kind`` is ``I1`` (a rebuild that keeps no turns,
for a thread whose system prompt a deploy changed) or ``I2`` (a summary
keeping the tail, for a large thread). ``base_checkpoint_id`` is the commit
the turn left, which the firing reads and installs onto only while the thread
still points there. ``armed_run_seq`` is the arming run's ``run_seq``: a job that
runs late never replaces or drops a later run's row.

Rollback: ``alembic downgrade 107``; rows armed meanwhile are dropped with
the table, and those threads summarize at their next turn start as before.
The previous build never reads the table, but its outbox drainer has no
executor for the ``idle_compact`` jobs this build writes: beside it, those
jobs fail and retry until dead, and their turns are not armed.

Revision ID: 108
Revises: 107
"""

from alembic import op

revision = "108"
down_revision = "107"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS idle_compactions (
            conversation_thread_id UUID PRIMARY KEY
                REFERENCES conversation_threads(conversation_thread_id) ON DELETE CASCADE,
            kind VARCHAR(2) NOT NULL CHECK (kind IN ('I1', 'I2')),
            fire_at TIMESTAMPTZ NOT NULL,
            deadline TIMESTAMPTZ NOT NULL,
            base_checkpoint_id TEXT NOT NULL,
            armed_by_run_id UUID NOT NULL,
            armed_run_seq BIGINT NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            armed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_idle_compactions_fire_at
        ON idle_compactions (fire_at)
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS idle_compactions")
