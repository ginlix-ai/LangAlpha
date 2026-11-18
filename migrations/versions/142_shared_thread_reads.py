"""Migration 142: each member's read cursor and pin on a shared thread.

A personal thread keeps its read cursor (``last_seen_run_seq``) and pin
(``is_pinned``) on its own row: one person reads it. A shared workspace's
thread is read by several, so one member opening it must not mark it read for
the others, and a pin is one member's own ordering. ``shared_thread_reads``
holds both per member; a member with no row has read none of the thread's
turns and pinned nothing, as a personal thread starts.

Online-safe: one new empty table, no change to an existing one, no backfill.
Rollback: roll the code back first, then ``alembic downgrade 141``, which
drops the table and with it every member's cursor and pin (the threads stay).
"""

from alembic import op

revision = "142"
down_revision = "141"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS shared_thread_reads (
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            last_seen_run_seq BIGINT NOT NULL DEFAULT 0,
            is_pinned BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (conversation_thread_id, user_id)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_shared_thread_reads_user "
        "ON shared_thread_reads (user_id)"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS shared_thread_reads")
