"""Thread pin + archive: per-thread is_pinned flag and archived_at stamp.

``archived_at`` (timestamp, not boolean) both marks and dates the archive;
NULL = active. Default list queries exclude archived rows entirely — they
only surface when a client explicitly requests the archived view. The
partial index serves the hot workspace listing (pinned-first, recency
order); 001's only workspace_id index is the (workspace_id, thread_index)
uniqueness constraint, which no listing sort can use.

The table's generic updated_at trigger — which would have silently bumped
recency on the /seen cursor stamp and the pin/archive writes below — was
dropped in 022, ahead of the backfills it would have corrupted.

Revision ID: 024
Revises: 023
"""

from alembic import op


revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Covered by 022's session-level SET when the batch runs together; repeated
    # here so a standalone 023→024 upgrade from a stamped DB gets the same cap.
    op.execute("SET lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_threads "
        "ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT FALSE"
    )
    op.execute(
        "ALTER TABLE conversation_threads "
        "ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ"
    )
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_threads_ws_pin_updated
        ON conversation_threads (workspace_id, is_pinned DESC, updated_at DESC)
        WHERE archived_at IS NULL
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_threads_ws_pin_updated")
    op.execute("ALTER TABLE conversation_threads DROP COLUMN IF EXISTS archived_at")
    op.execute("ALTER TABLE conversation_threads DROP COLUMN IF EXISTS is_pinned")
