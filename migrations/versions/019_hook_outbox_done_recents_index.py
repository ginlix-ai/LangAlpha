"""Partial index for the DONE-recents ledger read.

get_recent_notification_run_ids derives wake-miss recovery from DONE
hook_outbox rows, which are never purged — without an index every status
slice / watch snapshot scans the full DONE history. The predicate mirrors
the query's filters exactly so the planner can use it.

Revision ID: 019
Revises: 018
"""

from alembic import op


revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_hook_outbox_done_recents
        ON hook_outbox (ordering_key, hook_type, completed_at DESC)
        WHERE status = 'done' AND (payload->>'dispatched_run_id') IS NOT NULL
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_hook_outbox_done_recents")
