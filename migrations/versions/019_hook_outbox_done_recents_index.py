"""hook_outbox maintenance indexes.

Partial index for the DONE-recents ledger read: the wake-miss recovery
query slices DONE rows by (ordering_key, hook_type, completed_at) — the
predicate mirrors its filters exactly so the planner can use it.

Terminal-age index: the per-worker sweeps (dead-revive, retention purge)
range on COALESCE(completed_at, created_at) over done/dead rows every few
seconds — without it each sweep seq-scans the whole terminal history.
FK indexes: run/thread cascade deletes (edit/regenerate truncation)
otherwise seq-scan the table per deleted parent row.

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
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_hook_outbox_terminal_age
        ON hook_outbox (COALESCE(completed_at, created_at))
        WHERE status IN ('done', 'dead')
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_hook_outbox_run_id
        ON hook_outbox (run_id)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_hook_outbox_thread_id
        ON hook_outbox (conversation_thread_id)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_hook_outbox_thread_id")
    op.execute("DROP INDEX IF EXISTS idx_hook_outbox_run_id")
    op.execute("DROP INDEX IF EXISTS idx_hook_outbox_terminal_age")
    op.execute("DROP INDEX IF EXISTS idx_hook_outbox_done_recents")
