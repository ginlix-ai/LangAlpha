"""Migration 064: per-turn and per-subagent-run slices of checkpoint history.

A slice is stored once when a turn or run settles, so replay reads rows
instead of materializing checkpoint state. It is derived data: deleting rows
loses nothing, the next read rebuilds them. A slice is valid while
``slice_key`` names the code that cut it; ``lines`` is the projected replay
of the turn, wire-ready, valid while ``lines_key`` matches. ``run_claims``
pins each subagent launch without a ledger stamp to the run it started, which
only a pass over the whole thread can work out.

Rollback: ``downgrade`` drops both tables; the previous build never reads them.
"""

from alembic import op

revision = "064"
down_revision = "063"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Each foreign key takes a lock on the table it references, which live
    # turns write; fail fast rather than queue every writer behind it.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS turn_slices (
            conversation_response_id UUID PRIMARY KEY
                REFERENCES conversation_responses(conversation_response_id)
                ON DELETE CASCADE,
            conversation_thread_id UUID NOT NULL,
            input_checkpoint_id TEXT NOT NULL,
            tail_checkpoint_id TEXT NOT NULL,
            slice_key TEXT NOT NULL,
            slice_codec TEXT NOT NULL,
            slice BYTEA NOT NULL,
            lines_key TEXT,
            lines TEXT,
            run_claims JSONB,
            built_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    # Every read by thread filters on the code that cut the slice too.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_turn_slices_thread "
        "ON turn_slices (conversation_thread_id, slice_key)"
    )
    op.execute("""
        CREATE TABLE IF NOT EXISTS task_run_slices (
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            task_id TEXT NOT NULL,
            input_checkpoint_id TEXT NOT NULL,
            task_run_id UUID REFERENCES subagent_runs(task_run_id)
                ON DELETE CASCADE,
            tail_checkpoint_id TEXT NOT NULL,
            slice_key TEXT NOT NULL,
            slice_codec TEXT NOT NULL,
            slice BYTEA NOT NULL,
            built_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (conversation_thread_id, task_id, input_checkpoint_id)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_task_run_slices_run "
        "ON task_run_slices (task_run_id) WHERE task_run_id IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS task_run_slices")
    op.execute("DROP TABLE IF EXISTS turn_slices")
