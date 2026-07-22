"""Subagent run ledger — tasks + per-execution runs.

Background subagent executions get the same ledger discipline as root turns:
a subagent_runs row is born 'in_progress' under the task namespace guard
BEFORE the writer spawns, is the admission slot (partial unique per task),
and reaches exactly one immutable terminal via a guarded CAS. subagent_tasks
is the durable home for the logical task's identity (replacing Redis meta as
lifecycle truth at cutover). Uniqueness on (parent_run_id,
launch_tool_call_id) makes checkpoint re-execution of the same Task call a
no-op instead of a double spawn; UNIQUE(predecessor_run_id) keeps the resume
chain linear.

ON DELETE policy: a deleted branch takes its descendant task rows with it
(parent_run_id CASCADE) — fork/edit truncation of conversation_responses is
the only path that deletes parent runs, and it must not strand ledger rows
for a branch that no longer exists. Deleting a branch while a descendant run
is in_progress is rejected by the ThreadMutation guard (code-level, M4), not
by this schema. Thread FK cascades likewise.

Revision ID: 020
Revises: 019
"""

from alembic import op


revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS subagent_tasks (
            thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            task_id TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            subagent_type TEXT NOT NULL DEFAULT 'general-purpose',
            latest_run_id UUID,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (thread_id, task_id)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS subagent_runs (
            task_run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            thread_id UUID NOT NULL,
            task_id TEXT NOT NULL,
            parent_run_id UUID
                REFERENCES conversation_responses(conversation_response_id)
                ON DELETE CASCADE,
            launch_tool_call_id TEXT,
            predecessor_run_id UUID
                REFERENCES subagent_runs(task_run_id)
                ON DELETE SET NULL,
            cause TEXT NOT NULL DEFAULT 'init'
                CHECK (cause IN ('init', 'resume', 'hitl')),
            status TEXT NOT NULL DEFAULT 'in_progress'
                CHECK (status IN
                    ('in_progress', 'completed', 'interrupted',
                     'error', 'cancelled')),
            cancel_requested_at TIMESTAMPTZ,
            start_checkpoint_id TEXT,
            final_checkpoint_id TEXT,
            failure JSONB,
            result_delivered_at TIMESTAMPTZ,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finalized_at TIMESTAMPTZ,
            FOREIGN KEY (thread_id, task_id)
                REFERENCES subagent_tasks(thread_id, task_id)
                ON DELETE CASCADE
        )
    """)

    # The slot: at most one live execution per logical task.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_subagent_runs_active_slot
        ON subagent_runs (thread_id, task_id)
        WHERE status = 'in_progress'
    """)
    # Checkpoint re-execution of the same Task tool call must not double-spawn.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_subagent_runs_launch_call
        ON subagent_runs (parent_run_id, launch_tool_call_id)
        WHERE parent_run_id IS NOT NULL AND launch_tool_call_id IS NOT NULL
    """)
    # Linear resume chain: a run has at most one successor.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_subagent_runs_predecessor
        ON subagent_runs (predecessor_run_id)
        WHERE predecessor_run_id IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_subagent_runs_thread_task
        ON subagent_runs (thread_id, task_id, started_at)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_subagent_runs_open
        ON subagent_runs (started_at)
        WHERE status = 'in_progress'
    """)

    op.execute("""
        ALTER TABLE subagent_tasks
            ADD CONSTRAINT fk_subagent_tasks_latest_run
            FOREIGN KEY (latest_run_id)
            REFERENCES subagent_runs(task_run_id)
            ON DELETE SET NULL
            DEFERRABLE INITIALLY DEFERRED
    """)

    # Same lifecycle guard shape as conversation_responses (018): terminal
    # status and cancel intent are immutable; result_delivered_at may be
    # stamped once post-terminal (delivery legitimately trails the CAS).
    op.execute("""
        CREATE OR REPLACE FUNCTION subagent_run_lifecycle_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.status <> 'in_progress' THEN
                    RAISE EXCEPTION
                        'task run %: rows are born in_progress, not %',
                        NEW.task_run_id, NEW.status;
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.status <> 'in_progress' THEN
                IF NEW.status IS DISTINCT FROM OLD.status THEN
                    RAISE EXCEPTION
                        'task run %: terminal status % is immutable',
                        OLD.task_run_id, OLD.status;
                END IF;
                IF NEW.cancel_requested_at IS DISTINCT FROM
                        OLD.cancel_requested_at THEN
                    RAISE EXCEPTION
                        'task run %: cancel_requested_at is frozen once terminal',
                        OLD.task_run_id;
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER trg_subagent_run_lifecycle_guard
        BEFORE INSERT OR UPDATE ON subagent_runs
        FOR EACH ROW
        EXECUTE FUNCTION subagent_run_lifecycle_guard()
    """)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_subagent_run_lifecycle_guard ON subagent_runs"
    )
    op.execute("DROP FUNCTION IF EXISTS subagent_run_lifecycle_guard()")
    op.execute(
        "ALTER TABLE subagent_tasks DROP CONSTRAINT IF EXISTS fk_subagent_tasks_latest_run"
    )
    op.execute("DROP TABLE IF EXISTS subagent_runs")
    op.execute("DROP TABLE IF EXISTS subagent_tasks")
