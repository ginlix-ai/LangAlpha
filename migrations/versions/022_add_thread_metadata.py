"""Add conversation_threads.metadata with origin provenance.

General-purpose thread annotation column (mirrors conversation_queries.metadata).
First documented key is ``origin`` — who initiated the thread:
``{"origin": {"type": "agent"|"automation"|"system", "id": "<initiator id>"}}``;
an absent origin key means user-initiated, so the common case is never written.
``platform`` stays a scalar (surface identity — it backs the channel dedup
unique index and the market_view prefix filter); origin is orthogonal to it.

Backfills automation-created threads from execution/pin linkage and folds the
short-lived ``platform='automation'`` convention (never released) into origin.
The platform-null sweep is one-way: the downgrade drops the metadata column
that holds the derived origin, so a down/up cycle cannot restore it.

Drops the table's generic updated_at trigger FIRST: it unconditionally bumped
``updated_at`` on every row update, so the backfills here (and 023's seen-cursor
seed) would otherwise collapse every touched thread's recency to one migration
timestamp — destroying the sidebar's recency order irreversibly. Every write
path already sets ``updated_at = NOW()`` explicitly where recency should move.

Revision ID: 022
Revises: 021
"""

from alembic import op


revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Fail fast instead of queueing the cluster: the DROP TRIGGER below takes
    # the FIRST ACCESS EXCLUSIVE lock of the 022→024 batch (alembic runs them
    # in one transaction, so this session-level SET covers all three; 023
    # repeats it for standalone re-runs from a stamped DB). Without it, one
    # long reader parks this lock request and every later query queues behind
    # it for the full batch duration.
    op.execute("SET lock_timeout = '5s'")

    # Must precede every backfill UPDATE in 022/023: the BEFORE UPDATE trigger
    # would overwrite NEW.updated_at after any SET clause, so preserving the
    # column in the UPDATE itself is not possible.
    op.execute(
        "DROP TRIGGER IF EXISTS trg_conversation_threads_updated_at "
        "ON conversation_threads"
    )

    op.execute("""
        ALTER TABLE conversation_threads
            ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb
    """)

    # Automation-created threads: stamp origin from the earliest execution row.
    op.execute("""
        UPDATE conversation_threads ct
        SET metadata = ct.metadata || jsonb_build_object(
            'origin', jsonb_build_object('type', 'automation', 'id', a.automation_id::text)
        )
        FROM (
            SELECT DISTINCT ON (conversation_thread_id) conversation_thread_id, automation_id
            FROM automation_executions
            WHERE conversation_thread_id IS NOT NULL
            ORDER BY conversation_thread_id, created_at ASC
        ) a
        WHERE ct.conversation_thread_id = a.conversation_thread_id
          AND NOT ct.metadata ? 'origin'
    """)

    # Pinned continue-strategy threads without execution linkage.
    op.execute("""
        UPDATE conversation_threads ct
        SET metadata = ct.metadata || jsonb_build_object(
            'origin', jsonb_build_object('type', 'automation', 'id', au.automation_id::text)
        )
        FROM automations au
        WHERE au.conversation_thread_id = ct.conversation_thread_id
          AND NOT ct.metadata ? 'origin'
    """)

    # Fold the transitional platform value (dev-only, never released) into origin.
    op.execute("""
        UPDATE conversation_threads
        SET metadata = metadata || '{"origin": {"type": "automation"}}'::jsonb
        WHERE platform = 'automation' AND NOT metadata ? 'origin'
    """)
    op.execute("""
        UPDATE conversation_threads SET platform = NULL WHERE platform = 'automation'
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE conversation_threads DROP COLUMN IF EXISTS metadata
    """)
    op.execute("""
        CREATE OR REPLACE TRIGGER trg_conversation_threads_updated_at
            BEFORE UPDATE ON conversation_threads
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column()
    """)
