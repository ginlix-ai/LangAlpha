"""Turn lifecycle v4 — expand step (additive only).

A run's conversation_responses row is now born 'in_progress' at START and is
the slot/run record itself: at most one per thread, enforced by a partial
unique index. Retries become attempt chains (attempt_no, retry_of_run_id) and
lost-HTTP resubmits dedup on a globally unique caller-supplied request_key.
Post-commit side effects go through hook_outbox. This step is deploy-safe for
pre-v4 code: the old (thread_id, turn_index) uniqueness stays and no trigger
is installed — 018 contracts the schema after cutover.

request_key lands as nullable-add + separate default + backfill + NOT NULL so
the volatile default can't force a full-table rewrite under the ADD COLUMN
lock. Index builds are non-CONCURRENT on purpose: at current table sizes the
lock window is negligible; revisit with CONCURRENTLY in autocommit blocks if a
deployment ever carries large history.

Revision ID: 017
Revises: 016
"""

from alembic import op


revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Heal any legacy in_progress response rows (pre-v4 code only wrote rows at
    # terminal, so this should match zero rows) so the partial unique slot
    # index below cannot fail to build.
    op.execute("""
        UPDATE conversation_responses
        SET status = 'error',
            errors = array_append(COALESCE(errors, '{}'), 'healed by migration 017: legacy in_progress row'),
            metadata = COALESCE(metadata, '{}'::jsonb)
                || '{"healed_by": "017_turn_lifecycle_expand"}'::jsonb
        WHERE status = 'in_progress'
    """)

    # attempt_no's constant default is metadata-only (no rewrite); request_key
    # must NOT carry its volatile default through ADD COLUMN.
    op.execute("""
        ALTER TABLE conversation_responses
            ADD COLUMN IF NOT EXISTS attempt_no INTEGER NOT NULL DEFAULT 1,
            ADD COLUMN IF NOT EXISTS retry_of_run_id UUID,
            ADD COLUMN IF NOT EXISTS request_key UUID,
            ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            ADD CONSTRAINT fk_responses_retry_of
            FOREIGN KEY (retry_of_run_id)
            REFERENCES conversation_responses(conversation_response_id)
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            ALTER COLUMN request_key SET DEFAULT gen_random_uuid()
    """)
    op.execute("""
        UPDATE conversation_responses
        SET request_key = gen_random_uuid()
        WHERE request_key IS NULL
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            ALTER COLUMN request_key SET NOT NULL
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            ADD CONSTRAINT chk_responses_attempt_chain
            CHECK (attempt_no >= 1 AND ((attempt_no = 1) = (retry_of_run_id IS NULL)))
            NOT VALID
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            VALIDATE CONSTRAINT chk_responses_attempt_chain
    """)

    # The slot: at most one live run per thread. Doubles as the recovery
    # scanner's discovery index.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_responses_in_progress_slot
        ON conversation_responses (conversation_thread_id)
        WHERE status = 'in_progress'
    """)
    # Attempt-chain identity; also serves latest-attempt-per-turn readers.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_responses_thread_turn_attempt
        ON conversation_responses (conversation_thread_id, turn_index, attempt_no)
    """)
    # Globally unique (not thread-scoped): an initial-message retransmit must
    # dedup before it can create a second server-generated thread.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_responses_request_key
        ON conversation_responses (request_key)
    """)
    # At most one direct successor per attempt — concurrent /retry loses here.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_responses_retry_of
        ON conversation_responses (retry_of_run_id)
        WHERE retry_of_run_id IS NOT NULL
    """)

    # run_id cascades with its response row: edit/regenerate truncation deletes
    # runs, and a pending hook for a truncated run must never fire.
    op.execute("""
        CREATE TABLE IF NOT EXISTS hook_outbox (
            hook_outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            run_id UUID NOT NULL
                REFERENCES conversation_responses(conversation_response_id)
                ON DELETE CASCADE,
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            hook_type VARCHAR(50) NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            ordering_key VARCHAR(255),
            idempotency_key VARCHAR(255) NOT NULL UNIQUE,
            status VARCHAR(20) NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'claimed', 'done', 'dead')),
            attempts INTEGER NOT NULL DEFAULT 0,
            lease_expires_at TIMESTAMPTZ,
            next_retry_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_hook_outbox_open
        ON hook_outbox (created_at)
        WHERE status IN ('pending', 'claimed')
    """)
    # Ordered hooks (e.g. report-back per flash thread) claim oldest-first
    # within their ordering key.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_hook_outbox_ordering
        ON hook_outbox (ordering_key, created_at)
        WHERE ordering_key IS NOT NULL AND status IN ('pending', 'claimed')
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS hook_outbox")
    op.execute("DROP INDEX IF EXISTS uq_responses_retry_of")
    op.execute("DROP INDEX IF EXISTS uq_responses_request_key")
    op.execute("DROP INDEX IF EXISTS uq_responses_thread_turn_attempt")
    op.execute("DROP INDEX IF EXISTS uq_responses_in_progress_slot")
    op.execute("""
        ALTER TABLE conversation_responses
            DROP CONSTRAINT IF EXISTS chk_responses_attempt_chain
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            DROP CONSTRAINT IF EXISTS fk_responses_retry_of
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            DROP COLUMN IF EXISTS cancel_requested_at,
            DROP COLUMN IF EXISTS request_key,
            DROP COLUMN IF EXISTS retry_of_run_id,
            DROP COLUMN IF EXISTS attempt_no
    """)
