"""Thread lifecycle: per-run monotonic sequence + durable seen cursor.

``run_seq`` is the one ordering authority for runs: ``turn_index`` is NOT
monotonic (same-turn retries reuse it, edit/regenerate deletes higher turns
and re-runs lower branches), so every seen comparison, event guard, and
latest-attempt read keys on this sequence instead. Assigned at INSERT,
never updated (unique index is the immutability convention).

The backfill is a 4-step dance because a bare ``DEFAULT nextval()`` add
would assign historical rows in heap order: nullable add → deterministic
per-thread-history-order backfill → setval/default/NOT NULL → indexes.

``conversation_threads.last_seen_run_seq`` is the durable read/seen cursor,
seeded to each thread's latest terminal run so history ships as *seen* —
without the seed, day one floods every historical finished thread with an
unseen dot and makes the feed snapshot's unseen set unbounded at birth.

Revision ID: 023
Revises: 022
"""

from alembic import op


revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Fail fast instead of queueing the whole cluster behind one long reader:
    # the ADD COLUMN takes ACCESS EXCLUSIVE for the migration transaction, and
    # with no lock_timeout a single in-flight SSE append would pile every
    # other query behind the waiting lock for the full backfill duration.
    op.execute("SET lock_timeout = '5s'")

    # Step 1: sequence + nullable column (no default yet).
    op.execute("CREATE SEQUENCE IF NOT EXISTS conversation_responses_run_seq_seq")
    op.execute(
        "ALTER TABLE conversation_responses ADD COLUMN IF NOT EXISTS run_seq BIGINT"
    )

    # Step 2: deterministic backfill preserving per-thread history order,
    # with a stable tie-breaker so re-runs are reproducible.
    op.execute("""
        WITH ordered AS (
            SELECT conversation_response_id,
                   ROW_NUMBER() OVER (
                       ORDER BY conversation_thread_id, turn_index, attempt_no,
                                created_at, conversation_response_id
                   ) AS rn
            FROM conversation_responses
        )
        UPDATE conversation_responses cr
        SET run_seq = o.rn
        FROM ordered o
        WHERE cr.conversation_response_id = o.conversation_response_id
    """)

    # Step 3: advance the sequence past the backfilled max, then arm the
    # default and lock the column down.
    op.execute("""
        SELECT setval(
            'conversation_responses_run_seq_seq',
            COALESCE((SELECT MAX(run_seq) FROM conversation_responses), 0) + 1,
            false
        )
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            ALTER COLUMN run_seq
            SET DEFAULT nextval('conversation_responses_run_seq_seq')
    """)
    op.execute(
        "ALTER TABLE conversation_responses ALTER COLUMN run_seq SET NOT NULL"
    )
    op.execute("""
        ALTER SEQUENCE conversation_responses_run_seq_seq
            OWNED BY conversation_responses.run_seq
    """)

    # Step 4: uniqueness (immutability convention) + the latest-attempt path.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_responses_run_seq
        ON conversation_responses (run_seq)
    """)
    # Covering index: the latest-attempt LATERAL (list rows + the feed
    # snapshot's owned CTE) reads exactly these five payload columns for the
    # top-run_seq row per thread; INCLUDE lets that read stay index-only
    # instead of one heap fetch per owned thread per snapshot.
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_responses_thread_run_seq
        ON conversation_responses (conversation_thread_id, run_seq DESC)
        INCLUDE (conversation_response_id, status, cancel_requested_at,
                 interrupt_reason, created_at)
    """)
    # (thread_id, run_seq DESC) serves every predicate the 001 single-column
    # index served — keeping both just taxes the hottest write path.
    op.execute("DROP INDEX IF EXISTS idx_responses_thread_id")

    # Seen cursor + rollout seeding (history ships seen).
    op.execute("""
        ALTER TABLE conversation_threads
            ADD COLUMN IF NOT EXISTS last_seen_run_seq BIGINT NOT NULL DEFAULT 0
    """)
    op.execute("""
        UPDATE conversation_threads ct
        SET last_seen_run_seq = latest.run_seq
        FROM (
            SELECT DISTINCT ON (conversation_thread_id)
                   conversation_thread_id, run_seq
            FROM conversation_responses
            WHERE status IN ('completed', 'interrupted', 'error', 'cancelled')
            ORDER BY conversation_thread_id, run_seq DESC
        ) latest
        WHERE ct.conversation_thread_id = latest.conversation_thread_id
    """)


def downgrade() -> None:
    op.execute(
        "ALTER TABLE conversation_threads DROP COLUMN IF EXISTS last_seen_run_seq"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_responses_thread_id "
        "ON conversation_responses(conversation_thread_id)"
    )
    op.execute("DROP INDEX IF EXISTS ix_responses_thread_run_seq")
    op.execute("DROP INDEX IF EXISTS uq_responses_run_seq")
    # The sequence is OWNED BY the column, so it drops with it.
    op.execute("ALTER TABLE conversation_responses DROP COLUMN IF EXISTS run_seq")
