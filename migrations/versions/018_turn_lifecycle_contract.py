"""Turn lifecycle v4 — contract step (runs after code cutover, runs drained).

Drops the pre-v4 one-row-per-turn uniqueness so attempt chains (retry without
truncation) become legal, and installs the lifecycle guard trigger: rows are
born 'in_progress' (INSERT), status transitions exactly once in_progress ->
terminal (UPDATE), and cancel intent cannot be recorded on a finished run.
Run-keyed sse_events/metadata patches on terminal rows stay allowed (late
subagent archives). Pre-v4 code cannot run against this schema (its ON
CONFLICT upsert needs the dropped constraint) — downgrade this revision
before rolling the binary back.

Both directions take ACCESS EXCLUSIVE on conversation_responses up front so
the drain preflight and the destructive downgrade collapse cannot race a
concurrent writer (check-then-mutate would otherwise admit an open run
between the check and the DDL).

Revision ID: 018
Revises: 017
"""

from alembic import op


revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Serialize against writers first; the preflight below is only sound while
    # this lock is held (held to end of the migration transaction).
    op.execute("LOCK TABLE conversation_responses IN ACCESS EXCLUSIVE MODE")

    # Contract requires drained runs: an open run's owner still expects the
    # pre-contract write behavior it started under.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM conversation_responses WHERE status = 'in_progress') THEN
                RAISE EXCEPTION 'turn-lifecycle contract: open runs exist — drain in_progress runs first';
            END IF;
        END $$;
    """)

    op.execute("""
        ALTER TABLE conversation_responses
            DROP CONSTRAINT IF EXISTS unique_turn_index_per_thread_response
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION conversation_responses_lifecycle_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.status <> 'in_progress' THEN
                    RAISE EXCEPTION 'run %: rows are born in_progress, not %',
                        NEW.conversation_response_id, NEW.status;
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.status <> 'in_progress' THEN
                IF NEW.status IS DISTINCT FROM OLD.status THEN
                    RAISE EXCEPTION 'run %: terminal status % is immutable',
                        OLD.conversation_response_id, OLD.status;
                END IF;
                IF NEW.cancel_requested_at IS DISTINCT FROM OLD.cancel_requested_at THEN
                    RAISE EXCEPTION 'run %: cancel_requested_at is frozen once terminal',
                        OLD.conversation_response_id;
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$;
    """)
    op.execute("""
        CREATE TRIGGER trg_responses_lifecycle_guard
        BEFORE INSERT OR UPDATE ON conversation_responses
        FOR EACH ROW
        EXECUTE FUNCTION conversation_responses_lifecycle_guard()
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE conversation_responses IN ACCESS EXCLUSIVE MODE")
    op.execute("DROP TRIGGER IF EXISTS trg_responses_lifecycle_guard ON conversation_responses")
    op.execute("DROP FUNCTION IF EXISTS conversation_responses_lifecycle_guard()")

    # Pre-v4 uniqueness cannot hold with attempt chains present: keep only the
    # latest attempt per turn and flatten it back to a chainless row. The
    # attempt-chain FK/check must come off first — the survivor references the
    # predecessor being deleted. Superseded attempts are invisible to pre-v4
    # readers and would break its one-row-per-turn writes, so their deletion —
    # including feedback/provenance rows AND done/dead hook_outbox history that
    # CASCADE with them — is the deliberate data cost of rolling back past the
    # contract. An UNDRAINED hook (pending/claimed) is a promised external
    # effect, not history: refuse to destroy it — drain the outbox first.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM hook_outbox h
                JOIN conversation_responses r ON r.conversation_response_id = h.run_id
                JOIN conversation_responses newer
                  ON newer.conversation_thread_id = r.conversation_thread_id
                 AND newer.turn_index = r.turn_index
                 AND newer.attempt_no > r.attempt_no
                WHERE h.status IN ('pending', 'claimed')
            ) THEN
                RAISE EXCEPTION 'turn-lifecycle downgrade: superseded attempts have undrained outbox jobs — drain hook_outbox first';
            END IF;
        END $$;
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            DROP CONSTRAINT IF EXISTS chk_responses_attempt_chain
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            DROP CONSTRAINT IF EXISTS fk_responses_retry_of
    """)
    op.execute("""
        DELETE FROM conversation_responses r
        USING conversation_responses newer
        WHERE newer.conversation_thread_id = r.conversation_thread_id
          AND newer.turn_index = r.turn_index
          AND newer.attempt_no > r.attempt_no
    """)
    op.execute("""
        UPDATE conversation_responses
        SET attempt_no = 1, retry_of_run_id = NULL
        WHERE attempt_no <> 1 OR retry_of_run_id IS NOT NULL
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            ADD CONSTRAINT fk_responses_retry_of
            FOREIGN KEY (retry_of_run_id)
            REFERENCES conversation_responses(conversation_response_id)
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            ADD CONSTRAINT chk_responses_attempt_chain
            CHECK (attempt_no >= 1 AND ((attempt_no = 1) = (retry_of_run_id IS NULL)))
    """)
    op.execute("""
        ALTER TABLE conversation_responses
            ADD CONSTRAINT unique_turn_index_per_thread_response
            UNIQUE (conversation_thread_id, turn_index)
    """)
