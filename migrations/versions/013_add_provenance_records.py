"""Add provenance_records: a derived index of external data the agent accessed.

Populated post-turn by extracting top-level `event == "provenance"` entries
from `conversation_responses.sse_events`. The table is a self-healing index —
re-extractable from sse_events if an insert ever fails — so writes are keyed
by conversation_response_id (delete-then-insert) and safe to re-run.

Revision ID: 013
Revises: 012
"""

from alembic import op


revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS provenance_records (
            provenance_record_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_response_id UUID NOT NULL REFERENCES conversation_responses(conversation_response_id) ON DELETE CASCADE,
            conversation_thread_id UUID NOT NULL,
            turn_index INTEGER NOT NULL,
            tool_call_id TEXT,
            source_type TEXT NOT NULL,
            identifier TEXT,
            title TEXT,
            args_fingerprint JSONB,
            args JSONB,
            result_sha256 TEXT,
            result_size BIGINT,
            result_snippet TEXT,
            agent TEXT,
            provider TEXT,
            source_timestamp TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_provenance_records_thread
        ON provenance_records(conversation_thread_id)
    """)

    # Serves the delete-then-insert keyed by response_id + the ON DELETE CASCADE.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_provenance_records_response
        ON provenance_records(conversation_response_id)
    """)

    # NB: no (conversation_thread_id, source_type) index — the only read path
    # (get_provenance_for_thread) filters on conversation_thread_id alone and
    # computes by_source_type counts in Python, so the thread index above
    # already covers it; a composite would just be dead write overhead.


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS provenance_records CASCADE")
