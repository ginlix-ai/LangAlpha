"""Migration 056: stored thread transcripts.

A thread's transcript is rendered from its checkpoint into per-turn JSONL
files, which the computer's file mount serves from here, so a thread renders
once per change rather than once per read. It is stored per agent: the
thread's own, and each background task's under ``tasks/<task>/``. A
``thread_transcripts`` row records what one agent's copy was rendered from;
``thread_transcript_files`` holds every file of it, the manifest among them:
the bytes themselves when they are small (the manifest's always) or there is
no object storage, else the name of a blob in the same per-user
content-addressed store as workspace files. Both go with the thread, and the
blobs go with the last row that references them, through the registry's sweep.
"""

from alembic import op

revision = "056"
down_revision = "055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # ``prefix`` is where the agent's files sit in the thread's directory:
    # empty for the thread's own agent, ``tasks/<task>/`` for a task.
    op.execute("""
        CREATE TABLE IF NOT EXISTS thread_transcripts (
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            prefix TEXT NOT NULL,
            fingerprint VARCHAR(64) NOT NULL,
            checkpoint_id TEXT,
            stored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (conversation_thread_id, prefix)
        )
    """)
    # ``path`` is the whole path in the thread's directory, so a read needs no
    # agent. ``user_id`` is the blob's namespace, carried on the row so the
    # sweep's reference check needs no join to find whose object it is.
    op.execute("""
        CREATE TABLE IF NOT EXISTS thread_transcript_files (
            conversation_thread_id UUID NOT NULL,
            prefix TEXT NOT NULL,
            path TEXT NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            sha256 VARCHAR(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
            byte_len BIGINT NOT NULL CHECK (byte_len >= 0),
            content BYTEA,
            PRIMARY KEY (conversation_thread_id, path),
            FOREIGN KEY (conversation_thread_id, prefix)
                REFERENCES thread_transcripts(conversation_thread_id, prefix)
                ON DELETE CASCADE
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_thread_transcript_files_sha256 "
        "ON thread_transcript_files (sha256)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS thread_transcript_files")
    op.execute("DROP TABLE IF EXISTS thread_transcripts")
