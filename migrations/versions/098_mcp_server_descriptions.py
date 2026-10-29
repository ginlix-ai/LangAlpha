"""Migration 098: model-written MCP server descriptions and their queue.

Two new tables, nothing existing touched.

``mcp_server_profiles`` holds each scope's current description under each prompt
version, with the digest of the tools it was written from. A new tool surface
replaces the row, so the agent never reads a description of tools the server no
longer has; a new prompt version writes its own rows beside the old ones.
``model`` records the platform model that wrote a shared row, and is empty for a
user's own server, which runs on whatever model the user picked.

``scope_key`` is ``brokerage:<name>:<groups in force>`` for a shipped brokerage,
shared by every user whose grant leaves their agent the same tools, or
``user:<user_id>:<server>:<url hash>`` for a server a user added, private so one
user's server cannot write what another user's agent reads.

``mcp_description_jobs`` is the queue. Discovery inserts a row when a scope's
current description was written from other tools, and any worker of the same
prompt version claims it. One pending row per scope, for the surface discovery
saw last, so rediscovering the same tools neither queues twice nor resets a
backoff.

Rollback: ``alembic downgrade 097``. The previous build never reads these
tables, so it can also run beside them. Upgrading again starts with no
descriptions, and every server is described anew on its next discovery,
including on users' own models.

Revision ID: 098
Revises: 097
"""

from alembic import op

revision = "098"
down_revision = "097"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS mcp_server_profiles (
            scope_key TEXT NOT NULL,
            prompt_version VARCHAR(32) NOT NULL,
            server_digest VARCHAR(64) NOT NULL,
            model TEXT,
            description TEXT NOT NULL,
            written_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (scope_key, prompt_version)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS mcp_description_jobs (
            job_id BIGSERIAL PRIMARY KEY,
            scope_key TEXT NOT NULL,
            server_digest VARCHAR(64) NOT NULL,
            prompt_version VARCHAR(32) NOT NULL,
            user_id VARCHAR(255),
            server_name VARCHAR(255) NOT NULL,
            tools JSONB NOT NULL,
            status VARCHAR(10) NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'running', 'failed')),
            attempts INTEGER NOT NULL DEFAULT 0,
            run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            lease_until TIMESTAMPTZ,
            error TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_mcp_description_jobs_pending
        ON mcp_description_jobs (scope_key, prompt_version) WHERE status = 'pending'
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_mcp_description_jobs_scope
        ON mcp_description_jobs (scope_key, server_digest)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_mcp_description_jobs_due
        ON mcp_description_jobs (run_after) WHERE status IN ('pending', 'running')
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS mcp_description_jobs")
    op.execute("DROP TABLE IF EXISTS mcp_server_profiles")
