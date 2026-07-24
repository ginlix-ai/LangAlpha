"""Add trusted platform-secret rollout state.

Workspace ``config`` is user-replaceable, so security-critical rollout state
must live in a server-owned column.  Rollout rows track each provider Secret's
identity without storing either the placeholder or the plaintext value; the
fleet generation is MAX(generation) over the set, and a workspace's
``platform_secret_version`` records the generation its sandbox was certified
against (0 = never certified).

Revision ID: 021
Revises: 020
"""

from alembic import op


revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS platform_secret_rollouts (
            secret_key VARCHAR(128) PRIMARY KEY,
            provider VARCHAR(64) NOT NULL,
            secret_name VARCHAR(255) NOT NULL,
            provider_secret_id VARCHAR(255) NOT NULL,
            placeholder_sha256 VARCHAR(64) NOT NULL,
            current_credential_sha256 VARCHAR(64) NOT NULL,
            generation INTEGER NOT NULL CHECK (generation > 0),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        ALTER TABLE workspaces
            ADD COLUMN IF NOT EXISTS platform_secret_version
                INTEGER NOT NULL DEFAULT 0
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE workspaces
            DROP COLUMN IF EXISTS platform_secret_version
    """)
    op.execute("DROP TABLE IF EXISTS platform_secret_rollouts")
