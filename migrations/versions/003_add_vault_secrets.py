"""Add workspace vault secrets table.

Stores encrypted per-workspace secrets (API keys, credentials)
that are injected into sandbox code execution.

Revision ID: 003
Revises: 002
Create Date: 2026-03-20
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS workspace_vault_secrets (
            workspace_vault_secret_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            value BYTEA NOT NULL,
            description TEXT DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(workspace_id, name)
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_vault_secrets_workspace
        ON workspace_vault_secrets(workspace_id)
    """)

    # Auto-update updated_at on row modification
    op.execute("DROP TRIGGER IF EXISTS update_vault_secrets_updated_at ON workspace_vault_secrets")
    op.execute("""
        CREATE TRIGGER update_vault_secrets_updated_at
        BEFORE UPDATE ON workspace_vault_secrets
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS workspace_vault_secrets CASCADE")
