"""Per-user account-wide disables of builtin MCP servers.

A workspace can already turn a builtin off via a (source='builtin',
enabled=false) marker row, but that is per-workspace. This table is the
account-wide subtraction: a row here removes the builtin from every workspace
of the user, and no workspace marker can re-enable it (the same asymmetry
tombstones have — both tiers are pure subtractions).

name refers to agent_config.yaml builtins, which are config not data, so
there is nothing to FK against; user_id stays a bare VARCHAR per the
convention every user-scoped table has followed since the initial schema.

Revision ID: 027
Revises: 026
"""

from alembic import op


revision = "027"
down_revision = "026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_mcp_builtin_disables (
            user_id     VARCHAR(255) NOT NULL,
            name        VARCHAR(255) NOT NULL,
            disabled_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, name)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS user_mcp_builtin_disables")
