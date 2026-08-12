"""Add workspace resource tier and always-on columns.

resource_tier names the sandbox spec preset (standard/performance/max) and
is_always_on disables auto-stop. The platform COUNTs these columns to enforce
per-plan entitlement quotas, so they are real columns, not config-derived.

Revision ID: 016
Revises: 015
"""

from alembic import op


revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE workspaces
            ADD COLUMN IF NOT EXISTS resource_tier VARCHAR(32) NOT NULL DEFAULT 'standard'
    """)
    op.execute("""
        ALTER TABLE workspaces
            ADD COLUMN IF NOT EXISTS is_always_on BOOLEAN NOT NULL DEFAULT FALSE
    """)
    # Partial index: the platform's always-on quota count scans per user over
    # the small set of always-on rows.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_workspaces_always_on_by_user
        ON workspaces (user_id)
        WHERE is_always_on
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_workspaces_always_on_by_user")
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS is_always_on")
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS resource_tier")
