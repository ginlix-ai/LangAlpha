"""Add 'starting' to workspaces.status CHECK constraint.

Used by the lazy-restart path in WorkspaceManager: stopped -> starting -> running.
The intermediate 'starting' lets /files and /public callers fall back to the DB
while Phase 2 (ensure_sandbox_ready + asset sync) is still resolving.

Revision ID: 009
"""

from alembic import op


revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE workspaces
            DROP CONSTRAINT IF EXISTS workspaces_status_check
    """)
    op.execute("""
        ALTER TABLE workspaces
            ADD CONSTRAINT workspaces_status_check
            CHECK (status IN (
                'creating','running','starting','stopping',
                'stopped','error','deleted','flash'
            ))
    """)


def downgrade() -> None:
    # Any rows parked in 'starting' would violate the older constraint; coerce
    # them to 'stopped' so the next request re-enters the restart flow cleanly.
    op.execute("""
        UPDATE workspaces SET status = 'stopped' WHERE status = 'starting'
    """)
    op.execute("""
        ALTER TABLE workspaces
            DROP CONSTRAINT IF EXISTS workspaces_status_check
    """)
    op.execute("""
        ALTER TABLE workspaces
            ADD CONSTRAINT workspaces_status_check
            CHECK (status IN (
                'creating','running','stopping',
                'stopped','error','deleted','flash'
            ))
    """)
