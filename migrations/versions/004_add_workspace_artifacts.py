"""Add artifacts JSONB column to workspaces table.

Stores per-workspace artifact state such as preview server commands
(keyed by port) so the preview redirect endpoint can restart servers
without the frontend's help.

Revision ID: 004
Revises: 003
Create Date: 2026-03-25
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE workspaces
        ADD COLUMN IF NOT EXISTS artifacts JSONB NOT NULL DEFAULT '{}'::jsonb
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS artifacts")
