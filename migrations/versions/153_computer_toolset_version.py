"""Migration 153: the toolset a computer's sandbox was last seen to run.

``computers.toolset_version`` is the version of the tool table
(``toolset_runtime/units.json``) the sandbox the row names answered a converge
with. It is written only fenced on that sandbox, and a bind to a new sandbox
clears it, so a rebuilt sandbox never inherits the stamp of the one before it.
NULL is a sandbox not yet seen current, which the acquire and the sweep
converge.

A nullable column with no default rewrites no rows, and NULL on every existing
row is the truth: no machine has been asked. The previous build never reads the
column and runs against it untouched.

Rollback: ``alembic downgrade 152`` drops the column.
"""

from alembic import op

revision = "153"
down_revision = "152"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE computers ADD COLUMN IF NOT EXISTS toolset_version TEXT")


def downgrade() -> None:
    op.execute("ALTER TABLE computers DROP COLUMN IF EXISTS toolset_version")
