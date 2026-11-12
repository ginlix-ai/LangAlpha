"""Migration 119: sandboxes a create gave up on, until they are deleted.

A Daytona create answers only once the sandbox runs, so one the server stopped
waiting for still comes up, with no computer naming it, and Daytona refuses to
delete it while it is mid-transition. The provider names each create and
records here whatever it could not delete at once; a sweep in every worker
deletes it once Daytona lets it, and drops the row when nothing answers to
the name. ``ref`` is that name, or the sandbox's id when one was known.

A new table, so the previous build runs against it untouched.

Rollback: ``alembic downgrade 118``. Rows still waiting are dropped with the
table; their sandboxes are left to Daytona's auto-stop.
"""

from alembic import op

revision = "119"
down_revision = "118"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS abandoned_sandboxes (
            kind TEXT NOT NULL,
            ref TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            attempted_at TIMESTAMPTZ,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            PRIMARY KEY (kind, ref)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS abandoned_sandboxes")
