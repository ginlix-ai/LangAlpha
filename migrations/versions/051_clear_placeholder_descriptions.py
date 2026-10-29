"""Migration 051: clear the seeded description placeholder from workspace rows.

Workspaces seeded before agent.md lost its front matter had the template's
placeholder copied into ``description`` by the front-matter sync, and nothing
has overwritten it since, so cards show the instruction text as if it were a
description. Only the exact seeded string is cleared; the downgrade is a no-op
because the placeholder was never meant to be data.
"""

from alembic import op

revision = "051"
down_revision = "050"
branch_labels = None
depends_on = None

_PLACEHOLDER = (
    "Brief 1-2 sentence description — update based on the first conversation."
)


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # The updated_at trigger would stamp every cleaned row as just edited and
    # float long-idle workspaces to the top of the gallery.
    op.execute("ALTER TABLE workspaces DISABLE TRIGGER trg_workspaces_updated_at")
    op.execute(
        f"UPDATE workspaces SET description = NULL WHERE description = '{_PLACEHOLDER}'"
    )
    op.execute("ALTER TABLE workspaces ENABLE TRIGGER trg_workspaces_updated_at")


def downgrade() -> None:
    pass
