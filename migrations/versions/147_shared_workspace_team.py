"""Migration 147: a shared workspace may name one team.

A team is a group of an organization's people that the auth service keeps.
A shared workspace that names one (``team_id``) is reached by everyone in it,
beside the people listed on the workspace, and the team's admins manage it.
The id is the auth service's and is opaque here, so it carries no foreign key;
langalpha stores nothing else of the team. ``team_since`` is when the team was
named, from which a team member who never read a thread has its turns to read
(as ``everyone_since`` is for everyone in the organization).

A workspace open to everyone names no team: the two are alternatives, and the
check keeps a write that forgot one from holding both.

The index serves the service route that counts how many of an
organization's shared workspaces name a team.

Online-safe: two nullable columns, no rewrite; the checks hold on every
existing row (none names a team), and the index is built on a table with one
row per shared workspace. Rollback: roll the code back first, then
``alembic downgrade 146``, which drops the columns and with them every
workspace's team; its listed people keep it.
"""

from alembic import op

revision = "147"
down_revision = "146"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE shared_workspaces "
        "ADD COLUMN IF NOT EXISTS team_id TEXT, "
        "ADD COLUMN IF NOT EXISTS team_since TIMESTAMPTZ"
    )
    op.execute(
        "ALTER TABLE shared_workspaces "
        "ADD CONSTRAINT shared_workspaces_team_since "
        "CHECK ((team_id IS NULL) = (team_since IS NULL)), "
        "ADD CONSTRAINT shared_workspaces_team_or_everyone "
        "CHECK (team_id IS NULL OR NOT everyone)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_shared_workspaces_team "
        "ON shared_workspaces (org_id, team_id) WHERE team_id IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP INDEX IF EXISTS ix_shared_workspaces_team")
    op.execute(
        "ALTER TABLE shared_workspaces "
        "DROP CONSTRAINT IF EXISTS shared_workspaces_team_or_everyone, "
        "DROP CONSTRAINT IF EXISTS shared_workspaces_team_since, "
        "DROP COLUMN IF EXISTS team_since, "
        "DROP COLUMN IF EXISTS team_id"
    )
