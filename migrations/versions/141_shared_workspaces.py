"""Migration 141: shared workspaces.

A shared workspace is a workspace on a cloud computer of its own, both rows
owned by a holder: a ``users`` row that is not a person (id ``shared-<uuid>``,
``auth_provider = 'shared_workspace'``), made for that one workspace. It never
signs in and never acts; members' turns run as the member who sends them.

``shared_workspaces`` names the organization the workspace belongs to (the
auth service's opaque id, no foreign key) and whether everyone in it may use
the workspace. ``shared_workspace_members`` lists the people, each a manager
or a member. ``conversation_threads.created_by`` records who started a thread
in a shared workspace; it stays NULL everywhere else. ``shared_file_grant``
is the key a member's file grant is signed with (``services/file_grants``).

Online-safe: two new empty tables and a nullable column with no default
(catalog-only on a large table). No backfill. Rollback: roll the code back
first, then ``alembic downgrade 140``; it refuses while any shared workspace
is live, since its holder would otherwise stay owner of record of working
rows with no member list. A deleted one's holder stays as an inert row.
"""

from alembic import op

revision = "141"
down_revision = "140"
branch_labels = None
depends_on = None

_KEY = "shared_file_grant"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS shared_workspaces (
            workspace_id UUID PRIMARY KEY
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            holder_id VARCHAR(255) NOT NULL UNIQUE
                REFERENCES users(user_id) ON DELETE CASCADE,
            org_id TEXT NOT NULL,
            everyone BOOLEAN NOT NULL DEFAULT FALSE,
            created_by VARCHAR(255) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT shared_workspaces_holder_id
                CHECK (holder_id LIKE 'shared-%')
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_shared_workspaces_org "
        "ON shared_workspaces (org_id)"
    )
    op.execute("""
        CREATE TABLE IF NOT EXISTS shared_workspace_members (
            workspace_id UUID NOT NULL
                REFERENCES shared_workspaces(workspace_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            role VARCHAR(16) NOT NULL
                CONSTRAINT shared_workspace_members_role
                CHECK (role IN ('manager', 'member')),
            added_by VARCHAR(255) NOT NULL,
            added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (workspace_id, user_id)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_shared_workspace_members_user "
        "ON shared_workspace_members (user_id)"
    )
    op.execute(
        "ALTER TABLE conversation_threads "
        "ADD COLUMN IF NOT EXISTS created_by VARCHAR(255)"
    )
    op.execute(f"""
        INSERT INTO server_keys (name, secret)
        VALUES ('{_KEY}', gen_random_bytes(32))
        ON CONFLICT (name) DO NOTHING
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        DO $$ BEGIN
            IF to_regclass('shared_workspaces') IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM shared_workspaces s
                   JOIN workspaces w ON w.workspace_id = s.workspace_id
                   WHERE w.status <> 'deleted'
               ) THEN
                RAISE EXCEPTION 'Cannot downgrade while shared workspaces exist; delete them first';
            END IF;
        END $$;
    """)
    op.execute("ALTER TABLE conversation_threads DROP COLUMN IF EXISTS created_by")
    op.execute("DROP TABLE IF EXISTS shared_workspace_members")
    op.execute("DROP TABLE IF EXISTS shared_workspaces")
    op.execute(f"DELETE FROM server_keys WHERE name = '{_KEY}'")
