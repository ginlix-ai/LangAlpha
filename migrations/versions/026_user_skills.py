"""User- and workspace-tier skills: per-user skill records + their archives.

Adds the user-owned skill tiers. Until now a skill was either platform-authored
(SKILL_REGISTRY + the repo's skills/ directory) or agent-installed (discovered
by scanning a sandbox's .agents/skills, invisible to the server). Neither can
represent "a skill this user brought and owns", which is what plugin install
fans its skills component into.

A row is user-scoped (workspace_id NULL, visible in every workspace) or
workspace-scoped (workspace_id set, visible only there). Both scopes live in
one table because they share the whole archive/materialization pipeline; scope
is just a shadowing key. Uniqueness is per scope, via two partial indexes: a
workspace row may reuse a user-tier name and shadows it in that workspace,
mirroring how workspace MCP servers shadow user-level connectors by name.

workspace_skill_disables holds per-workspace disables of skills the workspace
merely inherits (platform builtins and the user tier). A dedicated table
rather than a row flag because the inherited skill has no row in this scope
to flag; mirrors user_mcp_builtin_disables. No FK on workspace_id for the
same reason user_id has none, and rows deliberately survive workspace soft
delete (the workspace convention; MCP rows behave the same).

A row carries the denormalized SKILL.md frontmatter so listings and the agent
build never need to open the archive; the archive itself is the source of
truth for the files, stored content-addressed in object storage (archive_key)
or, when no object storage is configured, inline (archive_blob). Exactly one
of the two is non-null.

plugin_id / plugin_skill_dir are declared here but left unconstrained: the
user_plugins table plugin_id will reference is a sibling change, and a skill
uploaded directly is plugin-less forever. Clearing them in place is the
fork-on-edit affordance — a plugin update then sees the name un-owned and
skips it rather than overwriting a customization.

user_id is deliberately a bare VARCHAR(255) with NO foreign key to users, the
convention every user-scoped table has followed since the initial schema: the
request path resolves user_id from a JWT sub or a relayed X-User-Id header
with no DB read and no get-or-create, so an FK would turn a first write into
an unhandled foreign-key violation.

Revision ID: 026
Revises: 025
"""

from alembic import op


revision = "026"
down_revision = "025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_skills (
            user_skill_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            workspace_id UUID NULL,
            name VARCHAR(64) NOT NULL,
            -- Slash-command alias; NULL means the name is the trigger. The
            -- column is authoritative after creation (frontmatter only seeds).
            command VARCHAR(64) NULL,
            description TEXT NOT NULL DEFAULT '',
            license TEXT NULL,
            frontmatter JSONB NOT NULL DEFAULT '{}',
            allowed_tools JSONB NOT NULL DEFAULT '[]',
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            confirmed BOOLEAN NOT NULL DEFAULT TRUE,
            plugin_id UUID NULL,
            plugin_skill_dir TEXT NULL,
            content_hash VARCHAR(71) NOT NULL,
            archive_key TEXT NULL,
            archive_blob BYTEA NULL,
            archive_bytes BIGINT NOT NULL DEFAULT 0,
            file_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            -- The archive lives in object storage or inline, never both and
            -- never neither: a row with no retrievable bytes would advertise a
            -- skill in the manifest that can never be materialized.
            CONSTRAINT user_skills_archive_present CHECK (
                (archive_key IS NULL) <> (archive_blob IS NULL)
            )
        )
    """)
    op.execute("DROP TRIGGER IF EXISTS update_user_skills_updated_at ON user_skills")
    op.execute("""
        CREATE TRIGGER update_user_skills_updated_at
        BEFORE UPDATE ON user_skills
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)

    # Per-scope uniqueness: one name per user tier, one name per workspace.
    # Partial indexes rather than one UNIQUE(user_id, workspace_id, name)
    # because NULL workspace_id rows would never collide under a composite
    # unique; ON CONFLICT infers each index by columns + predicate.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_user_skills_user_name
        ON user_skills(user_id, name)
        WHERE workspace_id IS NULL
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_user_skills_workspace_name
        ON user_skills(workspace_id, name)
        WHERE workspace_id IS NOT NULL
    """)
    # Command aliases mirror the name uniqueness per scope. Only explicit
    # aliases collide here; name-as-default-trigger overlap is checked
    # app-side (it spans two columns).
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_user_skills_user_command
        ON user_skills(user_id, command)
        WHERE workspace_id IS NULL AND command IS NOT NULL
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_user_skills_workspace_command
        ON user_skills(workspace_id, command)
        WHERE workspace_id IS NOT NULL AND command IS NOT NULL
    """)

    # Every per-user read goes through user_id: the per-turn enabled-rows
    # query, the management listings, and the cap check. Unpartitioned on
    # purpose — a `WHERE enabled` partial index would leave the listings and
    # the cap count (which must see disabled rows) on a sequential scan.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_skills_user
        ON user_skills(user_id)
    """)
    # Archive keys are content-addressed and shared, so every delete asks
    # whether another row still points at the object.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_skills_archive_key
        ON user_skills(archive_key)
        WHERE archive_key IS NOT NULL
    """)
    # Workspace skill management lists by workspace.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_skills_workspace
        ON user_skills(workspace_id)
        WHERE workspace_id IS NOT NULL
    """)
    # Plugin uninstall/update scans by owner.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_skills_plugin
        ON user_skills(plugin_id)
        WHERE plugin_id IS NOT NULL
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS workspace_skill_disables (
            workspace_id UUID NOT NULL,
            name VARCHAR(64) NOT NULL,
            disabled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (workspace_id, name)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS workspace_skill_disables CASCADE")
    op.execute("DROP TABLE IF EXISTS user_skills CASCADE")
