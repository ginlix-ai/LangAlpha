"""Agent Plugins: the plugin entity that owns installed-bundle lifecycle.

A plugin is a wrapper, not a fourth config tier. Installing one fans its
components into the primitives that already exist — user_mcp_servers and
user_skills — stamped with provenance (plugin_id + the original component
key), while user_plugins owns identity, the verbatim manifests, and the
plugin-level enable switch. The resolver never learns plugins exist:
plugin-level disable is a join predicate on the two delivery queries, not
merge algebra.

manifest and mcp_document hold plugin.json / mcp.json exactly as shipped
(mcp_document NULL = absent or invalid component). Nothing binary is stored
at the plugin level: skill bytes live in the sibling user_skills archives,
and export regenerates the package from these three sources. content_hash is
the sha256 of the fetched source archive, for update no-op detection.

The provenance FKs are ON DELETE RESTRICT, not CASCADE or SET NULL: a DB
cascade would delete component rows while skipping the purges their delete
helpers perform unconditionally (workspace tombstones squatting UNIQUE
slots, discovery-schema rows a same-name recreate would resurrect, archive
objects and deny-list markers on skills), so uninstall must walk the
existing helpers first. SET NULL is the detach semantic, which is a
different, deliberate operation. user_skills declared plugin_id and
plugin_skill_dir in 026 awaiting this table; this migration adds the
constraint they were forward-declared for.

Those FKs name (user_id, plugin_id) rather than plugin_id alone, so a row can
only be owned by a plugin belonging to the same user. Every writer already
scopes by user, which is exactly why the pairing is worth stating here: it is
the kind of invariant that holds until one query forgets, and the FK is the
only place it holds regardless. MATCH SIMPLE leaves unowned rows alone — a
NULL plugin_id satisfies the constraint whatever user_id says, so detached and
never-owned rows are unaffected.

user_id stays a bare VARCHAR(255) with NO foreign key to users, per the
convention documented in 025/026.

Revision ID: 028
Revises: 027
"""

from alembic import op


revision = "028"
down_revision = "027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_plugins (
            user_plugin_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            -- Spec §5.5 charset ([a-z0-9.-], no --/..), validated in Python.
            name VARCHAR(64) NOT NULL,
            version TEXT NULL,
            source_type VARCHAR(8) NOT NULL,
            -- Original URL for 'git'; the uploaded filename for 'zip'.
            source_ref TEXT NULL,
            manifest JSONB NOT NULL,
            mcp_document JSONB NULL,
            content_hash TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            installed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (user_id, name)
        )
    """)
    op.execute("DROP TRIGGER IF EXISTS update_user_plugins_updated_at ON user_plugins")
    op.execute("""
        CREATE TRIGGER update_user_plugins_updated_at
        BEFORE UPDATE ON user_plugins
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)
    # Redundant against the primary key, and there to be referenced: it is what
    # lets the provenance FKs below name (user_id, plugin_id) as a pair.
    op.execute(
        "ALTER TABLE user_plugins DROP CONSTRAINT IF EXISTS uq_user_plugins_owner"
    )
    op.execute("""
        ALTER TABLE user_plugins
        ADD CONSTRAINT uq_user_plugins_owner UNIQUE (user_id, user_plugin_id)
    """)

    op.execute("""
        ALTER TABLE user_mcp_servers
        ADD COLUMN IF NOT EXISTS plugin_id UUID NULL,
        ADD COLUMN IF NOT EXISTS plugin_server_key TEXT NULL
    """)
    op.execute("""
        ALTER TABLE user_mcp_servers
        DROP CONSTRAINT IF EXISTS fk_user_mcp_servers_plugin
    """)
    op.execute("""
        ALTER TABLE user_mcp_servers
        ADD CONSTRAINT fk_user_mcp_servers_plugin
        FOREIGN KEY (user_id, plugin_id)
        REFERENCES user_plugins(user_id, user_plugin_id)
        ON DELETE RESTRICT
    """)
    # Plugin uninstall/update scans by owner.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_mcp_servers_plugin
        ON user_mcp_servers(plugin_id)
        WHERE plugin_id IS NOT NULL
    """)

    op.execute("""
        ALTER TABLE user_skills
        DROP CONSTRAINT IF EXISTS fk_user_skills_plugin
    """)
    op.execute("""
        ALTER TABLE user_skills
        ADD CONSTRAINT fk_user_skills_plugin
        FOREIGN KEY (user_id, plugin_id)
        REFERENCES user_plugins(user_id, user_plugin_id)
        ON DELETE RESTRICT
    """)


def downgrade() -> None:
    op.execute(
        "ALTER TABLE user_skills DROP CONSTRAINT IF EXISTS fk_user_skills_plugin"
    )
    op.execute(
        "ALTER TABLE user_mcp_servers "
        "DROP CONSTRAINT IF EXISTS fk_user_mcp_servers_plugin"
    )
    op.execute("DROP INDEX IF EXISTS idx_user_mcp_servers_plugin")
    # Plugin-level suppression lives in the delivery join, (plugin_id IS NULL
    # OR p.enabled), so dropping the column reads as "always deliver" and a
    # rollback would switch every component of a disabled plugin back on. Carry
    # the OFF state onto the rows first, exactly as fork-on-edit does: the
    # downgrade is not consent to start running them.
    op.execute("""
        UPDATE user_mcp_servers s SET enabled = FALSE
        FROM user_plugins p
        WHERE s.plugin_id = p.user_plugin_id AND p.enabled = FALSE
    """)
    op.execute("""
        UPDATE user_skills k SET enabled = FALSE
        FROM user_plugins p
        WHERE k.plugin_id = p.user_plugin_id AND p.enabled = FALSE
    """)
    op.execute(
        "ALTER TABLE user_mcp_servers "
        "DROP COLUMN IF EXISTS plugin_id, DROP COLUMN IF EXISTS plugin_server_key"
    )
    # user_skills.plugin_id belongs to 026, so it survives this downgrade while
    # the rows it points at do not. ADD CONSTRAINT validates on add, so leaving
    # the ids behind makes the next upgrade abort against a recreated (empty)
    # user_plugins. The server-side columns need no equivalent: they are dropped
    # above.
    op.execute(
        "UPDATE user_skills SET plugin_id = NULL, plugin_skill_dir = NULL "
        "WHERE plugin_id IS NOT NULL"
    )
    op.execute("DROP TABLE IF EXISTS user_plugins CASCADE")
