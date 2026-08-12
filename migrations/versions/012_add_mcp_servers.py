"""Add per-workspace, user-configured MCP server tables.

Adds the catalog of user-level MCP server templates (user_mcp_servers),
the per-workspace source of truth (workspace_mcp_servers), the discovery
schema cache keyed by a per-server config hash (workspace_mcp_tool_schemas),
and a mcp_config_version counter on workspaces that every workspace-row
mutation bumps so sessions can detect config drift without a new per-turn
query.

Secrets are never stored here — env/header values hold "${vault:NAME}"
references resolved against workspace_vault_secrets at sandbox runtime.

Revision ID: 012
Revises: 011
"""

from alembic import op


revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # User-level templates: UI convenience, copied into a workspace on
    # "add to workspace" — never inherited at runtime.
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_mcp_servers (
            user_mcp_server_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            transport VARCHAR(16) NOT NULL DEFAULT 'stdio',
            command TEXT NULL,
            args JSONB NOT NULL DEFAULT '[]',
            url TEXT NULL,
            env JSONB NOT NULL DEFAULT '{}',
            headers JSONB NOT NULL DEFAULT '{}',
            description TEXT NOT NULL DEFAULT '',
            instruction TEXT NOT NULL DEFAULT '',
            tool_exposure_mode VARCHAR(16) NOT NULL DEFAULT 'summary',
            discovery_uses_secrets BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(user_id, name)
        )
    """)

    # No separate user_id index: UNIQUE(user_id, name) already serves
    # user_id-prefix lookups and ORDER BY name.
    op.execute("DROP TRIGGER IF EXISTS update_user_mcp_servers_updated_at ON user_mcp_servers")
    op.execute("""
        CREATE TRIGGER update_user_mcp_servers_updated_at
        BEFORE UPDATE ON user_mcp_servers
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)

    # Per-workspace source of truth: one row per active user server, plus one
    # disable-marker row per disabled built-in.
    op.execute("""
        CREATE TABLE IF NOT EXISTS workspace_mcp_servers (
            workspace_mcp_server_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            source VARCHAR(16) NOT NULL,
            enabled BOOLEAN NOT NULL,
            config JSONB NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(workspace_id, name)
        )
    """)

    # No separate workspace_id index: UNIQUE(workspace_id, name) covers it.
    op.execute("DROP TRIGGER IF EXISTS update_workspace_mcp_servers_updated_at ON workspace_mcp_servers")
    op.execute("""
        CREATE TRIGGER update_workspace_mcp_servers_updated_at
        BEFORE UPDATE ON workspace_mcp_servers
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)

    # Discovery cache keyed by a per-server config_hash (a fingerprint of that
    # server's own discovery-affecting config) so a cached snapshot survives
    # unrelated mutations to OTHER servers and is invalidated only when that
    # server's own config changes.
    op.execute("""
        CREATE TABLE IF NOT EXISTS workspace_mcp_tool_schemas (
            workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            server_name VARCHAR(255) NOT NULL,
            config_hash TEXT NOT NULL,
            tools JSONB NOT NULL DEFAULT '[]',
            status VARCHAR(16) NOT NULL DEFAULT 'pending',
            error TEXT NOT NULL DEFAULT '',
            observed_meta JSONB NOT NULL DEFAULT '{}',
            discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(workspace_id, server_name, config_hash)
        )
    """)

    # Lookup is latest-snapshot-per-server (any hash); the caller matches the
    # hash against the server's current fingerprint.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_workspace_mcp_tool_schemas_lookup
        ON workspace_mcp_tool_schemas(workspace_id, server_name, discovered_at DESC)
    """)

    # Versioned config tag: schema-cache key + session-cache invalidation
    # signal. Every workspace MCP-row mutation bumps it in the same txn.
    op.execute("""
        ALTER TABLE workspaces
        ADD COLUMN IF NOT EXISTS mcp_config_version INTEGER NOT NULL DEFAULT 0
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS mcp_config_version")
    op.execute("DROP TABLE IF EXISTS workspace_mcp_tool_schemas CASCADE")
    op.execute("DROP TABLE IF EXISTS workspace_mcp_servers CASCADE")
    op.execute("DROP TABLE IF EXISTS user_mcp_servers CASCADE")
