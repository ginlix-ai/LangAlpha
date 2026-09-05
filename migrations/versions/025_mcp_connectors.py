"""User-level MCP connectors: live user servers, user vault, OAuth, egress grants.

Promotes user_mcp_servers from an inert template catalog to live config (an
`enabled` flag; enabled rows are inherited by every workspace of the user at
resolve time). Adds the user-scoped vault (same pgcrypto-at-rest pattern as
workspace_vault_secrets), the OAuth connection store for remote MCP servers
(encrypted token bundle — the refresh token never leaves this table), the
user-level discovery cache for OAuth servers (discovered host-side, so it is
user- not workspace-scoped), and sandbox_egress_grants — the contract of the
egress relay: sandboxes reach credential-bearing remote servers only through
a grant whose destination was captured server-side at creation.

user_id is deliberately a bare VARCHAR(255) with NO foreign key to users, as
in every user-scoped table added since the initial schema (user_oauth_tokens,
user_mcp_servers). A users row is not guaranteed to exist: the request path
resolves user_id from a JWT sub or a relayed X-User-Id header with no DB read
and no get-or-create, so an FK would turn a first-touch connector write from
a channel integration into an unhandled foreign-key violation. Cleanup on
user deletion is therefore the deleter's job, not the schema's.

Revision ID: 025
Revises: 024
"""

from alembic import op


revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # sandbox_egress_grants below uses UNIQUE NULLS NOT DISTINCT, which requires
    # PostgreSQL 15+. Dev runs pg18 and prod Aurora is >=15, so this is met — but
    # fail with a message that names the requirement rather than a bare syntax
    # error if this migration is ever applied to an older server.
    op.execute("""
        DO $$
        BEGIN
            IF current_setting('server_version_num')::int < 150000 THEN
                RAISE EXCEPTION
                    'migration 025 requires PostgreSQL 15+ (UNIQUE NULLS NOT '
                    'DISTINCT); server is %', current_setting('server_version');
            END IF;
        END $$;
    """)

    # Existing template rows stay inert (enabled=false) until the user
    # activates them in the Connectors UI.
    op.execute("""
        ALTER TABLE user_mcp_servers
        ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT FALSE
    """)

    # User-scoped vault: merged with the workspace vault at sandbox push,
    # workspace wins on name collision.
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_vault_secrets (
            user_vault_secret_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            value BYTEA NOT NULL,
            description TEXT DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(user_id, name)
        )
    """)
    op.execute("DROP TRIGGER IF EXISTS update_user_vault_secrets_updated_at ON user_vault_secrets")
    op.execute("""
        CREATE TRIGGER update_user_vault_secrets_updated_at
        BEFORE UPDATE ON user_vault_secrets
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)

    # OAuth connection per (user, server). Tokens pgcrypto-encrypted with
    # BYOK_ENCRYPTION_KEY; the refresh token is a host-only singleton and is
    # never distributed anywhere. token_generation increments on every
    # successful refresh so a caller holding a stale bundle can detect
    # rotation. status: connected | needs_reauth | refresh_ambiguous | revoked
    # (refresh_ambiguous = a refresh timed out ambiguously — the old access
    # token stays in use until expiry but the refresh token must never be
    # retried).
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_mcp_oauth_connections (
            connection_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            server_name VARCHAR(255) NOT NULL,
            server_url TEXT NOT NULL,
            access_token BYTEA NULL,
            refresh_token BYTEA NULL,
            token_type VARCHAR(32) NOT NULL DEFAULT 'Bearer',
            scope TEXT NULL,
            expires_at TIMESTAMPTZ NULL,
            token_generation INTEGER NOT NULL DEFAULT 0,
            client_info JSONB NULL,
            client_secret BYTEA NULL,
            as_metadata JSONB NULL,
            resource_metadata JSONB NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'connected',
            last_refresh_at TIMESTAMPTZ NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(user_id, server_name)
        )
    """)
    op.execute("DROP TRIGGER IF EXISTS update_user_mcp_oauth_connections_updated_at ON user_mcp_oauth_connections")
    op.execute("""
        CREATE TRIGGER update_user_mcp_oauth_connections_updated_at
        BEFORE UPDATE ON user_mcp_oauth_connections
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)
    # The refresh sweeper scans due connected rows only.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_mcp_oauth_connections_due
        ON user_mcp_oauth_connections(expires_at)
        WHERE status = 'connected'
    """)

    # User-level discovery cache for OAuth servers (host-side short-lived SDK
    # sessions — not workspace sandboxes — run this discovery). schema_digest
    # is a content hash of the sanitized tool list: workspace version fan-out
    # happens only when the digest actually changes.
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_mcp_tool_schemas (
            user_id VARCHAR(255) NOT NULL,
            server_name VARCHAR(255) NOT NULL,
            config_hash TEXT NOT NULL,
            tools JSONB NOT NULL DEFAULT '[]',
            status VARCHAR(16) NOT NULL DEFAULT 'pending',
            error TEXT NOT NULL DEFAULT '',
            schema_digest TEXT NOT NULL DEFAULT '',
            observed_meta JSONB NOT NULL DEFAULT '{}',
            discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(user_id, server_name, config_hash)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_mcp_tool_schemas_lookup
        ON user_mcp_tool_schemas(user_id, server_name, discovered_at DESC)
    """)

    # The egress relay's contract. destination_url is captured server-side at
    # grant creation and is the ONLY place the relay will dial — requests can
    # never steer it. kind + resolver_config is the typed extension point for
    # future credential kinds; v1 is 'oauth_mcp' with connection_id set.
    # tool_allowlist NULL = no tool policy (Part 2 populates it).
    op.execute("""
        CREATE TABLE IF NOT EXISTS sandbox_egress_grants (
            grant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            kind VARCHAR(32) NOT NULL,
            connection_id UUID NULL REFERENCES user_mcp_oauth_connections(connection_id) ON DELETE CASCADE,
            resolver_config JSONB NOT NULL DEFAULT '{}',
            destination_url TEXT NOT NULL,
            allowed_methods JSONB NOT NULL DEFAULT '["POST"]',
            tool_allowlist JSONB NULL,
            policy_version INTEGER NOT NULL DEFAULT 1,
            status VARCHAR(16) NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            -- NULLS NOT DISTINCT: connection_id is NULL for any non-'oauth_mcp'
            -- kind, and the default NULLS DISTINCT would give those rows no
            -- uniqueness at all.
            UNIQUE NULLS NOT DISTINCT (workspace_id, kind, connection_id)
        )
    """)
    op.execute("DROP TRIGGER IF EXISTS update_sandbox_egress_grants_updated_at ON sandbox_egress_grants")
    op.execute("""
        CREATE TRIGGER update_sandbox_egress_grants_updated_at
        BEFORE UPDATE ON sandbox_egress_grants
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)
    # Connection revoke/cascade scans grants by connection_id alone; the UNIQUE
    # above leads with workspace_id and can't serve it.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_sandbox_egress_grants_connection
        ON sandbox_egress_grants(connection_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS sandbox_egress_grants CASCADE")
    op.execute("DROP TABLE IF EXISTS user_mcp_tool_schemas CASCADE")
    op.execute("DROP TABLE IF EXISTS user_mcp_oauth_connections CASCADE")
    op.execute("DROP TABLE IF EXISTS user_vault_secrets CASCADE")
    op.execute("ALTER TABLE user_mcp_servers DROP COLUMN IF EXISTS enabled")
