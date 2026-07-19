"""Add Robinhood OAuth (Agentic Trading MCP) connection table.

Stores per-(user, workspace) OAuth state for Robinhood's Agentic Trading MCP:
the dynamically-registered client (RFC 7591), the discovered authorization /
token endpoints, and encrypted access/refresh tokens. The live access token is
ALSO mirrored into the workspace vault (``ROBINHOOD_TOKEN``) so the sandbox can
send it; this table holds the refresh context the vault can't carry (client_id,
token endpoint, refresh token, expiry) plus the transient PKCE state while a
connect is pending.

Revision ID: 016
Revises: 015
Create Date: 2026-06-30
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "016"
down_revision: Union[str, None] = "015"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS robinhood_oauth (
            robinhood_oauth_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            workspace_id UUID NOT NULL
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            -- 'pending' between initiate and callback; 'connected' after exchange.
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            -- RFC 8707 resource indicator = the MCP server URL.
            resource TEXT NOT NULL,
            authorization_endpoint TEXT,
            token_endpoint TEXT,
            registration_endpoint TEXT,
            client_id TEXT,
            client_secret BYTEA,          -- encrypted; NULL for public clients
            redirect_uri TEXT,
            scopes TEXT DEFAULT '',
            state TEXT,                    -- transient (pending only)
            code_verifier BYTEA,           -- encrypted; transient (pending only)
            access_token BYTEA,            -- encrypted; NULL until connected
            refresh_token BYTEA,           -- encrypted; NULL until connected
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(user_id, workspace_id)
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_robinhood_oauth_workspace
        ON robinhood_oauth(workspace_id)
    """)

    # Auto-update updated_at on row modification (shared trigger fn from 001).
    op.execute(
        "DROP TRIGGER IF EXISTS update_robinhood_oauth_updated_at ON robinhood_oauth"
    )
    op.execute("""
        CREATE TRIGGER update_robinhood_oauth_updated_at
        BEFORE UPDATE ON robinhood_oauth
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS robinhood_oauth CASCADE")
