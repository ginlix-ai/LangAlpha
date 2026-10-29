"""Migration 067: what a sign-in saves once the user has signed in.

A server that asks for a sign-in is saved by the sign-in's callback, never
before it, so whatever the save needs has to wait somewhere while the user is
on the vendor's consent screen: the server as the user entered it, the edit
they made, or a whole plugin package with the secret values it was checked
with. ``mcp_oauth_pending`` holds that, one row per sign-in, for as long as
the sign-in can still finish.

Postgres rather than beside the state record in Redis: a plugin package runs
to tens of megabytes, and the start and the callback land on any worker. The
row is keyed by the SHA-256 of the OAuth state, never the state itself, so a
read of the table cannot be turned into a callback. ``payload`` is
``pgp_sym_encrypt`` of the JSON, with the key the vault and the OAuth tokens
use, since it carries secret values. The callback deletes the row as it reads
it, so a payload is spent once. A row whose sign-in expired goes at the same
user's next start or the token refresh sweeper's next pass, whichever comes
first.

Rollback: ``alembic stamp 066`` first, then redeploy the previous build; it
never reads the table. ``downgrade`` drops it, and with it every sign-in still
in flight, whose callbacks then fail as expired.
"""

from alembic import op

revision = "067"
down_revision = "066"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS mcp_oauth_pending (
            state_sha256 BYTEA PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            kind VARCHAR(32) NOT NULL,
            server_name VARCHAR(255) NOT NULL,
            payload BYTEA NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_mcp_oauth_pending_expires_at "
        "ON mcp_oauth_pending (expires_at)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS mcp_oauth_pending")
