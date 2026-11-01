"""Migration 099: the token the ``la`` CLI presents from inside a computer.

The server writes a random token into the computer's sandbox so the agent's
commands can call the API as its user, within the scopes the code grants
every computer's token (``cli_tokens.SCOPES``).
Only its SHA-256 is kept here, with the previous one valid until its own
expiry so a rewrite never races a call in flight. Stopping or deleting the
computer deletes the row, which ends every token it held.

``held_by`` names the sandbox that took the current token, since a mint can
land without its write; a sandbox that does not hold the current token is
owed one. ``attempted_at`` spaces out the sweep's tries at a computer that
keeps failing.
"""

from alembic import op

revision = "099"
down_revision = "098"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS computer_cli_tokens (
            computer_id UUID PRIMARY KEY
                REFERENCES computers(computer_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            token_sha256 BYTEA NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            prev_token_sha256 BYTEA,
            prev_expires_at TIMESTAMPTZ,
            held_by VARCHAR(255),
            attempted_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS computer_cli_tokens")
