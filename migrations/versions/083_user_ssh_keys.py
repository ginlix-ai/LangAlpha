"""Migration 083: each user's SSH client key, for the machines they bring.

A computer on the ``ssh`` kind is the user's own machine, reached with a key
the server holds for that user alone. One key per user, minted on first ask:
the user puts its public half in the machine's ``authorized_keys`` once, and
every machine they add trusts the same key, while no other user's computer
can authenticate with it.

``private_key`` is ``pgp_sym_encrypt`` of the OpenSSH private key text, with
the key the vault and the OAuth tokens use. ``public_key`` and ``fingerprint``
stay plain, since they are what the user is shown and pastes elsewhere.
``user_id`` is a bare VARCHAR with no foreign key, like the vault's.

Rollback: ``alembic downgrade 082``. It drops the table, so every machine
already added stops authenticating until the user adds a new key to it.
"""

from alembic import op

revision = "083"
down_revision = "082"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_ssh_keys (
            user_id VARCHAR(255) PRIMARY KEY,
            public_key TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            private_key BYTEA NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS user_ssh_keys")
