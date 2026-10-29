"""Migration 076: the market-data token each computer's sandbox holds.

The server mints a short-lived, access-only token per computer from the
platform and writes it into the sandbox; the sandbox never refreshes it. This
row is what every worker knows of it: when the token in the sandbox runs out
(``expires_at``) and which sandbox took it (``held_by``, NULL until a write
lands), so a turn's start and the background sweep each know whether a fresh
one is owed. The token itself is kept nowhere but the sandbox.

A stop or delete revokes the computer's tokens at the platform and leaves the
row with no expiry rather than deleting it. ``generation`` moves on every
write, revocations included, and a save names the generation it read: a push
that minted before a stop's revocation and saves after the computer started
again finds the generation moved and drops its token, instead of putting a
revoked one in front of the fresh one.

``attempted_at`` is when the background sweep last took the computer up,
written as it claims it and whatever the outcome, so a computer whose sandbox
keeps refusing the token waits its turn behind the others instead of filling
every pass.

A table of its own, not columns on ``computers``: writing a computer's row
moves its ``updated_at``, which the idle reaper compares to decide a stop, so
a token push every half hour would keep resetting it.
"""

from alembic import op

revision = "076"
down_revision = "075"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS computer_data_tokens (
            computer_id UUID PRIMARY KEY
                REFERENCES computers(computer_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            expires_at TIMESTAMPTZ,
            held_by VARCHAR(255),
            generation BIGINT NOT NULL DEFAULT 1,
            attempted_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS computer_data_tokens")
