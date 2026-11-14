"""Migration 124: when a newer pairing of a tunnel's machine superseded it.

A Mac under remote control holds one pairing. Pairing it again, as from a
second LangAlpha account signed in on the same macOS account, gives it new
keys and a new tunnel, and the helper then logs in to the gateway one last
time with the old tunnel's key to say the old tunnel was superseded. The
gateway stamps ``ssh_tunnels.superseded_at``, and a dial to that tunnel is
then answered "paired elsewhere" rather than "not connected", so the computer
still on it says the machine moved to another account or server and offers to
reconnect, instead of asking whether the machine is awake. A connect after
the stamp counts as the machine back on the tunnel.

A nullable column with no default rewrites no rows. NULL is every tunnel
today, which a dial reads as before. The previous build never reads the
column and runs against it untouched, so this runs before any process of the
new build starts, the gateway included, whose login lookup selects it.

Rollback: roll the code back first, then ``alembic downgrade 123`` drops the
column. Tunnels superseded until then read as not connected again.
"""

from alembic import op

revision = "124"
down_revision = "123"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE ssh_tunnels ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMPTZ"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE ssh_tunnels DROP COLUMN IF EXISTS superseded_at")
