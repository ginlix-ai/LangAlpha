"""Migration 080: a revocation of a computer's market-data tokens still owed
to the platform.

A stop or delete revokes the computer's tokens at the platform. A platform
deploying in place refuses connections for a few seconds, and a revocation
sent then was dropped, leaving every token the sandbox held good until it ran
out. The stop's tombstone now records the revocation as owed, and the sweep
sends it again until the platform confirms:

- ``revoke_until``: when the last token it covers runs out. Past then the
  platform would refuse every one of them anyway, so nothing is owed. NULL
  with nothing owed.
- ``revoke_generation``: the generation of the tombstone that last owed it.
  A confirmation settles only what was owed when its request was sent, so a
  stop recorded while it was in flight stays owed.
- ``revoke_users``: whose tokens it covers. The platform keys a revocation by
  user, and the row's own user moves to the computer's new one at the next
  save after it changes hands, so the users are recorded with what is owed.
  A user the platform confirms leaves the list, and nothing is owed once it
  is empty.
- ``revoke_attempted_at``: when a worker last sent it, so the sweep on every
  worker sends each one about once per retry interval.
- ``tokens_until``: when the last of the tokens saved since the last stop
  runs out, what a stop owes until. ``expires_at`` is the newest token's,
  which a shorter configured lifetime puts before an older one's.
"""

from alembic import op

revision = "080"
down_revision = "079"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE computer_data_tokens
            ADD COLUMN IF NOT EXISTS revoke_until TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS revoke_generation BIGINT,
            ADD COLUMN IF NOT EXISTS revoke_users TEXT[],
            ADD COLUMN IF NOT EXISTS revoke_attempted_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS tokens_until TIMESTAMPTZ
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE computer_data_tokens
            DROP COLUMN IF EXISTS tokens_until,
            DROP COLUMN IF EXISTS revoke_attempted_at,
            DROP COLUMN IF EXISTS revoke_users,
            DROP COLUMN IF EXISTS revoke_generation,
            DROP COLUMN IF EXISTS revoke_until
    """)
