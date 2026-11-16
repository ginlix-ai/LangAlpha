"""Migration 128: remember since when a command's machine has not answered.

``command_runs.away_since`` is when the command watcher first found the
command's computer away: a machine of the user's own whose tunnel is down, or
one dialed directly that did not answer. Any read of the computer clears it.
The watcher settles a command as lost once its machine has been away longer
than a command's notice is kept, so a machine that never comes back stops
being looked at. NULL is a command whose machine answered its last look, or
one written before this column.

Catalog work only: a nullable column with no default takes ACCESS EXCLUSIVE
for the catalog update and rewrites nothing. The upgrade clears any value a
stamped rollback left behind, since the previous build's reads never cleared
it and a stale one would give a command up early.

Rollback: this build also parks notifications in ``hook_outbox`` while their
computer is away (pending at ``next_retry_at = 'infinity'``, naming it under
``awaits_computer``), which the previous build never releases. Redeploy the
previous build, which never reads ``away_since``; once no process of this
build is left, release them with the statement in ``_RELEASE_PARKED`` and
``alembic stamp 127``. Run before then, the release misses what this build
parks after it. ``downgrade`` runs the same release and drops the column, so
it too waits until no process of this build is left.
"""

from alembic import op

revision = "128"
down_revision = "127"
branch_labels = None
depends_on = None

# Due again for the previous build's executors: a steer by its ``due_at``,
# every other type by ``next_retry_at``.
_RELEASE_PARKED = """
UPDATE hook_outbox
SET next_retry_at = CASE WHEN hook_type = 'steer'
                         THEN next_retry_at ELSE NOW() END,
    payload = CASE WHEN hook_type = 'steer'
                   THEN (payload - 'due_at')
                        || jsonb_build_object('due_at', NOW())
                   ELSE payload END - 'awaits_computer'
WHERE status = 'pending'
  AND next_retry_at = 'infinity'::timestamptz
  AND payload ? 'awaits_computer'
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE command_runs ADD COLUMN IF NOT EXISTS away_since TIMESTAMPTZ"
    )
    op.execute(
        "UPDATE command_runs SET away_since = NULL WHERE away_since IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(_RELEASE_PARKED)
    op.execute("ALTER TABLE command_runs DROP COLUMN IF EXISTS away_since")
