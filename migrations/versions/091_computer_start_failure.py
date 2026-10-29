"""Migration 091: why a computer's last start failed.

A start runs behind a 202 and its failure reached the owner only as a status
frame on the stream, so a client that was not listening, or reloaded, saw a
stopped computer with no reason and no way to fix it. A machine whose folder
another computer took, or whose tunnel is gone, fails the same way on every
start, and nothing said so.

``computers.start_failure`` keeps the last failed start: its ``code``, the
owner's ``message``, the ``owner`` a folder's mark named, the ``remedy`` the
server chose (reinstall, reconnect or retry) and ``at``. Every start path
writes it as it fails, and the write that marks the computer running clears
it, as do a reinstall or reconnect once accepted.

Adding a nullable column rewrites no rows, so the lock is held only to change
the catalog. The previous build lists its columns by name and never reads
this one.

Rollback: ``alembic downgrade 090``. It drops the column, and a failed start
is again only a frame on the stream.
"""

from alembic import op

revision = "091"
down_revision = "090"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE computers ADD COLUMN IF NOT EXISTS start_failure JSONB NULL"
    )
    op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET lock_timeout = '5s'")
    op.execute("ALTER TABLE computers DROP COLUMN IF EXISTS start_failure")
    op.execute("RESET lock_timeout")
