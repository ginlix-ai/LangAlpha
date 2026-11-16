"""Migration 129: name the computer a waiting automation run waits for, and
keep the tunnel gateway's own liveness, without which no tunnel's is judged.

A firing whose computer cannot be reached (``wait_reason`` is
``computer_unreachable``) is parked as ``waiting`` rather than held by a
process. ``automation_executions.awaits_computer`` is the computer it waits
for, so the scheduler's sweep can release it in one guarded statement once
that computer's tunnel is live again, as the outbox does for a notification.
``parked_status`` is the automation's status when it parked, so the release
tells an automation paused meanwhile from a manual run of a paused one.
``awaits_until`` is when the sweep gives the wait up, set at the first park
and kept by every later one, so a machine that comes back only briefly does
not start the wait over. All are NULL on a firing that never parked, or one
written before these columns, which the sweep releases to look at its
computer again.

``ssh_gateway_state`` is one row the running gateway writes: ``started_at``
when it took the lock, ``seen_at`` with every heartbeat, cleared when it
stops. A tunnel only reads as down while that gateway beat recently and has
run long enough for machines to reconnect to it, so the gateway's own outage
or restart never reads as every user's machine gone. No row is a gateway
that never ran on this build, under which no tunnel reads as down.

Catalog work only: nullable columns with no default take ACCESS EXCLUSIVE for
the catalog update and rewrite nothing, and the new table is empty.

Rollback: a parked firing's heartbeat is dated to the end of its wait, up to
seven days out, so the previous build's sweep of abandoned firings, which
alone would settle it, would not look at it until then. Redeploy the previous
build, which never reads the columns or the table; once no process of this
build is left, release the parked firings with the statement in
``_RELEASE_PARKED`` and ``alembic stamp 128``. The release dates their
heartbeat to now, so the sweep settles each as skipped within minutes, as it
settles any wait whose process is gone; run before then, it misses what this
build parks after it. ``downgrade`` runs the same release and drops them, so
it too waits until no process of this build is left, since the executor
writes the columns and the gateway the table.
"""

from alembic import op

revision = "129"
down_revision = "128"
branch_labels = None
depends_on = None

_RELEASE_PARKED = """
UPDATE automation_executions
SET heartbeat_at = NOW()
WHERE status = 'waiting'
  AND awaits_computer IS NOT NULL
  AND heartbeat_at > NOW()
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE automation_executions "
        "ADD COLUMN IF NOT EXISTS awaits_computer UUID, "
        "ADD COLUMN IF NOT EXISTS parked_status TEXT, "
        "ADD COLUMN IF NOT EXISTS awaits_until TIMESTAMPTZ"
    )
    op.execute("""
        CREATE TABLE IF NOT EXISTS ssh_gateway_state (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            started_at TIMESTAMPTZ NOT NULL,
            seen_at TIMESTAMPTZ
        )
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(_RELEASE_PARKED)
    op.execute("DROP TABLE IF EXISTS ssh_gateway_state")
    op.execute(
        "ALTER TABLE automation_executions "
        "DROP COLUMN IF EXISTS awaits_computer, "
        "DROP COLUMN IF EXISTS parked_status, "
        "DROP COLUMN IF EXISTS awaits_until"
    )
