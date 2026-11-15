"""Migration 129: name the computer a waiting automation run waits for, and
keep the tunnel gateway's own liveness, without which no tunnel's is judged.

A firing whose computer cannot be reached (``wait_reason`` is
``computer_unreachable``) is parked as ``waiting`` rather than held by a
process. ``automation_executions.awaits_computer`` is the computer it waits
for, so the scheduler's sweep can release it in one guarded statement once
that computer's tunnel is live again, as the outbox does for a notification.
``parked_status`` is the automation's status when it parked, so the release
tells an automation paused meanwhile from a manual run of a paused one. Both
are NULL on a firing that never parked, or one written before these columns,
which the sweep releases to look at its computer again.

``ssh_gateway_state`` is one row the running gateway writes: ``started_at``
when it took the lock, ``seen_at`` with every heartbeat, cleared when it
stops. A tunnel only reads as down while that gateway beat recently and has
run long enough for machines to reconnect to it, so the gateway's own outage
or restart never reads as every user's machine gone. No row is a gateway
that never ran on this build, under which no tunnel reads as down.

Catalog work only: nullable columns with no default take ACCESS EXCLUSIVE for
the catalog update and rewrite nothing, and the new table is empty. Rollback
leaves the columns and the table in place, since the previous build never
reads them: ``alembic stamp 128`` first, then redeploy it. ``downgrade`` is
only safe once no process of this build is left, since the executor writes
the columns and the gateway the table.
"""

from alembic import op

revision = "129"
down_revision = "128"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE automation_executions "
        "ADD COLUMN IF NOT EXISTS awaits_computer UUID, "
        "ADD COLUMN IF NOT EXISTS parked_status TEXT"
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
    op.execute("DROP TABLE IF EXISTS ssh_gateway_state")
    op.execute(
        "ALTER TABLE automation_executions "
        "DROP COLUMN IF EXISTS awaits_computer, "
        "DROP COLUMN IF EXISTS parked_status"
    )
