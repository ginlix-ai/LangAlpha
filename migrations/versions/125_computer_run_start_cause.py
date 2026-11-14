"""Migration 125: what started each computer run, and when its stop took.

The run ledger (088, 122) says when a computer ran and what it held, not
what started it. ``start_cause`` and ``start_client`` are what asked for the
start that opened the run (a turn by its origin, a warm, a button, an
automation; ``computer_usage/causes.py``) and the app it came from
(``browser``, ``desktop``, ``mobile``, ``cli``, ``channel``, NULL from the
server itself), so compute can be split by what started it. ``first_down_at``
is when the sweep first found the sandbox down after the run ended
(``seen_down_at`` is the latest such answer), which bounds a stop the
provider was slow to carry out.

All three ride statements the ledger already runs. Each start and stop in
detail (path, step timings, failures, idle time) goes to the computer events
file instead (``computer_usage/events.py``), which costs a status write no
statement at all.

Example: compute by what started it, last 14 days::

    SELECT start_cause, start_client, count(*) AS runs,
           round(sum(extract(epoch FROM
               coalesce(ended_at, now()) - started_at) * cpu) / 3600) AS vcpu_hours
    FROM computer_runs
    WHERE started_at > now() - interval '14 days'
    GROUP BY 1, 2 ORDER BY vcpu_hours DESC NULLS LAST;
"""

from alembic import op

revision = "125"
down_revision = "124"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE computer_runs
            ADD COLUMN IF NOT EXISTS start_cause VARCHAR(32),
            ADD COLUMN IF NOT EXISTS start_client VARCHAR(16),
            ADD COLUMN IF NOT EXISTS first_down_at TIMESTAMPTZ
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE computer_runs
            DROP COLUMN IF EXISTS first_down_at,
            DROP COLUMN IF EXISTS start_client,
            DROP COLUMN IF EXISTS start_cause
    """)
