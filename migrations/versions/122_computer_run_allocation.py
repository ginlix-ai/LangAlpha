"""Migration 122: each run's provider-reported size, and the disk held after it.

Two things 088's ledger could not say about what a user's sandboxes held.

The size a run was opened with is the tier's preset as configured at open,
not the sandbox's: a sandbox keeps the size it was created at, and a preset
changed since then (or a create that fell back to the base snapshot) left
runs naming a size their sandbox never had. The sweep now asks the provider
(``SandboxProvider.allocation``, a control-plane lookup) and writes the answer
over every run on that sandbox, since a sandbox is never resized.
``size_source`` is NULL until then, ``provider`` once the provider has said,
``partial`` while its answers have left a size out (asked again until one
has it), and ``tier`` when the sandbox was gone first, so the preset stays
for what it never said. A run opened on a sandbox whose size is known copies it.

A stopped Daytona sandbox still bills for its disk until it is archived or
deleted. ``held_until`` is when that hold ended after the run, and
``held_end`` why: ``restarted`` (the next run on the sandbox opened),
``deleted`` (its computer's delete, or the replacement that deleted the
sandbox), ``archived`` (once a check since the stop found the sandbox
down, so a stop that never finished is not cut short), ``gone`` (found
missing, at that check), or ``error``. NULL while the run is open or
the sandbox still sits stopped. A hold ends at the earliest of these, so a
sandbox restarted after it archived held its disk only until the archive.
``archive_after_s`` is the sandbox's own archive interval, from the
provider: Daytona archives a stopped sandbox when it runs out. NULL where
the sandbox is never archived (E2B's paused sandboxes, Daytona VMs), whose
hold ends only on a restart or a delete; an E2B hold is pause time. The
sweep re-checks a still-held sandbox hourly (``allocation_checked_at``),
which bounds a hold no event ended (a sandbox deleted by a path the ledger
does not hear from) at an hour over.

A run's end is the stop the server recorded, which a stop the provider
refused or never finished puts too early: Daytona has left a sandbox
stopping for an hour after a stop that timed out, billed as running all the
while. The sweep confirms each stop with the provider within a
cycle and, while the sandbox is still up, moves the run's end to its check.
``seen_down_at`` is when an answer last found the sandbox down or gone,
as of when its lookup started: a stop before it is confirmed, so no later
answer finding the sandbox up moves that run's end, and only then does
the archive end its hold.

Backfilled here only where the ledger already knows: a run that ended on a
replacement held nothing after, and a Docker run's size is the tier's for
good, the sweep never asking the host. A delete's hold ends at the
computer's delete, which may have come long after a stop whose own close was
lost, so it waits with every other hold for its sandbox's archive interval,
which only the provider knows, and is settled by the sweep once it has asked.

New nullable columns and two indexes on a table of a few thousand rows,
under the same 5 s lock limit as 088. The previous build never names these
columns, so it runs against them untouched.

Rollback: ``alembic downgrade 121``. It drops the columns and their history;
the corrected sizes stay in ``cpu``/``memory_gb``/``disk_gb``.

Example: what each user's sandboxes held per month, in vCPU-hours, memory
GiB-hours and disk GiB-hours, running and stopped (``date_trunc`` follows the
session's TimeZone; a span crossing a month is split)::

    WITH spans AS (
        SELECT user_id, provider_kind, cpu, memory_gb, disk_gb,
               started_at AS s, coalesce(ended_at, now()) AS e, true AS running
        FROM computer_runs
        UNION ALL
        SELECT user_id, provider_kind, cpu, memory_gb, disk_gb,
               ended_at, coalesce(held_until, now()), false
        FROM computer_runs WHERE ended_at IS NOT NULL
    ), split AS (
        SELECT sp.*, m, extract(epoch FROM least(e, m + interval '1 month')
                                    - greatest(s, m)) / 3600 AS h
        FROM spans sp
        CROSS JOIN LATERAL generate_series(
            date_trunc('month', s), date_trunc('month', e), interval '1 month') m
    )
    SELECT user_id, provider_kind, m AS month,
           round(sum(h * cpu) FILTER (WHERE running)::numeric, 2) AS vcpu_hours,
           round(sum(h * memory_gb) FILTER (WHERE running)::numeric, 2) AS mem_gib_hours,
           round(sum(h * disk_gb) FILTER (WHERE running)::numeric, 2) AS disk_gib_hours_running,
           round(sum(h * disk_gb) FILTER (WHERE NOT running)::numeric, 2) AS disk_gib_hours_stopped
    FROM split WHERE h > 0
    GROUP BY 1, 2, 3 ORDER BY month, vcpu_hours DESC NULLS LAST;
"""

from alembic import op

revision = "122"
down_revision = "121"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE computer_runs
            ADD COLUMN IF NOT EXISTS size_source VARCHAR(16),
            ADD COLUMN IF NOT EXISTS archive_after_s INTEGER,
            ADD COLUMN IF NOT EXISTS held_until TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS held_end VARCHAR(16),
            ADD COLUMN IF NOT EXISTS allocation_checked_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS seen_down_at TIMESTAMPTZ
    """)
    # A run's sandbox, newest run first: what an open copies a known size
    # from, and what ends the previous run's hold.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_runs_sandbox
        ON computer_runs (provider_kind, provider_sandbox_id, started_at DESC)
    """)
    # What the sweep asks the provider about: runs with no reported size, and
    # closed runs whose sandbox may still hold its disk.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_runs_allocation_due
        ON computer_runs (allocation_checked_at NULLS FIRST)
        WHERE provider_sandbox_id IS NOT NULL
          AND (size_source IS NULL OR size_source = 'partial'
               OR (ended_at IS NOT NULL AND held_until IS NULL))
    """)
    op.execute("""
        UPDATE computer_runs
        SET held_until = ended_at, held_end = 'deleted'
        WHERE ended_at IS NOT NULL AND held_until IS NULL
          AND end_reason = 'replaced'
    """)
    # The one metered kind the sweep never asks about: its own sizes stand.
    op.execute("""
        UPDATE computer_runs SET size_source = 'tier'
        WHERE provider_kind = 'docker' AND size_source IS NULL
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP INDEX IF EXISTS idx_computer_runs_allocation_due")
    op.execute("DROP INDEX IF EXISTS idx_computer_runs_sandbox")
    op.execute("""
        ALTER TABLE computer_runs
            DROP COLUMN IF EXISTS seen_down_at,
            DROP COLUMN IF EXISTS allocation_checked_at,
            DROP COLUMN IF EXISTS held_end,
            DROP COLUMN IF EXISTS held_until,
            DROP COLUMN IF EXISTS archive_after_s,
            DROP COLUMN IF EXISTS size_source
    """)
