"""Migration 088: computer_runs, one row per interval a metered computer runs.

``computers`` holds only a machine's current status and its latest stop, so
how long sandboxes run, and how much of their size they use, was not recorded
anywhere a later question could read it. The providers' own metric history
does not last (Daytona keeps three days), so the server keeps it here.

A run opens when the computer enters ``running`` and closes when it leaves
``running`` and ``stopping`` both: the sandbox bills until the provider has
stopped it, and a stop that aborts goes back to ``running`` on the same run.
``provider_sandbox_id`` is the sandbox the run was on; a computer rebound to
another sandbox while running closes its run as ``replaced`` and opens one.
``resource_tier`` and the ``cpu``/``memory_gb``/``disk_gb`` it resolved to
are copied in at open, so a later change to the tier presets rewrites no
history. Nothing is backfilled: the first sweep after the deploy opens a run,
from that moment, for every computer already running. An SSH computer is the
owner's own machine and has no runs.

``end_reason`` is ``stopped`` (``paused`` on E2B, whose stop is a pause),
``idle`` when the idle sweep stopped it, ``error``, ``deleted``, ``replaced``,
or ``reconciled`` when the sweep found the run open on a computer that had
left ``running`` with no hook to close it. A stop stamps its reason on the
open run while the row reads ``stopping``, so ``end_reason`` can be set on a
run whose ``ended_at`` is still NULL.

The metric columns are folded in by the server's 300 s sweep from the
provider's history, all nullable, for Daytona and E2B only. ``sample_count``
counts provider samples (E2B samples every 5 s; Daytona returns aggregated
buckets). ``cpu_p95_pct`` is read off ``cpu_pct_histogram``, twenty 5-point
buckets of samples kept so the fold stays incremental: it is the upper edge
of the bucket holding the 95th percentile, so it overstates by under 5
points. ``mem_peak_excl_cache_bytes`` is used memory less page cache, which
is what an OOM kill is decided on. ``metrics_through`` is the last sample
folded in, the cursor the next fold reads from; ``metrics_polled_at`` is a
claim, so the sweep on one worker reads a run and the others skip it;
``metrics_final`` is set once the fold after the run closed has been tried,
and at open for a kind with no metrics. ``metrics_error`` is the last
failure, cleared by the next fold that succeeds. ``oom_kills`` counts
ExecuteCode and Bash results that exited 137, which is any SIGKILL, so it is
an upper bound.

Lock discipline follows 082: the foreign key holds SHARE ROW EXCLUSIVE on
``computers`` until the transaction commits, and every other statement is on
the new, empty table, so the transaction is short and the lock waits 5 s at
most. The previous build never reads or writes this table.

Rollback: ``alembic downgrade 087``. It drops the table and its history.

Example queries (``date_trunc`` follows the session's TimeZone):

Billed sandbox-hours per user, provider and tier per month, with a run
crossing a month boundary split between the two::

    SELECT r.user_id, r.provider_kind, r.resource_tier, m AS month,
           round(sum(extract(epoch FROM
               least(coalesce(r.ended_at, now()), m + interval '1 month')
               - greatest(r.started_at, m))) / 3600, 2) AS sandbox_hours
    FROM computer_runs r
    CROSS JOIN LATERAL generate_series(
        date_trunc('month', r.started_at),
        date_trunc('month', coalesce(r.ended_at, now())),
        interval '1 month') AS m
    GROUP BY 1, 2, 3, 4
    ORDER BY month, sandbox_hours DESC;

Peak memory and disk, as a share of the size the run had, per tier over the
last 30 days (``percentile_cont`` skips NULLs, so runs with no samples drop
out)::

    SELECT provider_kind, resource_tier, count(*) AS runs,
           percentile_cont(ARRAY[0.5, 0.9, 0.99]) WITHIN GROUP (ORDER BY
               mem_peak_excl_cache_bytes::float8 / nullif(mem_total_bytes, 0))
               AS mem_peak_share_p50_p90_p99,
           percentile_cont(ARRAY[0.5, 0.9, 0.99]) WITHIN GROUP (ORDER BY
               disk_peak_bytes::float8 / nullif(disk_total_bytes, 0))
               AS disk_peak_share_p50_p90_p99,
           percentile_cont(ARRAY[0.5, 0.9, 0.99]) WITHIN GROUP (ORDER BY
               cpu_p95_pct) AS cpu_p95_pct_p50_p90_p99,
           sum(oom_kills) AS sigkills
    FROM computer_runs
    WHERE started_at >= now() - interval '30 days' AND sample_count > 0
    GROUP BY 1, 2
    ORDER BY 1, 2;
"""

from alembic import op

revision = "088"
down_revision = "087"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS computer_runs (
            run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            computer_id UUID NOT NULL
                REFERENCES computers(computer_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            provider_kind VARCHAR(32) NOT NULL,
            provider_sandbox_id VARCHAR(255),
            resource_tier VARCHAR(32),
            cpu REAL,
            memory_gb REAL,
            disk_gb REAL,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ended_at TIMESTAMPTZ,
            end_reason VARCHAR(32),
            sample_count INTEGER,
            cpu_avg_pct REAL,
            cpu_p95_pct REAL,
            cpu_max_pct REAL,
            cpu_pct_histogram INTEGER[],
            mem_peak_bytes BIGINT,
            mem_peak_excl_cache_bytes BIGINT,
            mem_total_bytes BIGINT,
            disk_peak_bytes BIGINT,
            disk_total_bytes BIGINT,
            metrics_through TIMESTAMPTZ,
            metrics_error TEXT,
            metrics_polled_at TIMESTAMPTZ,
            metrics_final BOOLEAN NOT NULL DEFAULT FALSE,
            oom_kills INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT computer_runs_ended_after_start
                CHECK (ended_at IS NULL OR ended_at >= started_at)
        )
    """)
    # At most one open run per computer: what makes the open hook idempotent.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_computer_runs_one_open
        ON computer_runs (computer_id) WHERE ended_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_runs_user_started
        ON computer_runs (user_id, started_at)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_runs_computer_started
        ON computer_runs (computer_id, started_at)
    """)
    # A tool result names its sandbox, not its computer.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_runs_open_sandbox
        ON computer_runs (provider_kind, provider_sandbox_id) WHERE ended_at IS NULL
    """)
    # What the metrics sweep claims from: open runs, and closed ones awaiting
    # their last fold.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_runs_metrics_due
        ON computer_runs (metrics_polled_at NULLS FIRST) WHERE NOT metrics_final
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS computer_runs")
