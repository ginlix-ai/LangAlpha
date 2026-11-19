"""Migration 137: an organization's activity log.

One row per act by a member of an organization that changes who can see or
reach its work: a link shared or stopped, a connector, sign-in, key or secret
added or removed, a computer added or removed, the trading permission changed,
and every refusal the organization's or the deployment's policy made. Rows are
written only for an account the auth service places in an organization, and
another service pulls them by organization, oldest first, through
``GET /api/v1/activity``. The log is append-only and has no foreign keys, so
it outlives the threads, workspaces and computers it names.

A new table and its index on it, both empty: nothing existing is locked or
rewritten. The previous build never reads or writes the table, so this runs
before any process of the new build starts.

Rollback: roll the code back first, then ``alembic downgrade 136`` drops the
table and every event recorded in it.
"""

from alembic import op

revision = "137"
down_revision = "136"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS activity_events (
            event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            org_id TEXT NOT NULL,
            actor_id VARCHAR(255) NOT NULL,
            auth TEXT,
            client_id TEXT,
            action TEXT NOT NULL,
            outcome TEXT NOT NULL
                CONSTRAINT activity_events_outcome CHECK (outcome IN ('done', 'refused')),
            resource_kind TEXT,
            resource_id TEXT,
            workspace_id UUID,
            thread_id UUID,
            detail JSONB NOT NULL DEFAULT '{}'::jsonb,
            trace_id TEXT
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_activity_events_org
        ON activity_events (org_id, occurred_at, event_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS activity_events")
