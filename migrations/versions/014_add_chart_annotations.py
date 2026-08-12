"""Add durable storage for agent-drawn chart annotations.

Each row is one annotation belonging to a chart instance. A chart instance is
identified by ``(workspace_id, chart_id)`` where ``chart_id`` is the disclosed
key ``"{SYMBOL}:{timeframe}"`` — so re-drawing with the same symbol+timeframe
edits the same chart, and a different ticker or timeframe is a new chart. The
``(workspace_id, symbol, timeframe)`` index serves the MarketView lookup that
renders the active workspace's chart.

Replaces the previous Redis-backed store (7-day TTL): annotations are a durable
workspace artifact set now, cascading away only when the workspace is deleted.

Revision ID: 014
Revises: 013
"""

from alembic import op


revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS chart_annotations (
            workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            chart_id VARCHAR(128) NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            timeframe VARCHAR(16) NOT NULL,
            annotation_id VARCHAR(64) NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (workspace_id, chart_id, annotation_id)
        )
    """)

    # MarketView renders the active workspace's (symbol, timeframe) chart; this
    # index serves that lookup and the per-instance list/clear operations.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_chart_annotations_view
        ON chart_annotations(workspace_id, symbol, timeframe)
    """)

    # update_updated_at_column() is defined in migration 001 (initial schema)
    # and reused here (as in 003 and 012); no redeclaration needed.
    op.execute("DROP TRIGGER IF EXISTS update_chart_annotations_updated_at ON chart_annotations")
    op.execute("""
        CREATE TRIGGER update_chart_annotations_updated_at
        BEFORE UPDATE ON chart_annotations
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS chart_annotations CASCADE")
