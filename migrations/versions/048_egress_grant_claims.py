"""Migration 048: one claim per project on a machine-scoped grant.

047 keyed a grant on the machine and left ``workspace_id`` as the provenance of
whoever resolved it first. Two projects on one machine reuse one row, so that
column cannot say which projects still need it: deleting the first project
dropped the row from the machine's credential map and its retirement sweep
revoked it under the sibling. The claims table records membership; the runtime
retires a grant when its last live claim goes and builds the credential map
from live claims. The backfill gives every existing grant its provenance
project's claim, so a sibling that already reuses a row claims it on its next
sync, which is also when the map first offers it under that sibling alone.

Every statement is IF NOT EXISTS / ON CONFLICT DO NOTHING so a blocked branch
can be re-applied by hand.
"""

from alembic import op

revision = "048"
down_revision = "047"
branch_labels = None
depends_on = None

_TABLE = "sandbox_egress_grant_claims"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {_TABLE} (
            grant_id UUID NOT NULL
                REFERENCES sandbox_egress_grants(grant_id) ON DELETE CASCADE,
            workspace_id UUID NOT NULL
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (grant_id, workspace_id)
        )
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_egress_grant_claims_workspace
        ON {_TABLE} (workspace_id)
    """)
    # Provenance becomes the first claim. Revoked rows are included: a relay
    # JWT may still name them, and a claim on a revoked grant spares nothing.
    op.execute(f"""
        INSERT INTO {_TABLE} (grant_id, workspace_id)
        SELECT g.grant_id, g.workspace_id
        FROM sandbox_egress_grants g
        JOIN workspaces w ON w.workspace_id = g.workspace_id
        ON CONFLICT DO NOTHING
    """)


def downgrade() -> None:
    """The grant rows are untouched; 047's provenance column still reads."""
    op.execute(f"DROP TABLE IF EXISTS {_TABLE}")
