"""Migration 047: enforce machine-scoped grant uniqueness after 046.

NULLS NOT DISTINCT, as in 025/045, covers nullable connection_id/server_name;
server_name distinguishes header_mcp grants with NULL connection_id.
Exclude NULL computer_id so flash and 046 shared-sandbox losers retain 045's
sandbox_egress_grants_scope_key, UNIQUE NULLS NOT DISTINCT
(workspace_id, kind, connection_id, server_name), without colliding.
Build before consolidation, while computer keys are unique; dedupe under EG,
keeping the active then latest updated_at survivor and revoking rather than
deleting losers whose grant_id may remain in relay JWTs.
Phase 4 drops the workspace constraint, sandbox_id and other shadows, plus
046's redundant idx_sandbox_egress_grants_computer, when relay authority moves
to computers.
"""

from alembic import op

revision = "047"
down_revision = "046"
branch_labels = None
depends_on = None

_COMPUTER_GRANT_INDEX = "idx_sandbox_egress_grants_computer_conn"


def upgrade() -> None:
    # Bound SHARE-lock waits so a grant sync cannot stall relay traffic during deployment.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {_COMPUTER_GRANT_INDEX}
        ON sandbox_egress_grants (computer_id, kind, connection_id, server_name)
        NULLS NOT DISTINCT
        WHERE computer_id IS NOT NULL
    """)


def downgrade() -> None:
    """No rows changed; 045's workspace constraint remains intact for rollback."""
    op.execute(f"DROP INDEX IF EXISTS {_COMPUTER_GRANT_INDEX}")
