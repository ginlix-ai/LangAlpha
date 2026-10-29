"""Record on each grant which tools are bound directly to the model.

A tool bound directly gets no sandbox wrapper, so nothing in generated code
calls it. That is a property of the composite, and a composite is not a gate:
a sandbox can hand-write the JSON-RPC frame and put it to the relay under the
same grant. The policy middleware that gates a direct call runs only in the
backend, so the relay has to refuse that frame from a sandbox caller itself,
and this column is what it reads to know which tools to refuse.

NULL means no tool is bound directly, which is every grant at the moment this
runs. Bumping the config version below makes the next resolve compute the set
for every workspace with a connection, the same way 038 had the denial
recomputed. Until then a sandbox could reach a direct-bound tool by hand for
the length of one resolve; the wrapper it would normally import is already
gone from the composite, so the window needs a deliberate frame to exploit.

Revision ID: 040
Revises: 039
"""

from alembic import op


revision = "040"
down_revision = "039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          ADD COLUMN tool_direct_only JSONB
    """)
    op.execute("""
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.user_id IN (
             SELECT DISTINCT user_id FROM user_mcp_oauth_connections
         )
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          DROP COLUMN IF EXISTS tool_direct_only
    """)
