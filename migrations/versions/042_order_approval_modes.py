"""One approval switch per order mode, in place of the single boolean.

An order tool is pinned to the direct path at every brokerage now, and the
three modes it can run in do not want the same answer: a live order and an
IBKR staged instruction stop for the user by default, a simulated account does
not. The column stores only the modes a user set and every reader fills in
the rest from ``ORDER_APPROVAL_DEFAULTS``, so a later change to a default
reaches each row that never set that mode. The old boolean defaulted to true,
so only a row that turned live approval off carries an answer forward.

Revision ID: 042
Revises: 041
"""

from alembic import op


revision = "042"
down_revision = "041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE user_mcp_servers
          ALTER COLUMN order_approval DROP DEFAULT;
        ALTER TABLE user_mcp_servers
          ALTER COLUMN order_approval TYPE JSONB
          USING CASE
              WHEN order_approval IS FALSE THEN '{"live": false}'::jsonb
              ELSE '{}'::jsonb
          END;
        ALTER TABLE user_mcp_servers
          ALTER COLUMN order_approval SET DEFAULT '{}'::jsonb;
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE user_mcp_servers
          ALTER COLUMN order_approval DROP DEFAULT;
        ALTER TABLE user_mcp_servers
          ALTER COLUMN order_approval TYPE BOOLEAN
          USING COALESCE((order_approval ->> 'live')::boolean, TRUE);
        ALTER TABLE user_mcp_servers
          ALTER COLUMN order_approval SET DEFAULT TRUE;
    """)
