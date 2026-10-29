"""Migration 095: give connected Robinhood rows the description that matches their tools.

A brokerage row is written with the shipped description when the user connects
it, and nothing rewrites it later, so every Robinhood row connected before now
names none of the tax lots, alerts, scans, Legend charts or trade approvals its
tools reach. Only a row still holding the exact shipped text is rewritten, so a
description the user edited stays theirs.

Pinned as literals rather than imported for the reason 093 gives: the shipped
text is free to change again, and a later change is that change's migration.

Revision ID: 095
Revises: 094
"""

from alembic import op

revision = "095"
down_revision = "094"
branch_labels = None
depends_on = None

_HOST = r"^https?://agent\.robinhood\.com(:[0-9]+)?([/?#]|$)"
_SHIPPED = (
    "Robinhood brokerage account: balances, positions, order history, "
    "instrument lookup, and order placement."
)
_CURRENT = (
    "Robinhood brokerage account: balances, positions, order history and tax "
    "lots, instrument lookup, price alerts, saved scans, Legend chart setups, "
    "trade approval status, and order placement."
)


def _rewrite(old: str, new: str) -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # The description reaches the agent through the composite, which is cached
    # per workspace against mcp_config_version, so the change is only seen once
    # each affected user's workspaces move on.
    op.execute(
        f"""
        WITH rewritten AS (
            UPDATE user_mcp_servers SET description = $d${new}$d$
             WHERE description = $d${old}$d$
               AND url ~* '{_HOST}'
            RETURNING user_id
        )
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.user_id IN (SELECT DISTINCT user_id FROM rewritten)
        """
    )


def upgrade() -> None:
    _rewrite(_SHIPPED, _CURRENT)


def downgrade() -> None:
    _rewrite(_CURRENT, _SHIPPED)
