"""Migration 093: give connected brokerage rows the description that matches their tools.

A brokerage row is written with the shipped description when the user connects
it, and nothing rewrites it later. Every IBKR row connected before its live
order tools were curated still offers only draft orders, every Webull row
still says that nothing there places, previews or stages an order, and every
moomoo row names no crypto orders, while those tools are offered beside the
sentence. Only a row still holding the exact shipped text is rewritten, so a
description the user edited stays theirs.

Pinned as literals rather than imported for the reason 030 gives: the shipped
text is free to change again, and a later change is that change's migration.

Revision ID: 093
Revises: 092
"""

from alembic import op

revision = "093"
down_revision = "092"
branch_labels = None
depends_on = None

# (host, shipped text, current text), one per brokerage whose text changed.
_REWRITES: tuple[tuple[str, str, str], ...] = (
    (
        r"^https?://api\.ibkr\.com(:[0-9]+)?([/?#]|$)",
        (
            "Interactive Brokers account: portfolio, positions, account "
            "performance, market data, and draft orders the user confirms in "
            "IBKR before anything is placed."
        ),
        (
            "Interactive Brokers account: portfolio, positions, account "
            "performance, market data, saved screeners, live stock and "
            "single-leg option orders, and draft orders the user confirms in "
            "IBKR before anything is placed."
        ),
    ),
    (
        r"^https?://api\.webull\.com(:[0-9]+)?([/?#]|$)",
        (
            "Webull brokerage account: balances, positions and buying power, "
            "order history, watchlists, real-time quotes, and instrument "
            "reference data. Reads only: nothing here places, previews or "
            "stages an order. Covers equities, options, futures, event "
            "contracts and crypto."
        ),
        (
            "Webull brokerage account: balances, positions and buying power, "
            "order history, watchlists, real-time quotes, instrument reference "
            "data, fundamentals, news, and insider and institutional holdings, "
            "and draft orders for stocks, options, futures and crypto that the "
            "user confirms in the Webull app before anything is placed. Covers "
            "equities, options, futures, event contracts and crypto."
        ),
    ),
    (
        r"^https?://mcp\.moomoo\.com(:[0-9]+)?([/?#]|$)",
        (
            "moomoo brokerage account: balances, positions, and order history, "
            "real-time quotes and order book, option chains and volatility, "
            "company fundamentals and analyst research, stock and IPO "
            "screeners, and order placement. Covers US, Greater China, Japan "
            "and Southeast Asia."
        ),
        (
            "moomoo brokerage account: balances, positions, and order history, "
            "real-time quotes and order book, option chains and volatility, "
            "company fundamentals and analyst research, stock and IPO "
            "screeners, and order placement for stocks, options, futures and "
            "crypto. Covers US, Greater China, Japan and Southeast Asia."
        ),
    ),
)


def _rewrite(host: str, old: str, new: str) -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # The description reaches the agent through the composite, which is cached
    # per workspace against mcp_config_version, so the change is only seen once
    # each affected user's workspaces move on.
    op.execute(
        f"""
        WITH rewritten AS (
            UPDATE user_mcp_servers SET description = $d${new}$d$
             WHERE description = $d${old}$d$
               AND url ~* '{host}'
            RETURNING user_id
        )
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.user_id IN (SELECT DISTINCT user_id FROM rewritten)
        """
    )


def upgrade() -> None:
    for host, old, new in _REWRITES:
        _rewrite(host, old, new)


def downgrade() -> None:
    for host, old, new in reversed(_REWRITES):
        _rewrite(host, new, old)
