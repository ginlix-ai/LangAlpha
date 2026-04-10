"""
LangChain tool for fetching portfolio holdings from Sharesight.

Lightweight tool suitable for Flash agent — no sandbox or MCP required.
"""

import logging

from langchain_core.tools import tool

from src.server.services.sharesight import sharesight_client

logger = logging.getLogger(__name__)


@tool
async def get_portfolio_holdings() -> str:
    """
    Get the user's portfolio holdings from Sharesight.

    Returns a summary of all holdings including symbol, name, quantity,
    current value, and gain/loss percentage.

    Use this tool when the user asks about their portfolio, holdings,
    positions, or investments.
    """
    if not sharesight_client.is_configured:
        return "Portfolio integration not configured. Sharesight credentials are missing."

    try:
        holdings = await sharesight_client.get_portfolio_holdings()
    except ValueError as exc:
        return f"Portfolio not found: {exc}"
    except RuntimeError as exc:
        return f"Unable to reach Sharesight: {exc}"
    except Exception:
        logger.exception("Sharesight API error in portfolio tool")
        return "Unable to fetch portfolio data. Please try again later."

    if not holdings:
        return "Portfolio is empty — no holdings found in Sharesight."

    lines = [f"Portfolio: {holdings[0].get('account_name', 'unknown')} ({len(holdings)} holdings)\n"]
    for h in holdings:
        symbol = h.get("symbol", "?")
        name = h.get("name", "")
        qty = h.get("quantity", 0)
        meta = h.get("metadata", {})
        value = meta.get("sharesight_value", 0)
        gain_pct = meta.get("capital_gain_pct")
        total_gain_pct = meta.get("total_gain_pct")

        gain_str = ""
        if total_gain_pct is not None:
            sign = "+" if total_gain_pct >= 0 else ""
            gain_str = f" ({sign}{total_gain_pct:.1f}%)"
        elif gain_pct is not None:
            sign = "+" if gain_pct >= 0 else ""
            gain_str = f" ({sign}{gain_pct:.1f}%)"

        lines.append(f"- {symbol} ({name}): {qty} shares, £{value:,.2f}{gain_str}")

    return "\n".join(lines)
