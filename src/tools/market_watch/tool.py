"""Agent tool to register/unregister tickers for live market watch."""

import logging
from typing import List, Literal, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from src.utils.market_watch import add_symbols, remove_symbols

logger = logging.getLogger(__name__)

# Returned with every successful watch: the reciprocal action (how to stop).
# Feed mechanics — newest block = current price, throttling, venue hours — live
# in the market-watch skill, not here, so the result stays action-specific.
_FEED_INSTRUCTIONS = (
    "Live prices now arrive automatically as <market-watch> blocks. When live "
    'tracking no longer matters, call watch_market with action="unwatch" (omit '
    "symbols to stop watching everything)."
)


def _thread_id(config: RunnableConfig) -> Optional[str]:
    return (config.get("configurable") or {}).get("thread_id")


@tool
async def watch_market(
    config: RunnableConfig,
    symbols: Optional[List[str]] = None,
    action: Literal["watch", "unwatch"] = "watch",
) -> str:
    """
    Start or stop watching tickers for live price updates during this conversation.

    Use for intraday-sensitive tasks (fast-moving stock, trading decision,
    live market event). Watched symbols get automatic `<market-watch>` price
    updates before/inside your turns, so manual get_quote pulls are usually
    unnecessary for them.

    Args:
        symbols: Ticker symbols (e.g. ["NVDA", "TSLA"]). Required to watch;
            with action="unwatch", omit to stop watching everything.
        action: "watch" (default) to start watching, "unwatch" to stop.
    """
    thread_id = _thread_id(config)
    if not thread_id:
        return "Market watch unavailable: no thread context."
    if action == "unwatch":
        remaining = await remove_symbols(thread_id, symbols or None)
        if remaining is None:
            return "Market watch unavailable (cache offline)."
        if remaining:
            return f"Stopped. Still watching: {', '.join(remaining)}."
        return "Stopped watching all symbols."
    if not symbols:
        return "Provide symbols to watch."
    watched = await add_symbols(thread_id, symbols)
    if watched is None:
        return "Market watch unavailable (cache offline)."
    if not watched:
        return "No valid ticker symbols provided."
    return f"Now watching: {', '.join(watched)}.\n\n{_FEED_INSTRUCTIONS}"
