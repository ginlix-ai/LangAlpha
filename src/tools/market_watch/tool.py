"""Agent tools to register/unregister tickers for live market watch."""

import logging
from typing import List, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from src.utils.market_watch import add_symbols, remove_symbols

logger = logging.getLogger(__name__)


def _thread_id(config: RunnableConfig) -> Optional[str]:
    return (config.get("configurable") or {}).get("thread_id")


@tool
async def watch_market(symbols: List[str], config: RunnableConfig) -> str:
    """
    Start watching tickers for live price updates during this conversation.

    Use for intraday-sensitive tasks (fast-moving stock, trading decision,
    live market event). Watched symbols get automatic `<market-watch>` price
    updates before/inside your turns — you do NOT need to poll get_quote for them.

    Args:
        symbols: Ticker symbols to watch (e.g. ["NVDA", "TSLA"]).
    """
    thread_id = _thread_id(config)
    if not thread_id:
        return "Market watch unavailable: no thread context."
    watched = await add_symbols(thread_id, symbols)
    if not watched:
        return "Market watch unavailable (cache offline)."
    return (
        f"Now watching: {', '.join(watched)}. Live prices will appear "
        "automatically as <market-watch> updates before/inside your turns."
    )


@tool
async def unwatch_market(
    config: RunnableConfig, symbols: Optional[List[str]] = None
) -> str:
    """
    Stop watching tickers (all of them if symbols is omitted).

    Args:
        symbols: Tickers to stop watching; omit to clear the whole watch list.
    """
    thread_id = _thread_id(config)
    if not thread_id:
        return "Market watch unavailable: no thread context."
    remaining = await remove_symbols(thread_id, symbols)
    if remaining:
        return f"Stopped. Still watching: {', '.join(remaining)}."
    return "Stopped watching all symbols."
