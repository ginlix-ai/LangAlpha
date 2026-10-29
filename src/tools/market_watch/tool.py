"""Agent tool to register/unregister tickers for live market watch.

Price alerts are wakeups now (``src/tools/wakeups``): a watch only shows
prices while the agent works.
"""

from typing import List, Literal, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from src.server.services.market_targets.listing import target_listing
from src.utils.market_watch import add_symbols, clean_symbols, get_watchlist, remove_symbols

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


def _rejected(requested: Optional[List[str]], watched: List[str]) -> List[str]:
    """Requested symbols that didn't land in the watch list (bad grammar or
    over the max-symbols cap) — echoed back so a drop can't read as success.
    Tokens are bounded to 20 chars; valid symbols (≤15) never truncate."""
    accepted = set(watched)
    seen: set = set()
    out: List[str] = []
    for raw in requested or []:
        if not isinstance(raw, (str, int)):
            continue
        sym = str(raw).strip().upper()[:20]
        if sym and sym not in accepted and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def _spellings(symbols: List[str]) -> List[str]:
    """Every name a symbol's watch goes by: unwatching ``SPX`` must reach
    the ``^GSPC`` it was watched as, and ``^GSPC`` the ``SPX`` it was.

    A name that reads as another listing is left out: the Hang Seng is
    stored as ``HSI``, which on the watch list is the US ticker.
    """
    out: List[str] = []
    for symbol in clean_symbols(symbols):
        listing, _ = target_listing(symbol)
        names = (symbol,) if listing is None else tuple(
            n for n in (symbol, listing.key, listing.watch)
            if target_listing(n)[0] == listing
        )
        out.extend(n for n in names if n not in out)
    return out


async def _status(thread_id: str) -> str:
    watched = await get_watchlist(thread_id)
    return f"Watching: {', '.join(watched)}." if watched else "Not watching any symbols."


@tool
async def watch_market(
    config: RunnableConfig,
    symbols: Optional[List[str]] = None,
    action: Literal["watch", "unwatch"] = "watch",
) -> str:
    """Watch tickers for live prices during this conversation.

    Watch for intraday-sensitive tasks (fast-moving stock, trading decision,
    live market event). Call with no symbols to see what is watched now.

    Args:
        symbols: Ticker symbols (e.g. ["NVDA", "TSLA"]) to watch. With
            action="unwatch", the symbols to stop; omit to stop all.
        action: "watch" (default) or "unwatch".

    Returns:
        A confirmation, not prices. Watched symbols then arrive on their own as
        `<market-watch>` updates before and inside your turns, so manual
        get_quote pulls are usually unnecessary for them.
    """
    thread_id = _thread_id(config)
    if not thread_id:
        return "Market watch unavailable: no thread context."

    if action == "unwatch":
        remaining = await remove_symbols(thread_id, _spellings(symbols) if symbols else None)
        if remaining is None:
            return "Market watch unavailable (cache offline)."
        if remaining:
            return f"Stopped. Still watching: {', '.join(remaining)}."
        return "Stopped watching all symbols."

    if not symbols:
        return await _status(thread_id)

    watched = await add_symbols(thread_id, symbols)
    if watched is None:
        return "Market watch unavailable (cache offline)."
    if not watched:
        return "No valid ticker symbols provided."
    line = f"Now watching: {', '.join(watched)}."
    rejected = _rejected(symbols, watched)
    if rejected:
        line += f" Not added (invalid symbol or watch list full): {', '.join(rejected)}."
    return f"{line}\n\n{_FEED_INSTRUCTIONS}"
