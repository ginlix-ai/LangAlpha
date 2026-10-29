"""Which listing a market-watch target is on, and the names it goes by.

A target is read by a price automation's rules, so it names what an alert
on the same symbol would: a caret spelling or a known index family (SPX,
GSPC) is the index, and any other bare spelling is the stock. It is stored
under the key the price monitor reads prices by (``^GSPC`` is ``SPX``) and
watched under the caret spelling the live feed reads as the same index, so
the feed, the stored row and the thread API all name one listing.
"""

from typing import Any, Mapping, NamedTuple, Optional, Tuple

from market_protocol import AssetClass, InstrumentRef, to_legacy_api

from src.data_client.ginlix_data.data_source import index_ticker
from src.server.models.automation import (
    MarketType,
    bare_alert_listing,
    is_pair_symbol,
    is_unsuffixed_code,
)
from src.server.services.price_monitor import watched_listing, watched_market
from src.tools.market_data.display import resolve_ref
from src.tools.wakeups.wording import (
    LISTING_NO_SUFFIX,
    LISTING_PAIR,
    LISTING_UNKNOWN_SUFFIX,
    LISTING_UNREADABLE as UNREADABLE,
)

# A % move from "now" is stored as from "armed", the moment it was set: the
# tool's vocabulary to the stored one, and back for the thread API.
PCT_FROM_STORED = {"previous_close": "previous_close", "now": "armed"}
PCT_FROM_TOOL = {stored: tool for tool, stored in PCT_FROM_STORED.items()}


class TargetListing(NamedTuple):
    key: str  # as stored and read by the price monitor
    market: MarketType
    watch: str  # the watch-list spelling of the same listing


def watch_spelling(key: str, market: MarketType) -> str:
    """The watch-list spelling of a stored target.

    The live feed reads a bare spelling as the stock, so an index is
    watched by its caret spelling.
    """
    if market is not MarketType.INDEX:
        return key
    ref = watched_listing(key, market)
    if ref is None:
        return key
    return "^" + to_legacy_api(ref).removeprefix("^")


def listing_ref(row: Mapping[str, Any]) -> Optional[InstrumentRef]:
    """The listing a stored row (a target, a price wakeup) watches; its market
    decides what a bare spelling names (``HSI`` on an index is the Hang Seng)."""
    asset_class = (
        AssetClass.INDEX if row.get("market") == "index" else AssetClass.EQUITY
    )
    return resolve_ref(row.get("symbol"), asset_class)


def shown_symbol(row: Mapping[str, Any]) -> Optional[str]:
    """A stored row's symbol as the tool, the thread API and the chip spell
    it, so an index is ``^GSPC`` everywhere rather than its stored ``SPX``."""
    symbol = row.get("symbol")
    if not symbol:
        return None
    try:
        return watch_spelling(symbol, MarketType(row.get("market") or "stock"))
    except ValueError:
        return symbol


def target_listing(symbol: str) -> Tuple[Optional[TargetListing], Optional[str]]:
    """The listing a target on a display-spelled *symbol* is on, or why the
    price monitor cannot watch it."""
    if is_pair_symbol(symbol):
        return None, LISTING_PAIR
    if is_unsuffixed_code(symbol):
        return None, LISTING_NO_SUFFIX
    if symbol.startswith("^"):
        key, market = index_ticker(symbol).removeprefix("I:"), MarketType.INDEX
    else:
        key, market = bare_alert_listing(symbol)
    ref = watched_listing(key, market)
    if ref is None or ref.asset_class in (AssetClass.FX, AssetClass.CRYPTO):
        return None, UNREADABLE
    watch = watch_spelling(key, market)
    if market is MarketType.INDEX:
        # The feed must read the watch spelling as this very index, or the
        # prices beside the target would be another listing's.
        seen = watched_listing(watch, MarketType.STOCK)
        if seen is None or (seen.index_family, seen.calendar_id) != (
            ref.index_family,
            ref.calendar_id,
        ):
            return None, UNREADABLE
    if watched_market(key, market) == "other":
        return None, LISTING_UNKNOWN_SUFFIX
    return TargetListing(key, market, watch), None
