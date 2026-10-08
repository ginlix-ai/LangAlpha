# pyright: ignore
"""Market intel snapshots: options chains, market movers, and live quotes."""

from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone
import logging

from langchain_core.runnables import RunnableConfig

from .currency import fmt_price
from .display import (
    _symbol_currency,
    resolve_listing,
    resolve_ref,
)
from .quote_format import (
    block_clock,
    format_quote_block,
    stamp_quote,
)
from src.data_client import get_financial_data_provider, get_market_data_provider
from src.data_client.freshness import is_live
from src.data_client.ginlix_data.directory import fill_quote_names
from src.data_client.market_data_provider import snapshot_key
from src.data_client.ginlix_data.pagination import paginate_cursor
from market_protocol import AssetClass, display_spelling

from ._shared import _get_user_id

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Market intel tools (options, short data, movers)
# ---------------------------------------------------------------------------


async def fetch_options_chain(
    underlying: str,
    contract_type: Optional[str] = None,
    expiration_date_gte: Optional[str] = None,
    expiration_date_lte: Optional[str] = None,
    strike_min: Optional[float] = None,
    strike_max: Optional[float] = None,
    limit: int = 20,
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Fetch options chain with snapshot pricing for an underlying ticker.

    Returns:
        Tuple of (markdown content, artifact dict)
    """
    try:
        provider = await get_financial_data_provider()
        user_id = _get_user_id(config)
        # Resolve once: normalize the agent-supplied underlying to the legacy form
        # provider calls use, and reuse the ref for currency display.
        ref, underlying = resolve_listing(underlying)
        cur = _symbol_currency(ref)
        if provider.intel is None:
            return (
                "Options chain data is not available"
                " (no MarketIntelSource configured).",
                {"type": "options_chain", "results": []},
            )

        # Per-page size matches limit (API max 1000), paginate until we have enough
        page_size = min(limit, 1000)
        filters: Dict[str, Any] = {"limit": page_size}
        if contract_type:
            filters["contract_type"] = contract_type
        if expiration_date_gte:
            filters["expiration_date_gte"] = expiration_date_gte
        if expiration_date_lte:
            filters["expiration_date_lte"] = expiration_date_lte
        if strike_min is not None:
            filters["strike_price_gte"] = strike_min
        if strike_max is not None:
            filters["strike_price_lte"] = strike_max

        async def _fetch_page(p: Dict) -> Dict:
            return await provider.intel.get_options_chain(
                underlying, user_id=user_id, **p,
            )

        results = await paginate_cursor(_fetch_page, filters, limit=limit)

        # Batch-fetch snapshots for pricing data
        snapshot_map: Dict[str, Dict] = {}
        market_status = None
        if results:
            tickers = [c.get("ticker") for c in results if c.get("ticker")]
            try:
                snapshots = await provider.intel.get_options_snapshot(
                    tickers, user_id=user_id,
                )
                for snap in snapshots:
                    snap_ticker = snap.get("ticker")
                    if snap_ticker:
                        snapshot_map[snap_ticker] = snap
                        if market_status is None:
                            market_status = snap.get("market_status")
            except Exception:
                logger.debug("Failed to fetch options snapshots", exc_info=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines: List[str] = []
        header = f"## Options Chain: {underlying} ({len(results)} contracts)"
        if market_status:
            header += f" | Market: {market_status}"
        lines.append(header)
        lines.append(f"**Retrieved:** {timestamp}")
        if contract_type:
            lines.append(f"**Type:** {contract_type.upper()}")
        lines.append("")

        if not results:
            lines.append("No contracts found matching the given criteria.")
        else:
            lines.append("| Ticker | Type | Strike | Expiry | Close | Chg% | Volume |")
            lines.append("|--------|------|--------|--------|-------|------|--------|")
            for c in results:
                ticker = c.get("ticker", "N/A")
                ctype = c.get("contract_type", "N/A")
                strike = c.get("strike_price")
                strike_str = fmt_price(strike, cur)
                expiry = c.get("expiration_date", "N/A")

                # Merge snapshot session data
                snap = snapshot_map.get(ticker, {})
                session = snap.get("session", {})
                close_val = session.get("close")
                close_str = fmt_price(close_val, cur) if close_val is not None else "—"
                chg_pct = session.get("change_percent")
                chg_str = f"{chg_pct:+.2f}%" if chg_pct is not None else "—"
                vol = session.get("volume")
                vol_str = f"{int(vol):,}" if vol is not None else "—"

                lines.append(
                    f"| {ticker} | {ctype} | {strike_str} | {expiry}"
                    f" | {close_str} | {chg_str} | {vol_str} |"
                )

        content = "\n".join(lines)
        artifact = {
            "type": "options_chain",
            "results": results,
            "underlying": underlying,
        }
        return content, artifact

    except Exception as e:
        logger.error(f"Error fetching options chain for {underlying}: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        err = (
            f"## Options Chain: {underlying}\n"
            f"**Retrieved:** {timestamp}\n"
            f"**Status:** Error\n\nError: {e}"
        )
        return err, {
            "type": "options_chain", "results": [],
            "error": str(e),
        }



async def fetch_market_movers(
    direction: str = "gainers",
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Fetch top market movers (gainers or losers).

    Returns:
        Tuple of (markdown content, artifact dict)
    """
    try:
        provider = await get_financial_data_provider()
        user_id = _get_user_id(config)
        if provider.intel is None:
            return (
                "Market movers data is not available"
                " (no MarketIntelSource configured).",
                {"type": "market_movers", "results": []},
            )

        results = await provider.intel.get_movers(direction, user_id=user_id)

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        label = "Gainers" if direction == "gainers" else "Losers"
        lines: List[str] = []
        lines.append(f"## Market {label} ({len(results)} stocks)")
        lines.append(f"**Retrieved:** {timestamp}")
        lines.append("")

        if not results:
            lines.append(f"No {direction} data available.")
        else:
            lines.append("| # | Symbol | Name | Price | Change% |")
            lines.append("|---|--------|------|-------|---------|")
            for i, stock in enumerate(results, 1):
                sym = stock.get("ticker", stock.get("symbol", "N/A"))
                name = stock.get("name", "N/A")
                if len(name) > 30:
                    name = name[:27] + "..."
                price = stock.get("price", stock.get("close"))
                price_str = fmt_price(price, _symbol_currency(resolve_ref(sym)))
                change_pct = stock.get("change_percent", stock.get("todaysChangePerc"))
                if change_pct is not None:
                    change_str = f"{change_pct:+.2f}%"
                else:
                    change_str = "N/A"
                lines.append(f"| {i} | {sym} | {name} | {price_str} | {change_str} |")

        content = "\n".join(lines)
        artifact = {"type": "market_movers", "direction": direction, "results": results}
        return content, artifact

    except Exception as e:
        logger.error(f"Error fetching market movers ({direction}): {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        err = (
            f"## Market Movers\n"
            f"**Retrieved:** {timestamp}\n"
            f"**Status:** Error\n\nError: {e}"
        )
        return err, {
            "type": "market_movers", "results": [],
            "error": str(e),
        }


async def fetch_quote(
    symbols: List[str],
    asset_type: str = "stocks",
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Fetch real-time quotes for up to 20 symbols via the snapshot provider chain."""
    empty = {"type": "quote", "quotes": []}
    # LLM-supplied value that reaches the provider request path — restrict to
    # the two documented values so it can't smuggle path segments.
    if asset_type not in ("stocks", "indices"):
        asset_type = "stocks"
    asset_class = AssetClass.INDEX if asset_type == "indices" else AssetClass.EQUITY
    syms = [display_spelling(s) for s in (symbols or []) if s and s.strip()][:20]
    if not syms:
        return "No symbols provided.", empty
    try:
        provider = await get_market_data_provider()
        user_id = _get_user_id(config)
        # Canonicalize each symbol to the legacy REST spelling the snapshot
        # provider chain expects (e.g. "0700.HK", index "^GSPC" -> "GSPC").
        # The class decides a bare alias: on the stocks path COMP is Compass,
        # not the Nasdaq Composite.
        listings = [resolve_listing(s, asset_class) for s in syms]
        resolved = [spelling for _, spelling in listings]
        asked = {snapshot_key(spelling): ref for ref, spelling in listings if ref is not None}
        snaps = await provider.get_snapshots(resolved, asset_type=asset_type, user_id=user_id)
        if asset_type == "stocks":
            # A listing no provider quotes (Beijing has no realtime one) reads
            # its last daily close while its venue is closed, as the REST
            # quote routes do; empty, the agent goes to the web for it.
            from src.server.services.cache.quote_daily_fallback import fill_from_daily

            served = [(asked[k], s) for s in snaps if (k := snapshot_key(s.get("symbol"))) in asked]
            filled = await fill_from_daily(served, list(asked.values()), user_id)
            snaps = [*snaps, *(row for _, row in filled[len(served):])]
        # The card names each row as the REST routes do; a daily row has none.
        await fill_quote_names(snaps)
        if not snaps:
            return f"No quote data available for {', '.join(syms)}.", empty
        # Stamp each row with what it actually is: the declared tier, the
        # measured freshness, and a venue-local clock at the PRINT time when the
        # provider gave one (retrieval time otherwise) — the artifact's ET as_of
        # doesn't locate a foreign price in venue time, and a delayed print is
        # not located by the retrieval clock at all.
        retrieved_at = datetime.now(timezone.utc)
        all_realtime = True
        for snap in snaps:
            # Each row is the listing it was asked as and is measured on that
            # listing's clock; the vendor's caret-free echo (GSPC) cannot tell
            # an index from a stock, and a stock read as an index has its
            # after-hours print called closed.
            ref = asked.get(snapshot_key(snap.get("symbol"))) or resolve_ref(
                snap.get("symbol"), asset_class
            )
            if ref is not None:
                snap["asset_class"] = ref.asset_class.value
            snap.update(stamp_quote(snap, retrieved_at, ref=ref))
            # The card prints each row in its listing currency; an index level
            # is a bare number, so index rows carry none.
            if asset_type == "stocks" and ref is not None and not snap.get("currency"):
                snap["currency"] = ref.price_currency
            all_realtime = all_realtime and is_live(snap["freshness"])
        content = format_quote_block(snaps, at=retrieved_at)
        # Diff on the RESOLVED spelling the provider actually saw (an alias like
        # "SPX" is requested as "GSPC"), but report under the caller's input
        # label, matched as the provider chain matches its rows.
        returned = {snapshot_key(s.get("symbol")) for s in snaps}
        missing = sorted(
            syms[i] for i, r in enumerate(resolved) if snapshot_key(r) not in returned
        )
        if missing:
            content += f"\n(no data: {', '.join(missing)})"
        return content, {
            "type": "quote",
            "quotes": snaps,
            "as_of": block_clock(snaps, retrieved_at),
            # Epoch ms so the frontend can offer the user-local time (tooltip)
            # without parsing the display strings.
            "as_of_ts": int(retrieved_at.timestamp() * 1000),
            # One flag for the card header: every quote in the artifact is
            # current, so the header may say so without reading each row.
            "all_realtime": all_realtime,
        }
    except Exception as e:
        logger.error(f"Error fetching quotes for {syms}: {e}")
        return f"Error fetching quotes: {e}", empty
