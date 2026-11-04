# pyright: ignore
"""Market intel snapshots: options chains, market movers, and live quotes."""

from typing import Optional, Dict, Any, List, Tuple
from datetime import date, datetime, timedelta, timezone
import logging

from langchain_core.runnables import RunnableConfig

from .currency import fmt_price, fmt_quote
from .display import (
    _symbol_currency,
    resolve_listing,
    resolve_ref,
    venue_phase,
)
from .quote_format import (
    block_clock,
    format_quote_block,
    stamp_quote,
)
from src.data_client import fallbacks, get_financial_data_provider
from src.data_client.base import MoverRow
from src.data_client.freshness import is_live
from src.data_client.ginlix_data.directory import fill_quote_names
from src.data_client.ginlix_data.pagination import follow_cursor
from src.data_client.market_data_provider import snapshot_key
from src.observability import stamp_tool_provider
from src.server.services.cache.quote_cache_service import QuoteCacheService
from src.utils.market_hours import ET
from market_protocol import AssetClass, InstrumentRef, MarketPhase, display_spelling, to_legacy_api

from ._shared import _get_user_id, _mark_failure, _mark_no_data

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
            _mark_failure("not_configured")
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

        results, truncated = await follow_cursor(_fetch_page, filters, limit=limit)

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

        retrieved_at = datetime.now(timezone.utc)
        timestamp = retrieved_at.strftime("%Y-%m-%d %H:%M UTC")
        lines: List[str] = []
        header = f"## Options Chain: {underlying} ({len(results)} contracts)"
        if market_status:
            header += f" | Market: {market_status}"
        lines.append(header)
        lines.append(f"**Retrieved:** {timestamp}")
        if contract_type:
            lines.append(f"**Type:** {contract_type.upper()}")
        if truncated:
            lines.append(
                f"**More contracts match than the {len(results)} shown.** "
                "Raise limit or narrow the expiry or strike filters to see the rest."
            )
        lines.append("")

        # Each contract carries its snapshot's session (close, change,
        # volume), which the table below and the card both print.
        contracts = [
            {**c, "session": snapshot_map.get(c.get("ticker"), {}).get("session") or None}
            for c in results
        ]
        if not contracts and not truncated:
            lines.append("No contracts found matching the given criteria.")
        elif contracts:
            lines.append("| Ticker | Type | Strike | Expiry | Close | Chg% | Volume |")
            lines.append("|--------|------|--------|--------|-------|------|--------|")
            for c in contracts:
                ticker = c.get("ticker", "N/A")
                ctype = c.get("contract_type", "N/A")
                strike = c.get("strike_price")
                strike_str = fmt_price(strike, cur)
                expiry = c.get("expiration_date", "N/A")

                session = c["session"] or {}
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
            "results": contracts,
            "underlying": underlying,
            "contract_type": contract_type,
            "market_status": market_status,
            "truncated": truncated,
            "currency": cur.currency,
            "as_of_ts": int(retrieved_at.timestamp() * 1000),
        }
        return content, artifact

    except Exception as e:
        logger.error(f"Error fetching options chain for {underlying}: {e}")
        _mark_failure(e)
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



def _movers_error(message: str) -> Tuple[str, Dict[str, Any]]:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    err = (
        f"## Market Movers\n"
        f"**Retrieved:** {timestamp}\n"
        f"**Status:** Error\n\nError: {message}"
    )
    return err, {
        "type": "market_movers", "results": [],
        "error": message,
    }


# What "significant volume" means for a mover: the money traded in its session.
_MIN_TRADED_VALUE = 100_000

# How long after the bell the closing auction's print can report.
_CLOSING_CROSS_REPORT = timedelta(seconds=10)


class MoverCardRow(MoverRow):
    """A mover as the text and the card read it. The name and the currency are
    the tool's to add: the source names no mover, and a price is in its
    listing's currency."""

    name: Optional[str]
    currency: Optional[str]


def _trade_session(ref: Optional[InstrumentRef], at: Optional[datetime], day: date) -> str:
    """A mover's last trade time (ET), dated when it is not *day*'s, and the
    session it printed in.

    A print in the seconds after the close is the closing auction's, which
    crosses at the close and reports a moment later.
    """
    if at is None:
        return "N/A"
    when = f"{at:%H:%M}" if at.date() == day else f"{at:%Y-%m-%d %H:%M}"
    phase = venue_phase(ref, at)
    if phase is MarketPhase.POST and venue_phase(ref, at - _CLOSING_CROSS_REPORT) is MarketPhase.REGULAR:
        phase = MarketPhase.REGULAR
    if phase is MarketPhase.REGULAR:
        return f"{when} Regular Hours"
    if phase in (MarketPhase.PRE, MarketPhase.POST):
        return f"{when} Extended Hours"
    return when


async def fetch_market_movers(
    direction: str = "gainers",
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Fetch top market movers (gainers or losers).

    Returns:
        Tuple of (markdown content, artifact dict)
    """
    # The agent's mistake, so refused here unmarked: the data API would answer
    # it with a 422 that alarms as our failure and shows the model its URL.
    if direction not in ("gainers", "losers"):
        return _movers_error(
            f"direction must be 'gainers' or 'losers', got {direction!r}"
        )
    try:
        provider = await get_financial_data_provider()
        user_id = _get_user_id(config)
        if provider.intel is None:
            _mark_failure("not_configured")
            return (
                "Market movers data is not available"
                " (no MarketIntelSource configured).",
                {"type": "market_movers", "results": []},
            )

        listed = await provider.intel.get_movers(direction, user_id=user_id)
        # A source's own volume floor counts shares, which a sub-cent warrant
        # clears with a few hundred dollars traded.
        movers: List[MoverRow] = []
        excluded: List[str] = []
        for m in listed:
            if m["traded_value"] is not None and m["traded_value"] < _MIN_TRADED_VALUE:
                excluded.append(m["symbol"])
            else:
                movers.append(m)
        traded = {m["symbol"]: datetime.fromtimestamp(m["traded_at"] / 1000, ET) for m in listed if m["traded_at"]}
        newest = max(traded.values()).date() if traded else None

        def shared_day(symbols: List[str]) -> str:
            """' on <date>' when every one of *symbols* last traded that ET day."""
            days = {traded[s].date() if s in traded else None for s in symbols}
            return f" on {days.pop():%Y-%m-%d}" if len(days) == 1 and None not in days else ""

        on_day = shared_day([m["symbol"] for m in movers])
        excluded_day = shared_day(excluded)
        refs = [resolve_ref(m["symbol"]) for m in movers]
        rows: List[MoverCardRow] = [
            {**m, "name": None, "currency": ref.price_currency if ref is not None else None}
            for m, ref in zip(movers, refs)
        ]
        await fill_quote_names(rows)

        retrieved_at = datetime.now(timezone.utc)
        timestamp = retrieved_at.strftime("%Y-%m-%d %H:%M UTC")
        label, noun = ("Gainers", "gainer") if direction == "gainers" else ("Losers", "loser")
        lines: List[str] = []
        lines.append(f"## Market {label} ({len(rows)} stocks)")
        lines.append(f"**Retrieved:** {timestamp}")
        if rows and any(m["symbol"] in traded for m in movers):
            untimed = "" if all(m["symbol"] in traded for m in movers) else ", or its day close where Last Trade is N/A"
            lines.append(
                f"**Prices:** each stock's latest trade{on_day}"
                f" (ET){untimed}; Change% is from the previous close"
            )
        if excluded:
            lines.append(
                f"**Excluded (under ${_MIN_TRADED_VALUE:,} traded{excluded_day}):** {', '.join(excluded)}"
            )
        lines.append("")

        if not rows:
            lines.append(
                f"No {noun} traded ${_MIN_TRADED_VALUE:,} or more{excluded_day}." if excluded
                else f"No {direction} data available."
            )
        else:
            # Names come only for listings the directory knows (CN, HK); a column
            # of N/A invites the reader to supply names from memory.
            named = any(row["name"] for row in rows)
            header = ["#", "Symbol", *(["Name"] if named else []), "Price", "Change%", "Last Trade (ET)"]
            lines.append(f"| {' | '.join(header)} |")
            lines.append(f"|{'|'.join('-' * (len(h) + 2) for h in header)}|")
            for i, (row, ref) in enumerate(zip(rows, refs), 1):
                sym = row["symbol"]
                name = row["name"] or "N/A"
                if len(name) > 30:
                    name = name[:27] + "..."
                price_str = fmt_quote(row["price"], _symbol_currency(ref))
                change_pct = row["change_percent"]
                if change_pct is not None:
                    change_str = f"{change_pct:+.2f}%"
                else:
                    change_str = "N/A"
                trade_str = _trade_session(ref, traded.get(sym), newest)
                cells = [str(i), sym, *([name] if named else []), price_str, change_str, trade_str]
                lines.append(f"| {' | '.join(cells)} |")

        content = "\n".join(lines)
        artifact = {
            "type": "market_movers",
            "direction": direction,
            "results": rows,
            "as_of_ts": int(retrieved_at.timestamp() * 1000),
        }
        return content, artifact

    except Exception as e:
        logger.error(f"Error fetching market movers ({direction}): {e}")
        _mark_failure(e)
        return _movers_error(str(e))


async def fetch_quote(
    symbols: List[str],
    asset_type: str = "stocks",
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Fetch real-time quotes for up to 20 symbols through the shared quote cache."""
    empty = {"type": "quote", "quotes": []}
    # LLM-supplied value that reaches the provider request path — restrict to
    # the two documented values so it can't smuggle path segments.
    if asset_type not in ("stocks", "indices"):
        asset_type = "stocks"
    asset_class = AssetClass.INDEX if asset_type == "indices" else AssetClass.EQUITY
    # Two spellings of one listing (BRK.B, BRK-B) ask for it once.
    syms = list(dict.fromkeys(display_spelling(s) for s in (symbols or []) if s and s.strip()))[:20]
    if not syms:
        return "No symbols provided.", empty
    try:
        # The class decides a bare alias: on the stocks path COMP is Compass,
        # not the Nasdaq Composite. A spelling only an index has (^GSPC, I:SPX,
        # SPX.INDEX) resolves as the index on either path, and the cache asks
        # each instrument on the endpoint of the class it resolved to.
        refs = {s: resolve_ref(s, asset_class) for s in syms}
        quotes, failure = await QuoteCacheService.get_instance().fetch_quotes(
            [ref for ref in refs.values() if ref is not None],
            _get_user_id(config),
            # A listing no provider quotes (Beijing has no realtime one) reads
            # its last daily close while its venue is closed, as the REST
            # quote routes do; empty, the agent goes to the web for it.
            daily_fallback=True,
            # Read from the chain, not a row another request cached, so the
            # Source fallbacks note names what this read's chain took.
            fresh=True,
        )
        for ref, row in quotes:
            if row.get("source") == "daily":
                fallbacks.answered(snapshot_key(to_legacy_api(ref)), "quote", "daily close")
        if not quotes:
            if failure is not None:
                raise failure.error
            _mark_no_data()
            return f"No quote data available for {', '.join(syms)}.", empty
        # Stamp each row with what it actually is: the declared tier, the
        # measured freshness, and a venue-local clock at the PRINT time when the
        # provider gave one (retrieval time otherwise) — the artifact's ET as_of
        # doesn't locate a foreign price in venue time, and a delayed print is
        # not located by the retrieval clock at all.
        retrieved_at = datetime.now(timezone.utc)
        all_realtime = True
        snaps: List[Dict[str, Any]] = []
        for ref, row in quotes:
            # A copy, since requests that shared one fetch share its rows. Each
            # row is measured on the clock of the listing it was asked as; a
            # stock read as an index has its after-hours print called closed.
            snap = {**row, "asset_class": ref.asset_class.value}
            snap.update(stamp_quote(snap, retrieved_at, ref=ref))
            # The card prints each row in its listing currency; an index level
            # is a bare number, so index rows carry none.
            if ref.asset_class is not AssetClass.INDEX and not snap.get("currency"):
                snap["currency"] = ref.price_currency
            all_realtime = all_realtime and is_live(snap["freshness"])
            snaps.append(snap)
        content = format_quote_block(snaps, at=retrieved_at)
        # A batch can be split across the chain: one stable name for the set.
        sources = {snap.get("source") for snap in snaps}
        stamp_tool_provider(
            ",".join(sorted(s for s in sources if isinstance(s, str) and s)) or None
        )
        # Report under the caller's input label. A failed request is not an
        # absent listing: the agent may retry it, and a listing whose sources
        # failed is named in the Source fallbacks note.
        key = QuoteCacheService.quote_key
        unreached = {key(ref) for ref in failure.refs} if failure else set()
        seen = unreached | {key(ref) for ref, _ in quotes}
        unsourced = {f.symbol for f in fallbacks.taken() if f.what == "quote" and f.served_by is None}
        missing = sorted(
            s for s, ref in refs.items()
            if ref is None or (key(ref) not in seen and snapshot_key(to_legacy_api(ref)) not in unsourced)
        )
        if missing:
            content += f"\n(no data: {', '.join(missing)})"
        if failure is not None:
            _mark_failure(failure.error)
            failed = sorted(s for s, ref in refs.items() if ref is not None and key(ref) in unreached)
            content += f"\n(request failed: {', '.join(failed)})"
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
        _mark_failure(e)
        return f"Error fetching quotes: {e}", empty
