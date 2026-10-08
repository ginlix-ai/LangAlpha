# pyright: ignore
"""Daily OHLCV price history: fetch_daily_prices and its table/summary formatting."""

from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone
import contextlib
import logging
import asyncio

from langchain_core.runnables import RunnableConfig

from .currency import fmt_count, fmt_price
from .display import (
    _market_label,
    _symbol_currency,
    resolve_listing,
    resolve_ref,
    venue_local_time,
)
from .quote_format import FRESHNESS_WORDS, build_live_stamp
from .utils import finite_or_none, format_percentage
from src.data_client import get_market_data_provider
from src.data_client.freshness import Freshness, FreshnessLabel, measure_bars, measure_daily
from src.data_client.normalize import series_lineage
from src.data_client.ginlix_data.directory import display_names
from market_protocol import AssetClass, PriceTreatment

from ._shared import _get_user_id, _normalize_market_bars

logger = logging.getLogger(__name__)


def _format_price_data_as_table(data: List[Dict[str, Any]]) -> str:
    """
    Format OHLCV price data as a markdown table.

    Args:
        data: List of daily OHLCV dictionaries (newest first)

    Returns:
        Markdown-formatted table string
    """
    if not data or len(data) == 0:
        return "No price data available."

    symbol = data[0].get("symbol", "N/A")
    cur = _symbol_currency(resolve_ref(symbol))
    num_days = len(data)

    # Get date range
    dates = [d.get("date") for d in data if d.get("date")]
    if dates:
        sorted_dates = sorted(dates)
        start_date = sorted_dates[0]
        end_date = sorted_dates[-1]
    else:
        start_date = end_date = "N/A"

    lines = []

    # Header
    lines.append(f"## {symbol} - Daily Prices ({num_days} Trading Days)")
    lines.append("")
    lines.append(f"**Period:** {start_date} to {end_date}")
    lines.append("")

    # Table header
    lines.append(
        "| Date       | Open      | High      | Low       | Close     | Volume    | Change    |"
    )
    lines.append(
        "|------------|-----------|-----------|-----------|-----------|-----------|-----------|"
    )

    # Table rows
    total_volume = 0
    for record in data:
        date = record.get("date", "N/A")
        open_price = record.get("open")
        high_price = record.get("high")
        low_price = record.get("low")
        close_price = record.get("close")
        volume = record.get("volume")
        change_pct = record.get("changePercent")

        # Format prices
        open_str = fmt_price(open_price, cur)
        high_str = fmt_price(high_price, cur)
        low_str = fmt_price(low_price, cur)
        close_str = fmt_price(close_price, cur)

        # Format volume
        volume_str = (
            fmt_count(volume) if volume is not None else "N/A"
        )
        if volume is not None:
            total_volume += volume

        # Format change percentage
        if change_pct is not None:
            sign = "+" if change_pct >= 0 else ""
            change_str = f"{sign}{change_pct:.2f}%"
        else:
            change_str = "N/A"

        lines.append(
            f"| {date} | {open_str:>9} | {high_str:>9} | {low_str:>9} | {close_str:>9} | {volume_str:>9} | {change_str:>9} |"
        )

    # Summary
    lines.append("")
    total_vol_str = fmt_count(total_volume)
    lines.append(f"**Total Volume:** {total_vol_str}")

    return "\n".join(lines)


def _calculate_price_statistics(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate aggregated statistics for a list of daily price data.

    Args:
        data: List of daily OHLCV dictionaries (sorted newest first)

    Returns:
        Dictionary containing aggregated statistics
    """
    if not data or len(data) == 0:
        return {}

    # Sort to have oldest first for calculations
    sorted_data = sorted(data, key=lambda x: x.get("date", ""), reverse=False)

    # Extract closing prices for calculations (finite values only — a forming
    # bar can carry NaN prices that would poison every derived stat)
    closes = [c for c in (finite_or_none(d.get("close")) for d in sorted_data) if c is not None]
    if not closes:
        return {}

    highs = [h for h in (finite_or_none(d.get("high")) for d in sorted_data) if h is not None]
    lows = [lo for lo in (finite_or_none(d.get("low")) for d in sorted_data) if lo is not None]

    # Aggregated OHLC
    first_day = sorted_data[0]
    last_day = sorted_data[-1]

    stats = {
        "symbol": data[0].get("symbol", "N/A"),
        "period_days": len(data),
        "start_date": first_day.get("date", "N/A"),
        "end_date": last_day.get("date", "N/A"),
        # Aggregated OHLC
        "period_open": finite_or_none(first_day.get("open")),
        "period_close": finite_or_none(last_day.get("close")),
        "period_high": max(highs) if highs else None,
        "period_low": min(lows) if lows else None,
        # Price range
        "min_close": min(closes),
        "max_close": max(closes),
        # Period performance
        "period_change": None,
        "period_change_pct": None,
    }

    # Calculate period performance
    if stats["period_open"] and stats["period_close"]:
        stats["period_change"] = stats["period_close"] - stats["period_open"]
        stats["period_change_pct"] = (
            stats["period_change"] / stats["period_open"]
        ) * 100

    # Moving averages (only calculate if enough data)
    stats["ma_20"] = None
    stats["ma_50"] = None
    stats["ma_200"] = None

    if len(closes) >= 20:
        stats["ma_20"] = sum(closes[-20:]) / 20
    if len(closes) >= 50:
        stats["ma_50"] = sum(closes[-50:]) / 50
    if len(closes) >= 200:
        stats["ma_200"] = sum(closes[-200:]) / 200

    # Volatility (standard deviation of daily returns)
    if len(closes) >= 2:
        daily_returns = []
        for i in range(1, len(closes)):
            if closes[i - 1] != 0:
                ret = ((closes[i] - closes[i - 1]) / closes[i - 1]) * 100
                daily_returns.append(ret)

        if daily_returns:
            # Calculate standard deviation
            mean_return = sum(daily_returns) / len(daily_returns)
            variance = sum((r - mean_return) ** 2 for r in daily_returns) / len(
                daily_returns
            )
            stats["volatility"] = variance**0.5  # Standard deviation
        else:
            stats["volatility"] = None
    else:
        stats["volatility"] = None

    # Volume statistics
    volumes = [v for v in (finite_or_none(d.get("volume")) for d in sorted_data) if v is not None]
    if volumes:
        stats["avg_volume"] = sum(volumes) / len(volumes)
        stats["total_volume"] = sum(volumes)
    else:
        stats["avg_volume"] = None
        stats["total_volume"] = None

    return stats


def _format_price_summary(stats: Dict[str, Any]) -> str:
    """
    Format price statistics into a human-readable summary report.

    Args:
        stats: Dictionary of calculated statistics

    Returns:
        Formatted string report
    """
    if not stats:
        return "No data available for summary"

    cur = _symbol_currency(resolve_ref(stats.get("symbol")))
    lines = []

    # Header
    period_days = stats.get("period_days", 0)
    start_date = stats.get("start_date", "N/A")
    end_date = stats.get("end_date", "N/A")

    lines.append(f"**Period:** {start_date} to {end_date} ({period_days} trading days)")
    lines.append("")

    # Collect all metrics for table
    metrics_rows = []

    # Period OHLC
    period_open = stats.get("period_open")
    period_close = stats.get("period_close")
    period_high = stats.get("period_high")
    period_low = stats.get("period_low")

    if period_open is not None:
        metrics_rows.append(("Period Open", fmt_price(period_open, cur)))
    if period_close is not None:
        metrics_rows.append(("Period Close", fmt_price(period_close, cur)))
    if period_high is not None:
        metrics_rows.append(("Period High", fmt_price(period_high, cur)))
    if period_low is not None:
        metrics_rows.append(("Period Low", fmt_price(period_low, cur)))

    # Performance
    period_change = stats.get("period_change")
    period_change_pct = stats.get("period_change_pct")

    if period_change is not None and period_change_pct is not None:
        sign = "+" if period_change >= 0 else ""
        metrics_rows.append(
            (
                "Period Change",
                f"{sign}{fmt_price(period_change, cur)} ({format_percentage(period_change_pct)})",
            )
        )

    min_close = stats.get("min_close")
    max_close = stats.get("max_close")
    if min_close is not None and max_close is not None:
        range_pct = ((max_close - min_close) / min_close) * 100 if min_close != 0 else 0
        metrics_rows.append(
            (
                "Price Range",
                f"{fmt_price(min_close, cur)} - {fmt_price(max_close, cur)} ({format_percentage(range_pct)} range)",
            )
        )

    volatility = stats.get("volatility")
    if volatility is not None:
        metrics_rows.append(("Volatility (Daily Std Dev)", f"{volatility:.2f}%"))

    # Moving Averages
    ma_20 = stats.get("ma_20")
    ma_50 = stats.get("ma_50")
    ma_200 = stats.get("ma_200")

    if ma_20 is not None:
        metrics_rows.append(("20-Day MA", fmt_price(ma_20, cur)))
    if ma_50 is not None:
        metrics_rows.append(("50-Day MA", fmt_price(ma_50, cur)))
    if ma_200 is not None:
        metrics_rows.append(("200-Day MA", fmt_price(ma_200, cur)))

    # Volume Statistics
    avg_volume = stats.get("avg_volume")
    total_volume = stats.get("total_volume")

    if avg_volume is not None:
        avg_vol_formatted = fmt_count(avg_volume)
        metrics_rows.append(("Average Daily Volume", avg_vol_formatted))
    if total_volume is not None:
        total_vol_formatted = fmt_count(total_volume)
        metrics_rows.append(("Total Volume", total_vol_formatted))

    # Output as markdown table
    if metrics_rows:
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        for metric, value in metrics_rows:
            lines.append(f"| {metric} | {value} |")
        lines.append("")

    return "\n".join(lines)


# A chart series names what an incomplete one lacks; a quote has no bars to miss.
_CHART_FRESHNESS_WORDS = {
    **FRESHNESS_WORDS,
    FreshnessLabel.INCOMPLETE: "incomplete (missing the session's final bars)",
}


def _chart_freshness_line(ref, freshness: Freshness) -> Optional[str]:
    """One line naming the chart series' provider and lag; None when it is current."""
    words = _CHART_FRESHNESS_WORDS.get(freshness.label)
    if not words:
        return None
    line = f"**Chart bars:** {freshness.source or 'unknown source'}, {words}"
    try:
        last = freshness.actual_latest
        local = venue_local_time(ref, datetime.fromtimestamp(last / 1000, tz=timezone.utc)) if last else None
    except (OverflowError, OSError, ValueError):
        local = None
    if local is not None:
        line += f" (last bar {local.strftime('%H:%M')} {local.strftime('%Z') or ref.tz})"
    return line


# A CN daily series is Tushare's forward-adjusted one where it serves and a
# fallback's unadjusted or split-only one where it does not. They differ by
# every dividend since, so the agent must know which it holds before comparing.
_TREATMENT_WORDS = {
    PriceTreatment.RAW: "unadjusted",
    PriceTreatment.SPLIT_ADJUSTED: "adjusted for splits only",
    PriceTreatment.DIVIDEND_ADJUSTED: "adjusted for splits and dividends",
}


def _treatment_line(source: Optional[str], ref) -> str:
    """The header line naming the daily rows' price treatment; empty with no source."""
    if not source:
        return ""
    words = _TREATMENT_WORDS.get(series_lineage(source, ref)[0])
    return f"**Prices:** {words}, from {source}\n" if words else ""


async def _live_stamp_for(
    provider,
    symbols: List[str],
    user_id: Optional[str],
    asset_type: str = "stocks",
) -> Optional[str]:
    """Best-effort `[Live: ...]` stamp; never raises."""
    try:
        snaps = await provider.get_snapshots(symbols, asset_type=asset_type, user_id=user_id)
        asset_class = AssetClass.INDEX if asset_type == "indices" else AssetClass.EQUITY
        return build_live_stamp(snaps or [], asset_class=asset_class)
    except Exception:
        return None


def _window_is_open(end_date: Optional[str], venue_now: datetime) -> bool:
    """Whether a window ending at *end_date* runs to the present.

    A date-only end is open from the venue's today on; a datetime end is a
    closed intraday window unless it is at or after the venue's clock, so a
    string compare against today's date would call 09:30-10:30 open-ended.
    A naive end reads as venue-local; one with an offset is an instant.
    """
    if not end_date:
        return True
    text = end_date.strip()
    if len(text) <= 10:
        return text >= venue_now.date().isoformat()
    try:
        end = datetime.fromisoformat(text)
    except ValueError:
        return text[:10] >= venue_now.date().isoformat()
    if end.tzinfo is None:
        return end >= venue_now.replace(tzinfo=None)
    return end >= venue_now.astimezone()


async def fetch_daily_prices(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch historical daily OHLCV price data for a stock.

    For periods < 14 trading days: Returns markdown table with daily OHLCV data
    For periods >= 14 trading days: Returns formatted summary report with aggregated statistics

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SH", "0700.HK")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Limit number of records (if not using date range)
        config: LangChain RunnableConfig (injected by @tool decorator)

    Returns:
        Tuple of (content string, artifact dict with structured data for charts)
    """
    stamp_task = None
    try:
        # Resolve once: normalize the agent-supplied spelling to the legacy form
        # provider calls / cache keys use, and reuse the ref for the display label.
        ref, symbol = resolve_listing(symbol)
        provider = await get_market_data_provider()
        user_id = _get_user_id(config)
        # Index symbols need index-market routing in the provider chain; the
        # resolved ref knows, and the caret prefix covers unresolved spellings.
        is_index = (
            ref.asset_class is AssetClass.INDEX
            if ref is not None
            else symbol.startswith("^")
        )
        # Concurrent freshness fetch on the resolved symbol so the live stamp and
        # the price history agree on spelling; awaited only on the success paths.
        stamp_task = asyncio.create_task(
            _live_stamp_for(
                provider, [symbol], user_id,
                asset_type="indices" if is_index else "stocks",
            )
        )

        # Default to last 60 trading days if no parameters
        if not start_date and not end_date and not limit:
            limit = 60

        # Fetch daily bars via provider chain (ginlix-data → FMP fallback)
        if start_date or end_date:
            raw_bars, daily_source, _ = await provider.get_daily_with_source(
                symbol, from_date=start_date, to_date=end_date,
                is_index=is_index, user_id=user_id,
            )
            results = _normalize_market_bars(raw_bars, symbol)
        else:
            if limit:
                end = datetime.now().date()
                # ~252 trading days a year plus a 50% weekend buffer; the flat two
                # weeks keep a small limit clear of a closure like Golden Week.
                days_back = int(limit * 1.5) + 14
                start = end - timedelta(days=days_back)

                raw_bars, daily_source, _ = await provider.get_daily_with_source(
                    symbol, from_date=start.isoformat(), to_date=end.isoformat(),
                    is_index=is_index, user_id=user_id,
                )
                results = _normalize_market_bars(raw_bars, symbol)

                # Apply limit after fetching (results are newest-first)
                if results and len(results) > limit:
                    results = results[:limit]
            else:
                raw_bars, daily_source, _ = await provider.get_daily_with_source(
                    symbol, is_index=is_index, user_id=user_id
                )
                results = _normalize_market_bars(raw_bars, symbol)

        if not results:
            logger.warning(f"No price data found for {symbol}")
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            content = f"""## Stock Price Data: {symbol}
**Retrieved:** {timestamp}
**Status:** No data available

No price data available for the specified period."""
            return content, {"type": "stock_prices", "symbol": symbol}

        # Generate file-ready header
        num_days = len(results)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        # Get actual date range from results
        dates = [d.get("date") for d in results if d.get("date")]
        if dates:
            sorted_dates = sorted(dates)
            actual_start = sorted_dates[0]
            actual_end = sorted_dates[-1]
        else:
            actual_start = start_date or "N/A"
            actual_end = end_date or "N/A"

        # Generate descriptive title
        if start_date and end_date:
            title = f"Stock Price Data: {symbol} ({start_date} to {end_date})"
        elif actual_start != "N/A" and actual_end != "N/A":
            title = f"Stock Price Data: {symbol} ({actual_start} to {actual_end})"
        else:
            title = f"Stock Price Data: {symbol}"

        cn_name = (await display_names(symbol))[0]
        company_line = f"**Company:** {cn_name}\n" if cn_name else ""
        header = f"""## {title}
{company_line}**Retrieved:** {timestamp}
**Market:** {_market_label(ref)}
**Period:** {actual_start} to {actual_end}
**Data Points:** {num_days} trading days
{_treatment_line(daily_source, ref)}
"""

        # Build OHLCV artifact data (sorted oldest first for charting)
        sorted_for_chart = sorted(
            results, key=lambda x: x.get("date", ""), reverse=False
        )
        ohlcv = [
            {
                "date": d.get("date"),
                "open": d.get("open"),
                "high": d.get("high"),
                "low": d.get("low"),
                "close": d.get("close"),
                "volume": d.get("volume"),
            }
            for d in sorted_for_chart
            if d.get("date")
        ]

        stats = _calculate_price_statistics(results)

        # Fetch intraday data at an appropriate interval for better chart
        # rendering. Short periods need finer granularity.
        chart_ohlcv = ohlcv
        chart_interval = "daily"
        # Measured in trading days, against the bars already in hand, so no
        # second fetch describes them. The chart series starts as the daily one.
        daily_freshness = measure_daily(
            symbol, raw_bars, is_index=is_index, source=daily_source
        )
        chart_source = daily_source
        chart_freshness = daily_freshness
        if num_days <= 60 and actual_start != "N/A" and actual_end != "N/A":
            if num_days <= 5:
                intraday_interval = "5min"
            elif num_days <= 20:
                intraday_interval = "1hour"
            else:
                intraday_interval = "4hour"

            try:
                # The daily series ends on the last PUBLISHED daily bar, which
                # during a session is yesterday; the intraday chart must run to
                # today or it stops a whole session short of the live stamp.
                # A Shanghai session is already on tomorrow's date in New York.
                venue_today = (venue_local_time(ref) or datetime.now()).date().isoformat()
                intraday_to = end_date or max(actual_end, venue_today)
                intraday_bars, intraday_source, _ = await provider.get_intraday_with_source(
                    symbol,
                    interval=intraday_interval,
                    from_date=actual_start,
                    to_date=intraday_to,
                    is_index=is_index,
                    user_id=user_id,
                )
                if intraday_bars and len(intraday_bars) > 5:
                    # Normalize and sort oldest-first for charting
                    intraday_norm = _normalize_market_bars(
                        intraday_bars, symbol, datetime_format=True
                    )
                    intraday_sorted = sorted(
                        intraday_norm,
                        key=lambda x: x.get("date", ""),
                        reverse=False,
                    )
                    chart_ohlcv = [
                        {
                            "date": d.get("date"),
                            "open": d.get("open"),
                            "high": d.get("high"),
                            "low": d.get("low"),
                            "close": d.get("close"),
                            "volume": d.get("volume"),
                        }
                        for d in intraday_sorted
                        if d.get("date")
                    ]
                    chart_interval = intraday_interval
                    chart_source = intraday_source
                    chart_freshness = measure_bars(
                        symbol, intraday_interval, intraday_bars,
                        is_index=is_index, source=intraday_source,
                        tier=series_lineage(intraday_source, ref)[1].value,
                    )
                    logger.debug(
                        f"Fetched {len(chart_ohlcv)} intraday ({intraday_interval}) "
                        f"data points for {symbol}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to fetch intraday data for {symbol}, "
                    f"falling back to daily: {e}"
                )

        # Freshness measures the series against now, so it only describes a
        # window that runs to the present. A closed historical window would
        # always read STALE.
        venue_now = venue_local_time(ref) or datetime.now()
        open_ended = _window_is_open(end_date, venue_now)

        artifact = {
            "type": "stock_prices",
            "symbol": symbol,
            **({"name": cn_name} if cn_name else {}),
            # The chart renders the axis and tooltips off this — without it the
            # client falls back to a suffix guess and a CNY series reads as USD.
            **({"price_currency": ref.price_currency} if ref is not None else {}),
            "ohlcv": ohlcv,
            "chart_ohlcv": chart_ohlcv,
            "chart_interval": chart_interval,
            # Who served `ohlcv`. The table and the card's sparkline both
            # read it, so the card must credit the daily publisher, not whoever
            # happened to fill the finer-grained chart series below.
            "source": daily_source,
            **({"freshness": daily_freshness.model_dump(mode="json")} if open_ended else {}),
            # `chart_ohlcv` can come from a different provider at a different
            # tier; the detail chart is the only surface that renders it.
            "chart_source": chart_source,
            **({"chart_freshness": chart_freshness.model_dump(mode="json")} if open_ended else {}),
            "stats": {
                "period_change_pct": stats.get("period_change_pct"),
                "ma_20": stats.get("ma_20"),
                "ma_50": stats.get("ma_50"),
                "volatility": stats.get("volatility"),
                "avg_volume": stats.get("avg_volume"),
                "period_high": stats.get("period_high"),
                "period_low": stats.get("period_low"),
            },
        }

        # Check if we should return normalized summary or markdown table
        if num_days >= 14:
            # Return normalized summary for long periods
            logger.debug(
                f"Retrieved {num_days} days for {symbol}, returning normalized summary"
            )
            content = header + _format_price_summary(stats)
        else:
            # Return markdown table for short periods
            logger.debug(
                f"Retrieved {num_days} daily price records for {symbol}, returning markdown table"
            )
            content = header + _format_price_data_as_table(results)

        # Only for an open-ended window; see ``open_ended`` above.
        if chart_interval != "daily" and open_ended:
            note = _chart_freshness_line(ref, chart_freshness)
            if note:
                content = f"{content}\n\n{note}"

        stamp = await stamp_task
        if stamp:
            content = f"{stamp}\n\n{content}"
        return content, artifact

    except Exception as e:
        logger.error(f"Error retrieving daily prices for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        content = f"""## Stock Price Data: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving price data: {str(e)}"""
        return content, {"type": "stock_prices", "symbol": symbol, "error": str(e)}
    finally:
        # Success paths awaited the task (done → no-op); every other exit —
        # early return, error, or cancellation of this coroutine — must not
        # leak a pending fetch. cancel() only requests; await so the underlying
        # HTTP call is actually closed before returning.
        if stamp_task is not None and not stamp_task.done():
            stamp_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stamp_task
