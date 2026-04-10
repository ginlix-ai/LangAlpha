# pyright: ignore
"""
Core implementation logic for market data tools.

Contains business logic separated from LangChain tool decorators.
"""

from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone
import logging
import asyncio

from langchain_core.runnables import RunnableConfig

from .utils import format_number, format_percentage, get_market_session
from src.data_client import get_financial_data_provider, get_market_data_provider
from src.data_client.ginlix_data.pagination import paginate_cursor
from src.data_client.market_data_provider import symbol_timezone

logger = logging.getLogger(__name__)


def _get_user_id(config: Optional[RunnableConfig] = None) -> Optional[str]:
    """Extract user_id from RunnableConfig, or return None."""
    if config is None:
        return None
    return config.get("configurable", {}).get("user_id")


def _safe_result(result, default=None):
    """Extract result from asyncio.gather, returning default if exception."""
    if isinstance(result, Exception):
        return default
    return result if result is not None else default


def _normalize_market_bars(
    bars: list, symbol: str, datetime_format: bool = False
) -> List[Dict[str, Any]]:
    """Convert MarketDataSource bars to the format expected by formatting helpers.

    MarketDataSource returns ``{time, open, high, low, close, volume}``.
    Formatters expect ``{date, open, high, low, close, volume, change,
    changePercent, symbol}``.  Returns newest-first.
    """
    if not bars:
        return []

    # Sort ascending for correct change computation
    sorted_bars = sorted(bars, key=lambda b: b.get("time", 0))
    result: List[Dict[str, Any]] = []
    prev_close: Optional[float] = None

    tz = symbol_timezone(symbol)

    for bar in sorted_bars:
        ts = bar.get("time")
        if ts is not None and isinstance(ts, (int, float)) and ts > 0:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(tz)
            date_str = (
                dt.strftime("%Y-%m-%d %H:%M:%S")
                if datetime_format
                else dt.strftime("%Y-%m-%d")
            )
        else:
            date_str = bar.get("date", "N/A")

        close = bar.get("close")
        change: Optional[float] = None
        change_pct: Optional[float] = None
        if close is not None and prev_close is not None and prev_close != 0:
            change = close - prev_close
            change_pct = (change / prev_close) * 100

        result.append(
            {
                "date": date_str,
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": close,
                "volume": bar.get("volume"),
                "change": change,
                "changePercent": change_pct,
                "symbol": symbol,
            }
        )

        if close is not None:
            prev_close = close

    # Return newest-first (matching FMP's original order)
    result.reverse()
    return result


async def _fmp_request(method: str, *args: Any, **kwargs: Any) -> Any:
    """Make a direct FMP API call for methods not in the protocol."""
    from src.data_client.fmp import get_fmp_client

    client = await get_fmp_client()
    return await getattr(client, method)(*args, **kwargs)


# Constants for fiscal period matching
FILING_DATE_TOLERANCE_DAYS = (
    5  # Allow 5 days difference when matching filings to earnings
)
DAYS_PER_QUARTER = 90  # Approximate days per fiscal quarter


def _build_fiscal_period_lookup(income_stmt: List[Dict]) -> Dict[str, str]:
    """Build a lookup dict mapping fiscal end dates to period names (e.g., 'Q3 FY2026')."""
    lookup = {}
    for stmt in income_stmt:
        stmt_date = stmt.get("date")
        period = stmt.get("period")  # Q1, Q2, Q3, Q4
        calendar_year = stmt.get("calendarYear")
        if stmt_date and period and calendar_year:
            lookup[stmt_date] = f"{period} FY{calendar_year}"
    return lookup


def _infer_fiscal_period(
    fiscal_ending: str, fiscal_period_lookup: Dict[str, str]
) -> Optional[str]:
    """
    Infer fiscal period name for a date not in the lookup.
    Uses the pattern from existing quarters to estimate future quarters.
    """
    if not fiscal_ending or not fiscal_period_lookup:
        return None

    try:
        fe_date = datetime.strptime(fiscal_ending, "%Y-%m-%d")

        # Find the most recent known quarter
        for date_str, period_str in sorted(fiscal_period_lookup.items(), reverse=True):
            if not period_str.startswith("Q"):
                continue

            last_date = datetime.strptime(date_str, "%Y-%m-%d")
            last_q = int(period_str[1])
            last_fy = int(period_str.split("FY")[1])

            # Calculate quarter offset from days difference
            days_diff = (fe_date - last_date).days
            quarters_ahead = round(days_diff / DAYS_PER_QUARTER)
            next_q = last_q + quarters_ahead
            next_fy = last_fy

            # Handle fiscal year rollover
            while next_q > 4:
                next_q -= 4
                next_fy += 1
            while next_q < 1:
                next_q += 4
                next_fy -= 1

            return f"Q{next_q} FY{next_fy}"

    except (ValueError, KeyError) as e:
        logger.debug(f"Could not infer fiscal period for {fiscal_ending}: {e}")

    return None


def _match_filing_to_fiscal_period(
    filing_date: str,
    earnings_calendar: List[Dict],
    fiscal_period_lookup: Dict[str, str],
) -> str:
    """
    Match a SEC filing date to its fiscal period using earnings calendar.
    Returns the fiscal period name or 'Quarterly' if no match found.
    """
    if not earnings_calendar or not filing_date or filing_date == "N/A":
        return "Quarterly"

    try:
        filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        best_match = None
        min_diff = float("inf")

        for cal in earnings_calendar:
            cal_date = cal.get("date")
            fiscal_ending = cal.get("fiscalDateEnding")
            if not cal_date or not fiscal_ending:
                continue

            try:
                cal_dt = datetime.strptime(cal_date, "%Y-%m-%d")
                diff = abs((filing_dt - cal_dt).days)
                if diff < min_diff and diff <= FILING_DATE_TOLERANCE_DAYS:
                    min_diff = diff
                    if fiscal_ending in fiscal_period_lookup:
                        best_match = fiscal_period_lookup[fiscal_ending]
            except ValueError:
                continue

        return best_match or "Quarterly"

    except ValueError:
        return "Quarterly"


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
        open_str = f"${open_price:.2f}" if open_price is not None else "N/A"
        high_str = f"${high_price:.2f}" if high_price is not None else "N/A"
        low_str = f"${low_price:.2f}" if low_price is not None else "N/A"
        close_str = f"${close_price:.2f}" if close_price is not None else "N/A"

        # Format volume
        volume_str = (
            format_number(volume).replace("$", "") if volume is not None else "N/A"
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
    total_vol_str = format_number(total_volume).replace("$", "")
    lines.append(f"**Total Volume:** {total_vol_str}")

    return "\n".join(lines)


def _format_indices_data_as_table(indices_data: Dict[str, List[Dict[str, Any]]]) -> str:
    """
    Format multiple market indices data as markdown tables.

    Args:
        indices_data: Dictionary mapping index symbol to list of price data

    Returns:
        Markdown-formatted tables string (one table per index)
    """
    if not indices_data:
        return "No index data available."

    lines = []

    # Count total days
    all_dates = set()
    for data_list in indices_data.values():
        for record in data_list:
            if record.get("date"):
                all_dates.add(record.get("date"))

    num_days = len(all_dates)
    sorted_dates = sorted(all_dates)
    start_date = sorted_dates[0] if sorted_dates else "N/A"
    end_date = sorted_dates[-1] if sorted_dates else "N/A"

    # Header
    lines.append(f"## Market Indices ({num_days} Trading Days)")
    lines.append("")
    lines.append(f"**Period:** {start_date} to {end_date}")
    lines.append("")

    # Create table for each index
    for i, (symbol, data) in enumerate(indices_data.items()):
        if not data:
            continue

        # Index name
        index_name = _get_index_name(symbol)
        lines.append(f"### {index_name} ({symbol})")
        lines.append("")

        # Table header
        lines.append(
            "| Date       | Open        | High        | Low         | Close       | Volume      | Change    |"
        )
        lines.append(
            "|------------|-------------|-------------|-------------|-------------|-------------|-----------|"
        )

        # Table rows
        for record in data:
            date = record.get("date", "N/A")
            open_price = record.get("open")
            high_price = record.get("high")
            low_price = record.get("low")
            close_price = record.get("close")
            volume = record.get("volume")
            change_pct = record.get("changePercent")

            # Format prices
            open_str = f"{open_price:,.2f}" if open_price is not None else "N/A"
            high_str = f"{high_price:,.2f}" if high_price is not None else "N/A"
            low_str = f"{low_price:,.2f}" if low_price is not None else "N/A"
            close_str = f"{close_price:,.2f}" if close_price is not None else "N/A"

            # Format volume
            volume_str = (
                format_number(volume).replace("$", "") if volume is not None else "N/A"
            )

            # Format change percentage
            if change_pct is not None:
                sign = "+" if change_pct >= 0 else ""
                change_str = f"{sign}{change_pct:.2f}%"
            else:
                change_str = "N/A"

            lines.append(
                f"| {date} | {open_str:>11} | {high_str:>11} | {low_str:>11} | {close_str:>11} | {volume_str:>11} | {change_str:>9} |"
            )

        # Add spacing between indices
        if i < len(indices_data) - 1:
            lines.append("")

    return "\n".join(lines)


def _format_sectors_as_table(sectors_data: List[Dict[str, Any]]) -> str:
    """
    Format sector performance data as a markdown table.

    Args:
        sectors_data: List of sector performance dictionaries

    Returns:
        Markdown-formatted table string
    """
    if not sectors_data or len(sectors_data) == 0:
        return "No sector performance data available."

    lines = []

    # Header
    lines.append("## Sector Performance")
    lines.append("")

    # Table header
    lines.append("| Sector                      | Change    | Status    |")
    lines.append("|-----------------------------|-----------|-----------|")

    # Parse and sort sectors by performance
    parsed_sectors = []
    for sector in sectors_data:
        sector_name = sector.get("sector", "N/A")
        change_str = sector.get("changesPercentage", "0%")

        # Parse percentage (handle formats like "+1.50%" or "-0.42%")
        try:
            change_val = float(change_str.replace("%", "").replace("+", ""))
        except (ValueError, AttributeError):
            change_val = 0.0

        parsed_sectors.append(
            {"name": sector_name, "change_str": change_str, "change_val": change_val}
        )

    # Sort by performance (descending)
    parsed_sectors.sort(key=lambda x: x["change_val"], reverse=True)

    # Table rows
    for sector in parsed_sectors:
        name = sector["name"]
        change_str = sector["change_str"]
        change_val = sector["change_val"]

        # Add status indicator
        if change_val > 0:
            status = "📈 Up"
        elif change_val < 0:
            status = "📉 Down"
        else:
            status = "➡️ Flat"

        # Pad percentage for alignment
        if not change_str.startswith("+") and not change_str.startswith("-"):
            if change_val >= 0:
                change_str = "+" + change_str

        lines.append(f"| {name:27} | {change_str:>9} | {status:9} |")

    # Summary
    if parsed_sectors:
        best = parsed_sectors[0]
        worst = parsed_sectors[-1]

        lines.append("")
        lines.append(f"**Best Performing:** {best['name']} ({best['change_str']})")
        lines.append(f"**Worst Performing:** {worst['name']} ({worst['change_str']})")

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

    # Extract closing prices for calculations
    closes = [d.get("close") for d in sorted_data if d.get("close") is not None]
    if not closes:
        return {}

    # Aggregated OHLC
    first_day = sorted_data[0]
    last_day = sorted_data[-1]

    stats = {
        "symbol": data[0].get("symbol", "N/A"),
        "period_days": len(data),
        "start_date": first_day.get("date", "N/A"),
        "end_date": last_day.get("date", "N/A"),
        # Aggregated OHLC
        "period_open": first_day.get("open"),
        "period_close": last_day.get("close"),
        "period_high": max(
            d.get("high") for d in sorted_data if d.get("high") is not None
        ),
        "period_low": min(
            d.get("low") for d in sorted_data if d.get("low") is not None
        ),
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
    volumes = [d.get("volume") for d in sorted_data if d.get("volume") is not None]
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

    from .utils import format_number, format_percentage

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
        metrics_rows.append(("Period Open", f"${period_open:.2f}"))
    if period_close is not None:
        metrics_rows.append(("Period Close", f"${period_close:.2f}"))
    if period_high is not None:
        metrics_rows.append(("Period High", f"${period_high:.2f}"))
    if period_low is not None:
        metrics_rows.append(("Period Low", f"${period_low:.2f}"))

    # Performance
    period_change = stats.get("period_change")
    period_change_pct = stats.get("period_change_pct")

    if period_change is not None and period_change_pct is not None:
        sign = "+" if period_change >= 0 else ""
        metrics_rows.append(
            (
                "Period Change",
                f"{sign}${period_change:.2f} ({format_percentage(period_change_pct)})",
            )
        )

    min_close = stats.get("min_close")
    max_close = stats.get("max_close")
    if min_close is not None and max_close is not None:
        range_pct = ((max_close - min_close) / min_close) * 100 if min_close != 0 else 0
        metrics_rows.append(
            (
                "Price Range",
                f"${min_close:.2f} - ${max_close:.2f} ({format_percentage(range_pct)} range)",
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
        metrics_rows.append(("20-Day MA", f"${ma_20:.2f}"))
    if ma_50 is not None:
        metrics_rows.append(("50-Day MA", f"${ma_50:.2f}"))
    if ma_200 is not None:
        metrics_rows.append(("200-Day MA", f"${ma_200:.2f}"))

    # Volume Statistics
    avg_volume = stats.get("avg_volume")
    total_volume = stats.get("total_volume")

    if avg_volume is not None:
        avg_vol_formatted = format_number(avg_volume).replace("$", "")
        metrics_rows.append(("Average Daily Volume", avg_vol_formatted))
    if total_volume is not None:
        total_vol_formatted = format_number(total_volume).replace("$", "")
        metrics_rows.append(("Total Volume", total_vol_formatted))

    # Output as markdown table
    if metrics_rows:
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        for metric, value in metrics_rows:
            lines.append(f"| {metric} | {value} |")
        lines.append("")

    return "\n".join(lines)


def _format_indices_summary(
    indices_data: Dict[str, List[Dict[str, Any]]], period_info: Dict[str, Any]
) -> str:
    """
    Format multiple market indices statistics into a summary report.

    Args:
        indices_data: Dictionary mapping index symbol to list of price data
        period_info: Dictionary with period metadata (num_days, start_date, end_date)

    Returns:
        Formatted string report with sections for each index
    """
    if not indices_data:
        return "No index data available for summary"

    from .utils import format_percentage

    lines = []

    # Header
    num_days = period_info.get("num_days", 0)
    start_date = period_info.get("start_date", "N/A")
    end_date = period_info.get("end_date", "N/A")

    lines.append(f"**Period:** {start_date} to {end_date} ({num_days} trading days)")
    lines.append("")

    # Process each index
    for i, (symbol, data) in enumerate(indices_data.items()):
        if not data:
            continue

        # Calculate statistics for this index using existing helper
        stats = _calculate_price_statistics(data)

        if not stats:
            continue

        # Index section header
        index_name = _get_index_name(symbol)
        lines.append(f"### {index_name} ({symbol})")
        lines.append("")

        # Collect metrics for table
        metrics_rows = []

        # Period OHLC
        period_open = stats.get("period_open")
        period_close = stats.get("period_close")
        period_high = stats.get("period_high")
        period_low = stats.get("period_low")

        if period_open is not None and period_close is not None:
            metrics_rows.append(("Period", f"${period_open:.2f} → ${period_close:.2f}"))
        if period_high is not None and period_low is not None:
            metrics_rows.append(("Range", f"${period_low:.2f} - ${period_high:.2f}"))

        # Performance
        period_change = stats.get("period_change")
        period_change_pct = stats.get("period_change_pct")
        if period_change is not None and period_change_pct is not None:
            sign = "+" if period_change >= 0 else ""
            metrics_rows.append(
                (
                    "Change",
                    f"{sign}${period_change:.2f} ({format_percentage(period_change_pct)})",
                )
            )

        # Volatility
        volatility = stats.get("volatility")
        if volatility is not None:
            metrics_rows.append(("Volatility", f"{volatility:.2f}%"))

        # Moving Averages
        ma_20 = stats.get("ma_20")
        ma_50 = stats.get("ma_50")
        ma_200 = stats.get("ma_200")

        if ma_20 is not None:
            metrics_rows.append(("20-Day MA", f"${ma_20:.2f}"))
        if ma_50 is not None:
            metrics_rows.append(("50-Day MA", f"${ma_50:.2f}"))
        if ma_200 is not None:
            metrics_rows.append(("200-Day MA", f"${ma_200:.2f}"))

        # Output as markdown table
        if metrics_rows:
            lines.append("| Metric | Value |")
            lines.append("|--------|-------|")
            for metric, value in metrics_rows:
                lines.append(f"| {metric} | {value} |")

        # Add spacing between indices (except for last one)
        if i < len(indices_data) - 1:
            lines.append("")

    return "\n".join(lines)


def _get_index_name(symbol: str) -> str:
    """Get human-readable name for common market indices."""
    index_names = {
        "^GSPC": "S&P 500",
        "^IXIC": "NASDAQ Composite",
        "^DJI": "Dow Jones Industrial",
        "^RUT": "Russell 2000",
        "^VIX": "CBOE Volatility Index",
        "000001.SS": "SSE Composite",
        "399001.SZ": "SZSE Component",
        "000300.SS": "CSI 300",
        "^HSI": "Hang Seng Index",
        "^HSCE": "Hang Seng China Enterprises",
    }
    return index_names.get(symbol, symbol)


async def fetch_stock_daily_prices(
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
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Limit number of records (if not using date range)
        config: LangChain RunnableConfig (injected by @tool decorator)

    Returns:
        Tuple of (content string, artifact dict with structured data for charts)
    """
    try:
        provider = await get_market_data_provider()
        user_id = _get_user_id(config)

        # Default to last 60 trading days if no parameters
        if not start_date and not end_date and not limit:
            limit = 60

        # Fetch daily bars via provider chain (ginlix-data → FMP fallback)
        if start_date or end_date:
            raw_bars = await provider.get_daily(
                symbol, from_date=start_date, to_date=end_date, user_id=user_id
            )
            results = _normalize_market_bars(raw_bars, symbol)
        else:
            if limit:
                end = datetime.now().date()
                # Estimate: ~252 trading days per year, add 50% buffer for weekends/holidays
                days_back = int(limit * 1.5)
                start = end - timedelta(days=days_back)

                raw_bars = await provider.get_daily(
                    symbol, from_date=start.isoformat(), to_date=end.isoformat(),
                    user_id=user_id,
                )
                results = _normalize_market_bars(raw_bars, symbol)

                # Apply limit after fetching (results are newest-first)
                if results and len(results) > limit:
                    results = results[:limit]
            else:
                raw_bars = await provider.get_daily(symbol, user_id=user_id)
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

        header = f"""## {title}
**Retrieved:** {timestamp}
**Market:** US Stock
**Period:** {actual_start} to {actual_end}
**Data Points:** {num_days} trading days

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
        if num_days <= 60 and actual_start != "N/A" and actual_end != "N/A":
            if num_days <= 5:
                intraday_interval = "5min"
            elif num_days <= 20:
                intraday_interval = "1hour"
            else:
                intraday_interval = "4hour"

            try:
                intraday_bars = await provider.get_intraday(
                    symbol,
                    interval=intraday_interval,
                    from_date=actual_start,
                    to_date=actual_end,
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
                    logger.debug(
                        f"Fetched {len(chart_ohlcv)} intraday ({intraday_interval}) "
                        f"data points for {symbol}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to fetch intraday data for {symbol}, "
                    f"falling back to daily: {e}"
                )

        artifact = {
            "type": "stock_prices",
            "symbol": symbol,
            "ohlcv": ohlcv,
            "chart_ohlcv": chart_ohlcv,
            "chart_interval": chart_interval,
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
            return header + _format_price_summary(stats), artifact
        else:
            # Return markdown table for short periods
            logger.debug(
                f"Retrieved {num_days} daily price records for {symbol}, returning markdown table"
            )
            return header + _format_price_data_as_table(results), artifact

    except Exception as e:
        logger.error(f"Error retrieving daily prices for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        content = f"""## Stock Price Data: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving price data: {str(e)}"""
        return content, {"type": "stock_prices", "symbol": symbol, "error": str(e)}


async def _yfinance_fallback_overview(
    symbol: str, artifact: Dict[str, Any]
) -> None:
    """Fill missing Company Overview sections from YFinance when FMP returned sparse data.

    Mutates *artifact* in-place. Skips entirely if YFinance is not installed or
    if the primary provider was already YFinance.
    """
    from src.data_client.registry import _fmp_available, _eodhd_available, _yfinance_available

    # Run fallback when FMP or EODHD is primary and YFinance is available
    if not (_fmp_available() or _eodhd_available()) or not _yfinance_available():
        return

    # Determine which sections are missing or contain only zeros
    def _is_empty_dict(d: Any) -> bool:
        """True if dict is missing, None, empty, or all values are falsy/zero."""
        if not d or not isinstance(d, dict):
            return True
        return all(not v for v in d.values())

    missing_quote = _is_empty_dict(artifact.get("quote"))
    missing_perf = _is_empty_dict(artifact.get("performance"))
    missing_ratings = "analystRatings" not in artifact
    missing_fundamentals = "quarterlyFundamentals" not in artifact
    missing_earnings = "earningsSurprises" not in artifact
    missing_cashflow = "cashFlow" not in artifact

    if not any([
        missing_quote, missing_perf, missing_ratings,
        missing_fundamentals, missing_earnings, missing_cashflow,
    ]):
        return

    try:
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        yf_source = YFinanceFinancialSource()
        # Fetch only what's missing, in parallel
        tasks = {}
        if missing_quote:
            tasks["quote"] = yf_source.get_realtime_quote(symbol)
        if missing_perf:
            tasks["perf"] = yf_source.get_price_performance(symbol)
        if missing_ratings:
            tasks["ratings"] = yf_source.get_analyst_ratings(symbol)
        if missing_fundamentals:
            tasks["income"] = yf_source.get_income_statements(
                symbol, period="quarter", limit=8
            )
        if missing_earnings:
            tasks["earnings"] = yf_source.get_earnings_history(symbol, limit=10)
        if missing_cashflow:
            tasks["cashflow"] = yf_source.get_cash_flows(
                symbol, period="quarter", limit=8
            )

        keys = list(tasks.keys())
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        fetched = {k: _safe_result(v, []) for k, v in zip(keys, results)}

        # Build fiscal period lookup from income statements (needed by multiple sections)
        yf_income = fetched.get("income", [])
        yf_fiscal_lookup = _build_fiscal_period_lookup(yf_income) if yf_income else {}

        # Quote — only use if at least price or previousClose is non-zero
        yf_quote = fetched.get("quote", [])
        if yf_quote and len(yf_quote) > 0:
            q = yf_quote[0]
            if q.get("price") or q.get("previousClose"):
                artifact["quote"] = {
                    "price": q.get("price"),
                    "change": q.get("change"),
                    "changePct": q.get("changesPercentage"),
                    "dayHigh": q.get("dayHigh"),
                    "dayLow": q.get("dayLow"),
                    "yearHigh": q.get("yearHigh"),
                    "yearLow": q.get("yearLow"),
                    "open": q.get("open"),
                    "previousClose": q.get("previousClose"),
                    "volume": q.get("volume"),
                    "avgVolume": q.get("avgVolume"),
                    "marketCap": q.get("marketCap"),
                    "pe": q.get("pe"),
                    "eps": q.get("eps"),
                }

        # Performance — filter out all-zero results (means provider had no data)
        yf_perf = fetched.get("perf", [])
        if yf_perf:
            changes = yf_perf[0]
            perf = {
                k: changes.get(k)
                for k in ["1D", "5D", "1M", "3M", "6M", "ytd", "1Y", "3Y", "5Y"]
                if changes.get(k) is not None
            }
            # Only use if at least one non-zero value exists
            if perf and any(v != 0 for v in perf.values()):
                artifact["performance"] = perf

        # Analyst ratings
        yf_ratings = fetched.get("ratings", [])
        if yf_ratings:
            gs = yf_ratings[0]
            artifact["analystRatings"] = {
                "strongBuy": gs.get("strongBuy", 0),
                "buy": gs.get("buy", 0),
                "hold": gs.get("hold", 0),
                "sell": gs.get("sell", 0),
                "strongSell": gs.get("strongSell", 0),
                "consensus": gs.get("consensus", "N/A"),
            }

        # Quarterly fundamentals
        if yf_income:
            artifact["quarterlyFundamentals"] = [
                {
                    "period": yf_fiscal_lookup.get(
                        stmt.get("date"), stmt.get("date", "")
                    ),
                    "date": stmt.get("date"),
                    "revenue": stmt.get("revenue"),
                    "netIncome": stmt.get("netIncome"),
                    "grossProfit": stmt.get("grossProfit"),
                    "operatingIncome": stmt.get("operatingIncome"),
                    "ebitda": stmt.get("ebitda"),
                    "epsDiluted": stmt.get("epsdiluted"),
                    "grossMargin": stmt.get("grossProfitRatio"),
                    "operatingMargin": stmt.get("operatingIncomeRatio"),
                    "netMargin": stmt.get("netIncomeRatio"),
                }
                for stmt in reversed(yf_income)
            ]

        # Earnings surprises
        yf_earnings = fetched.get("earnings", [])
        reported = [e for e in yf_earnings if e.get("eps") is not None]
        if reported:
            artifact["earningsSurprises"] = [
                {
                    "period": yf_fiscal_lookup.get(
                        e.get("fiscalDateEnding"), e.get("date", "")
                    ),
                    "date": e.get("date"),
                    "epsActual": e.get("eps"),
                    "epsEstimate": e.get("epsEstimated"),
                    "revenueActual": e.get("revenue"),
                    "revenueEstimate": e.get("revenueEstimated"),
                }
                for e in reversed(reported)
            ]

        # Cash flow
        yf_cashflow = fetched.get("cashflow", [])
        if yf_cashflow:
            artifact["cashFlow"] = [
                {
                    "period": yf_fiscal_lookup.get(
                        cf.get("date"), cf.get("date", "")
                    ),
                    "date": cf.get("date"),
                    "operatingCashFlow": cf.get("operatingCashFlow"),
                    "capitalExpenditure": cf.get("capitalExpenditure"),
                    "freeCashFlow": cf.get("freeCashFlow"),
                }
                for cf in reversed(yf_cashflow)
            ]

    except Exception:
        logger.debug(
            "yfinance fallback for company overview failed | symbol=%s",
            symbol,
            exc_info=True,
        )

    # If performance is still missing, compute from Alpaca/market-data daily bars
    if "performance" not in artifact:
        await _market_data_performance_fallback(symbol, artifact)


async def _market_data_performance_fallback(
    symbol: str, artifact: Dict[str, Any]
) -> None:
    """Compute price performance from MarketDataSource daily bars (Alpaca/Ginlix).

    Used as a last resort when both FMP and YFinance lack performance data.
    """
    try:
        provider = await get_market_data_provider()
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=1900)).strftime("%Y-%m-%d")
        bars = await provider.get_daily(symbol, from_date=start)
        if hasattr(bars, "bars"):
            bars = bars.bars
        if not bars or len(bars) < 2:
            return

        # Sort by time ascending
        bars.sort(key=lambda b: b.get("time", 0))
        latest_close = bars[-1].get("close")
        if not latest_close:
            return

        def _pct_change(days: int) -> Optional[float]:
            target_ts = (now - timedelta(days=days)).timestamp() * 1000
            ref_bar = None
            for b in bars:
                if b.get("time", 0) <= target_ts:
                    ref_bar = b
                else:
                    break
            if ref_bar and ref_bar.get("close"):
                ref = ref_bar["close"]
                return round((latest_close - ref) / ref * 100, 2)
            return None

        # YTD
        ytd_start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
        ytd_days = (now - ytd_start).days

        perf = {}
        for label, days in [
            ("1D", 1), ("5D", 5), ("1M", 30), ("3M", 90),
            ("6M", 180), ("1Y", 365), ("3Y", 1095), ("5Y", 1825),
        ]:
            val = _pct_change(days)
            if val is not None:
                perf[label] = val
        ytd_val = _pct_change(ytd_days)
        if ytd_val is not None:
            perf["ytd"] = ytd_val

        if perf:
            artifact["performance"] = perf

    except Exception:
        logger.debug(
            "market data performance fallback failed | symbol=%s",
            symbol,
            exc_info=True,
        )


async def fetch_company_overview_data(symbol: str) -> Dict[str, Any]:
    """
    Fetch company overview data and return structured artifact dict.

    Shared by both the agent tool and the REST API endpoint.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")

    Returns:
        Dict with structured data for charts (same shape as agent artifact)
    """
    provider = await get_financial_data_provider()
    financial = provider.financial
    if financial is None:
        return {"type": "company_overview", "symbol": symbol}

    profile_data = await financial.get_company_profile(symbol)
    if not profile_data:
        # FMP has no profile — try YFinance fallback for the entire overview
        artifact: Dict[str, Any] = {"type": "company_overview", "symbol": symbol}
        await _yfinance_fallback_overview(symbol, artifact)
        # Try to get the name from YFinance quote data
        if "quote" in artifact and not artifact.get("name"):
            from src.data_client.registry import _yfinance_available
            if _yfinance_available():
                try:
                    from src.data_client.yfinance.financial_source import (
                        YFinanceFinancialSource,
                    )
                    yf_profile = await YFinanceFinancialSource().get_company_profile(
                        symbol
                    )
                    if yf_profile:
                        artifact["name"] = yf_profile[0].get(
                            "companyName", symbol
                        )
                except Exception:
                    pass
        return artifact

    profile = profile_data[0]
    company_name = profile.get("companyName", symbol)

    # === PARALLEL DATA FETCH ===
    (
        income_stmt_result,
        earnings_calendar_result,
        price_change_result,
        key_metrics_result,
        ratios_result,
        price_target_consensus_result,
        grades_summary_result,
        product_data_result,
        geo_data_result,
        quote_result,
        cash_flow_result,
    ) = await asyncio.gather(
        financial.get_income_statements(symbol, period="quarter", limit=8),
        financial.get_earnings_history(symbol, limit=10),
        financial.get_price_performance(symbol),
        financial.get_key_metrics(symbol),
        financial.get_financial_ratios(symbol),
        financial.get_analyst_price_targets(symbol),
        financial.get_analyst_ratings(symbol),
        financial.get_revenue_by_segment(
            symbol, segment_type="product", period="quarter", structure="flat"
        ),
        financial.get_revenue_by_segment(
            symbol, segment_type="geography", period="quarter", structure="flat"
        ),
        financial.get_realtime_quote(symbol),
        financial.get_cash_flows(symbol, period="quarter", limit=8),
        return_exceptions=True,
    )

    income_stmt = _safe_result(income_stmt_result, [])
    earnings_calendar = _safe_result(earnings_calendar_result, [])
    price_change_data = _safe_result(price_change_result, [])
    quote_data = _safe_result(quote_result, [])
    grades_summary_data = _safe_result(grades_summary_result, [])
    product_data = _safe_result(product_data_result, [])
    geo_data = _safe_result(geo_data_result, [])
    cash_flow_data = _safe_result(cash_flow_result, [])

    fiscal_period_lookup = _build_fiscal_period_lookup(income_stmt)

    # Build artifact
    artifact: Dict[str, Any] = {
        "type": "company_overview",
        "symbol": symbol,
        "name": company_name,
    }

    # Quote data
    if quote_data and len(quote_data) > 0:
        quote = quote_data[0]
        artifact["quote"] = {
            "price": quote.get("price"),
            "change": quote.get("change"),
            "changePct": quote.get("changesPercentage"),
            "dayHigh": quote.get("dayHigh"),
            "dayLow": quote.get("dayLow"),
            "yearHigh": quote.get("yearHigh"),
            "yearLow": quote.get("yearLow"),
            "open": quote.get("open"),
            "previousClose": quote.get("previousClose"),
            "volume": quote.get("volume"),
            "avgVolume": quote.get("avgVolume"),
            "marketCap": quote.get("marketCap"),
            "pe": quote.get("pe"),
            "eps": quote.get("eps"),
        }

    # Performance data
    if price_change_data:
        changes = price_change_data[0]
        artifact["performance"] = {
            k: changes.get(k)
            for k in ["1D", "5D", "1M", "3M", "6M", "ytd", "1Y", "3Y", "5Y"]
            if changes.get(k) is not None
        }

    # Analyst ratings
    if grades_summary_data:
        gs = grades_summary_data[0]
        artifact["analystRatings"] = {
            "strongBuy": gs.get("strongBuy", 0),
            "buy": gs.get("buy", 0),
            "hold": gs.get("hold", 0),
            "sell": gs.get("sell", 0),
            "strongSell": gs.get("strongSell", 0),
            "consensus": gs.get("consensus", "N/A"),
        }

    # Revenue by product
    if product_data and len(product_data) > 0:
        latest_product_record = product_data[0]
        if latest_product_record and isinstance(latest_product_record, dict):
            fiscal_date = list(latest_product_record.keys())[0]
            product_revenues = latest_product_record[fiscal_date]
            if product_revenues and isinstance(product_revenues, dict) and len(product_revenues) > 0:
                artifact["revenueByProduct"] = product_revenues

    # Revenue by geography
    if geo_data and len(geo_data) > 0:
        latest_geo_record = geo_data[0]
        if latest_geo_record and isinstance(latest_geo_record, dict):
            geo_date = list(latest_geo_record.keys())[0]
            geo_revenues = latest_geo_record[geo_date]
            if geo_revenues and isinstance(geo_revenues, dict) and len(geo_revenues) > 0:
                artifact["revenueByGeo"] = geo_revenues

    # Quarterly fundamentals from income statement (oldest-first for charting)
    if income_stmt:
        artifact["quarterlyFundamentals"] = [
            {
                "period": fiscal_period_lookup.get(stmt.get("date"), stmt.get("date", "")),
                "date": stmt.get("date"),
                "revenue": stmt.get("revenue"),
                "netIncome": stmt.get("netIncome"),
                "grossProfit": stmt.get("grossProfit"),
                "operatingIncome": stmt.get("operatingIncome"),
                "ebitda": stmt.get("ebitda"),
                "epsDiluted": stmt.get("epsdiluted"),
                "grossMargin": stmt.get("grossProfitRatio"),
                "operatingMargin": stmt.get("operatingIncomeRatio"),
                "netMargin": stmt.get("netIncomeRatio"),
            }
            for stmt in reversed(income_stmt)
        ]

    # Earnings surprises (reported only, oldest-first)
    reported_for_artifact = [
        e for e in earnings_calendar if e.get("eps") is not None
    ]
    if reported_for_artifact:
        artifact["earningsSurprises"] = [
            {
                "period": fiscal_period_lookup.get(
                    e.get("fiscalDateEnding"), e.get("date", "")
                ),
                "date": e.get("date"),
                "epsActual": e.get("eps"),
                "epsEstimate": e.get("epsEstimated"),
                "revenueActual": e.get("revenue"),
                "revenueEstimate": e.get("revenueEstimated"),
            }
            for e in reversed(reported_for_artifact)
        ]

    # Cash flow (oldest-first for charting)
    if cash_flow_data:
        artifact["cashFlow"] = [
            {
                "period": fiscal_period_lookup.get(cf.get("date"), cf.get("date", "")),
                "date": cf.get("date"),
                "operatingCashFlow": cf.get("operatingCashFlow"),
                "capitalExpenditure": cf.get("capitalExpenditure"),
                "freeCashFlow": cf.get("freeCashFlow"),
            }
            for cf in reversed(cash_flow_data)
        ]

    # === YFINANCE FALLBACK for missing sections ===
    # If the primary provider (FMP) returned sparse data, try YFinance
    await _yfinance_fallback_overview(symbol, artifact)

    return artifact


async def fetch_company_overview(
    symbol: str,
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch comprehensive investment analysis overview for a company.

    Retrieves and formats investment-relevant data including financial health ratings,
    analyst consensus, earnings performance, and revenue segmentation.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")
        config: LangChain RunnableConfig (injected by @tool decorator)

    Returns:
        Tuple of (content string, artifact dict with structured data for charts)
    """
    try:
        provider = await get_financial_data_provider()
        financial = provider.financial
        user_id = _get_user_id(config)
        if financial is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            content = f"""## Company Overview: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

No financial data source configured"""
            return content, {"type": "company_overview", "symbol": symbol}

        output_lines = []

        # ═══ BASIC INFORMATION ═══
        profile_data = await financial.get_company_profile(symbol)
        if not profile_data:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            content = f"""## Company Overview: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

No data found for symbol {symbol}"""
            return content, {"type": "company_overview", "symbol": symbol}

        profile = profile_data[0]
        company_name = profile.get("companyName", symbol)
        sector = profile.get("sector", "N/A")
        industry = profile.get("industry", "N/A")
        market_cap = profile.get("mktCap")
        price = profile.get("price")
        exchange = profile.get("exchangeShortName", "N/A")

        # Add file-ready header
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        output_lines.append(f"## Company Overview: {symbol}")
        output_lines.append(f"**Company:** {company_name}")
        output_lines.append(f"**Retrieved:** {timestamp}")
        output_lines.append(f"**Market:** {exchange}")
        output_lines.append("")

        output_lines.append(f"Company: {company_name} ({symbol})")
        output_lines.append(f"Sector: {sector} | Industry: {industry}")
        output_lines.append(
            f"Market Cap: {format_number(market_cap)} | Current Price: ${price:.2f}"
            if price
            else f"Market Cap: {format_number(market_cap)}"
        )
        output_lines.append("")

        # === PARALLEL DATA FETCH ===
        # Fetch all data in parallel for performance optimization
        # Build optional intel/snapshot calls
        async def _fetch_snapshot():
            """Fetch ginlix-data snapshot for real-time extended-hours data."""
            try:
                mdp = await get_market_data_provider()
                snaps = await mdp.get_snapshots([symbol], asset_type="stocks", user_id=user_id)
                return snaps[0] if snaps else None
            except Exception:
                return None

        async def _fetch_float():
            if provider.intel is None:
                return None
            return await provider.intel.get_float_shares(symbol, user_id=user_id)

        async def _fetch_short_interest():
            if provider.intel is None:
                return None
            result = await provider.intel.get_short_interest(
                symbol, limit=1, sort="settlement_date.desc", user_id=user_id,
            )
            return result[0] if result else None

        async def _fetch_short_volume():
            if provider.intel is None:
                return None
            result = await provider.intel.get_short_volume(
                symbol, limit=1, sort="date.desc", user_id=user_id,
            )
            return result[0] if result else None

        (
            income_stmt_result,
            earnings_calendar_result,
            price_change_result,
            key_metrics_result,
            ratios_result,
            filings_10q_result,
            filings_10k_result,
            price_target_consensus_result,
            grades_summary_result,
            stock_grades_result,
            price_target_summary_result,
            product_data_result,
            geo_data_result,
            quote_result,
            cash_flow_result,
            snapshot_result,
            float_result,
            short_interest_result,
            short_volume_result,
        ) = await asyncio.gather(
            financial.get_income_statements(symbol, period="quarter", limit=8),
            financial.get_earnings_history(symbol, limit=10),
            financial.get_price_performance(symbol),
            financial.get_key_metrics(symbol),
            financial.get_financial_ratios(symbol),
            _fmp_request("get_sec_filings", symbol, filing_type="10-Q", limit=3),
            _fmp_request("get_sec_filings", symbol, filing_type="10-K", limit=2),
            financial.get_analyst_price_targets(symbol),
            financial.get_analyst_ratings(symbol),
            _fmp_request("get_stock_grades", symbol, limit=10),
            _fmp_request("get_price_target_summary", symbol),
            financial.get_revenue_by_segment(
                symbol, segment_type="product", period="quarter", structure="flat"
            ),
            financial.get_revenue_by_segment(
                symbol, segment_type="geography", period="quarter", structure="flat"
            ),
            financial.get_realtime_quote(symbol),
            financial.get_cash_flows(symbol, period="quarter", limit=8),
            _fetch_snapshot(),
            _fetch_float(),
            _fetch_short_interest(),
            _fetch_short_volume(),
            return_exceptions=True,
        )

        # Extract safe results
        income_stmt = _safe_result(income_stmt_result, [])
        earnings_calendar = _safe_result(earnings_calendar_result, [])
        price_change_data = _safe_result(price_change_result, [])
        key_metrics_data = _safe_result(key_metrics_result, [])
        ratios_data = _safe_result(ratios_result, [])
        filings_10q = _safe_result(filings_10q_result, [])
        filings_10k = _safe_result(filings_10k_result, [])
        price_target_consensus = _safe_result(price_target_consensus_result, [])
        grades_summary_data = _safe_result(grades_summary_result, [])
        recent_grades = _safe_result(stock_grades_result, [])
        price_target_summary = _safe_result(price_target_summary_result, [])
        product_data = _safe_result(product_data_result, [])
        geo_data = _safe_result(geo_data_result, [])
        quote_data = _safe_result(quote_result, [])
        cash_flow_data = _safe_result(cash_flow_result, [])
        snapshot_data = _safe_result(snapshot_result, None)
        float_data = _safe_result(float_result, None)
        short_interest_data = _safe_result(short_interest_result, None)
        short_volume_data = _safe_result(short_volume_result, None)

        # Build fiscal_period_lookup using helper function
        fiscal_period_lookup = _build_fiscal_period_lookup(income_stmt)

        # === REAL-TIME QUOTE ===
        # Prefer ginlix-data snapshot (has extended-hours breakdown), fall back to FMP quote
        _has_snapshot = snapshot_data is not None and snapshot_data.get("price") is not None
        _has_fmp_quote = quote_data and len(quote_data) > 0

        if _has_snapshot or _has_fmp_quote:
            session_name, current_time_et = get_market_session()
            output_lines.append("### Real-Time Quote")

            if _has_snapshot:
                snap = snapshot_data
                # Map ginlix-data market_status to display label
                _STATUS_LABELS = {
                    "early_trading": "Pre-Market",
                    "open": "Regular Hours",
                    "late_trading": "After-Hours",
                    "closed": "Market Closed",
                }
                market_status_raw = snap.get("market_status", "")
                market_label = _STATUS_LABELS.get(market_status_raw, session_name.replace("_", " ").title())
                output_lines.append(
                    f"**Market Status:** {market_label} | **As of:** {current_time_et.strftime('%H:%M ET')}"
                )
                output_lines.append("")

                prev_close = snap.get("previous_close")
                reg_close = snap.get("price")  # session.close = regular session close
                last_price = snap.get("last_trade_price")  # actual current price

                # Regular session close with change from previous close
                reg_change = snap.get("regular_trading_change")
                reg_change_pct = snap.get("regular_trading_change_percent")

                if reg_close is not None:
                    if reg_change is not None and reg_change_pct is not None:
                        sign = "+" if reg_change >= 0 else ""
                        output_lines.append(
                            f"**Regular Close:** ${reg_close:.2f} ({sign}{reg_change:.2f} / {sign}{reg_change_pct:.3f}%)"
                        )
                    else:
                        output_lines.append(f"**Regular Close:** ${reg_close:.2f}")

                # Extended-hours current price (if different from regular close)
                is_extended = market_status_raw in ("early_trading", "late_trading")
                if is_extended and last_price is not None and reg_close is not None and last_price != reg_close:
                    ext_label = "Pre-Market" if market_status_raw == "early_trading" else "After-Hours"
                    if market_status_raw == "early_trading":
                        ext_change = snap.get("early_trading_change")
                        ext_change_pct = snap.get("early_trading_change_percent")
                    else:
                        ext_change = snap.get("late_trading_change")
                        ext_change_pct = snap.get("late_trading_change_percent")

                    if ext_change is not None and ext_change_pct is not None:
                        ext_sign = "+" if ext_change >= 0 else ""
                        output_lines.append(
                            f"**{ext_label} Price:** ${last_price:.2f} ({ext_sign}{ext_change:.2f} / {ext_sign}{ext_change_pct:.3f}% from close)"
                        )
                    else:
                        # Compute from regular close
                        diff = last_price - reg_close
                        diff_pct = (diff / reg_close * 100) if reg_close else 0
                        diff_sign = "+" if diff >= 0 else ""
                        output_lines.append(
                            f"**{ext_label} Price:** ${last_price:.2f} ({diff_sign}{diff:.2f} / {diff_sign}{diff_pct:.2f}% from close)"
                        )

                # Total day change (from previous close)
                total_change = snap.get("change")
                total_change_pct = snap.get("change_percent")
                if total_change is not None and total_change_pct is not None:
                    t_sign = "+" if total_change >= 0 else ""
                    output_lines.append(
                        f"**Day Change (from prev close):** {t_sign}{total_change:.2f} / {t_sign}{total_change_pct:.3f}%"
                    )

                output_lines.append("")

                # Build quote detail table from snapshot + FMP (FMP has 52-week range)
                quote_rows = []
                if snap.get("open"):
                    quote_rows.append(("Open", f"${snap['open']:.2f}"))
                if prev_close:
                    quote_rows.append(("Previous Close", f"${prev_close:.2f}"))
                if snap.get("low") and snap.get("high"):
                    quote_rows.append(("Day Range", f"${snap['low']:.2f} - ${snap['high']:.2f}"))
                # 52-week range from FMP quote
                fmp_quote = quote_data[0] if _has_fmp_quote else {}
                year_low = fmp_quote.get("yearLow")
                year_high = fmp_quote.get("yearHigh")
                if year_low and year_high:
                    quote_rows.append(("52-Week Range", f"${year_low:.2f} - ${year_high:.2f}"))
                if snap.get("volume"):
                    vol_str = format_number(snap["volume"]).replace("$", "")
                    avg_volume = fmp_quote.get("avgVolume") if _has_fmp_quote else None
                    if avg_volume:
                        avg_str = format_number(avg_volume).replace("$", "")
                        quote_rows.append(("Volume", f"{vol_str} (Avg: {avg_str})"))
                    else:
                        quote_rows.append(("Volume", vol_str))

                if quote_rows:
                    output_lines.append("| Metric | Value |")
                    output_lines.append("|--------|-------|")
                    for metric, value in quote_rows:
                        output_lines.append(f"| {metric} | {value} |")
                    output_lines.append("")

            else:
                # FMP-only fallback (no extended-hours breakdown available)
                quote = quote_data[0]
                session_str = session_name.replace("_", " ").title()
                output_lines.append(
                    f"**Market Status:** {session_str} | **As of:** {current_time_et.strftime('%H:%M ET')}"
                )
                output_lines.append("")

                q_price = quote.get("price", 0)
                q_change = quote.get("change", 0)
                q_change_pct = quote.get("changesPercentage", 0)
                change_sign = "+" if q_change >= 0 else ""
                output_lines.append(
                    f"**Price:** ${q_price:.2f} ({change_sign}{q_change:.2f} / {change_sign}{q_change_pct:.2f}%)"
                )
                output_lines.append("")

                quote_rows = []
                open_price = quote.get("open")
                day_low = quote.get("dayLow")
                day_high = quote.get("dayHigh")
                year_low = quote.get("yearLow")
                year_high = quote.get("yearHigh")
                volume = quote.get("volume")
                avg_volume = quote.get("avgVolume")
                previous_close = quote.get("previousClose")

                if open_price:
                    quote_rows.append(("Open", f"${open_price:.2f}"))
                if previous_close:
                    quote_rows.append(("Previous Close", f"${previous_close:.2f}"))
                if day_low and day_high:
                    quote_rows.append(("Day Range", f"${day_low:.2f} - ${day_high:.2f}"))
                if year_low and year_high:
                    quote_rows.append(("52-Week Range", f"${year_low:.2f} - ${year_high:.2f}"))
                if volume:
                    vol_str = format_number(volume).replace("$", "")
                    if avg_volume:
                        avg_str = format_number(avg_volume).replace("$", "")
                        quote_rows.append(("Volume", f"{vol_str} (Avg: {avg_str})"))
                    else:
                        quote_rows.append(("Volume", vol_str))

                if quote_rows:
                    output_lines.append("| Metric | Value |")
                    output_lines.append("|--------|-------|")
                    for metric, value in quote_rows:
                        output_lines.append(f"| {metric} | {value} |")
                    output_lines.append("")

        # === FLOAT & SHORT DATA ===
        _has_float = float_data is not None and isinstance(float_data, dict) and float_data.get("free_float") is not None
        _has_si = short_interest_data is not None and isinstance(short_interest_data, dict) and short_interest_data.get("short_interest") is not None
        _has_sv = short_volume_data is not None and isinstance(short_volume_data, dict) and short_volume_data.get("short_volume_ratio") is not None

        if _has_float or _has_si or _has_sv:
            output_lines.append("### Share Structure")
            output_lines.append("")

            struct_rows = []
            if _has_float:
                free_float = float_data.get("free_float")
                if free_float:
                    struct_rows.append(("Float", format_number(free_float).replace("$", "")))
                ff_pct = float_data.get("free_float_percent")
                if ff_pct is not None:
                    struct_rows.append(("Float %", f"{ff_pct:.1f}%"))

            if _has_si:
                si_val = short_interest_data["short_interest"]
                si_date = short_interest_data.get("settlement_date", "")
                struct_rows.append(("Short Interest", f"{si_val:,} (as of {si_date})" if si_date else f"{si_val:,}"))
                if _has_float and float_data.get("free_float"):
                    si_pct = si_val / float_data["free_float"] * 100
                    struct_rows.append(("Short % of Float", f"{si_pct:.2f}%"))
                dtc = short_interest_data.get("days_to_cover")
                if dtc:
                    struct_rows.append(("Days to Cover", f"{dtc:.2f}"))

            if _has_sv:
                sv_ratio = short_volume_data["short_volume_ratio"]
                sv_date = short_volume_data.get("date", "")
                struct_rows.append(("Short Volume Ratio", f"{sv_ratio:.1f}% (as of {sv_date})" if sv_date else f"{sv_ratio:.1f}%"))

            if struct_rows:
                output_lines.append("| Metric | Value |")
                output_lines.append("|--------|-------|")
                for metric, value in struct_rows:
                    output_lines.append(f"| {metric} | {value} |")
                output_lines.append("")

        # === STOCK PRICE PERFORMANCE ===
        if price_change_data:
            changes = price_change_data[0]

            output_lines.append("### Stock Price Performance")
            output_lines.append("")

            # Build performance table
            performance_rows = []

            # Short-term (up to 1 month)
            if changes.get("1D") is not None:
                performance_rows.append(("1 Day", format_percentage(changes.get("1D"))))
            if changes.get("5D") is not None:
                performance_rows.append(
                    ("5 Days", format_percentage(changes.get("5D")))
                )
            if changes.get("1M") is not None:
                performance_rows.append(
                    ("1 Month", format_percentage(changes.get("1M")))
                )

            # Medium-term (3-6 months)
            if changes.get("3M") is not None:
                performance_rows.append(
                    ("3 Months", format_percentage(changes.get("3M")))
                )
            if changes.get("6M") is not None:
                performance_rows.append(
                    ("6 Months", format_percentage(changes.get("6M")))
                )
            if changes.get("ytd") is not None:
                performance_rows.append(("YTD", format_percentage(changes.get("ytd"))))

            # Long-term (1+ years)
            if changes.get("1Y") is not None:
                performance_rows.append(
                    ("1 Year", format_percentage(changes.get("1Y")))
                )
            if changes.get("3Y") is not None:
                performance_rows.append(
                    ("3 Years", format_percentage(changes.get("3Y")))
                )
            if changes.get("5Y") is not None:
                performance_rows.append(
                    ("5 Years", format_percentage(changes.get("5Y")))
                )

            if performance_rows:
                output_lines.append("| Period | Performance |")
                output_lines.append("|--------|-------------|")
                for period, perf in performance_rows:
                    output_lines.append(f"| {period} | {perf} |")
                output_lines.append("")

        # === KEY FINANCIAL METRICS ===
        if key_metrics_data:
            metrics = key_metrics_data[0]
            ratios = ratios_data[0] if ratios_data else {}

            output_lines.append("### Key Financial Metrics (TTM)")
            output_lines.append("*Data based on Trailing Twelve Months*")
            output_lines.append("")

            # Collect all metrics for table
            metrics_rows = []

            # Valuation Ratios
            pe_ratio = metrics.get("peRatioTTM") or profile.get("pe")
            pb_ratio = metrics.get("pbRatioTTM")
            peg_ratio = metrics.get("pegRatioTTM")
            ev_to_ebitda = metrics.get("evToOperatingCashFlowTTM")

            if pe_ratio:
                metrics_rows.append(("P/E Ratio", f"{pe_ratio:.2f}x"))
            if pb_ratio:
                metrics_rows.append(("P/B Ratio", f"{pb_ratio:.2f}x"))
            if peg_ratio:
                metrics_rows.append(("PEG Ratio", f"{peg_ratio:.2f}"))
            if ev_to_ebitda:
                metrics_rows.append(("EV/OCF", f"{ev_to_ebitda:.2f}x"))

            # Profitability Metrics
            roe = metrics.get("roeTTM") or ratios.get("returnOnEquityTTM")
            roa = metrics.get("roaTTM") or ratios.get("returnOnAssetsTTM")
            net_margin = ratios.get("netProfitMarginTTM")
            operating_margin = ratios.get("operatingProfitMarginTTM")

            if roe:
                roe_val = f"{roe * 100:.2f}%" if roe < 1 else f"{roe:.2f}%"
                metrics_rows.append(("ROE (Return on Equity)", roe_val))
            if roa:
                roa_val = f"{roa * 100:.2f}%" if roa < 1 else f"{roa:.2f}%"
                metrics_rows.append(("ROA (Return on Assets)", roa_val))
            if net_margin:
                nm_val = (
                    f"{net_margin * 100:.2f}%"
                    if net_margin < 1
                    else f"{net_margin:.2f}%"
                )
                metrics_rows.append(("Net Profit Margin", nm_val))
            if operating_margin:
                om_val = (
                    f"{operating_margin * 100:.2f}%"
                    if operating_margin < 1
                    else f"{operating_margin:.2f}%"
                )
                metrics_rows.append(("Operating Margin", om_val))

            # Leverage & Liquidity
            debt_to_equity = ratios.get("debtEquityRatioTTM")
            current_ratio = ratios.get("currentRatioTTM")
            quick_ratio = ratios.get("quickRatioTTM")
            interest_coverage = ratios.get("interestCoverageTTM")

            if debt_to_equity:
                metrics_rows.append(("Debt/Equity Ratio", f"{debt_to_equity:.2f}"))
            if current_ratio:
                metrics_rows.append(("Current Ratio", f"{current_ratio:.2f}"))
            if quick_ratio:
                metrics_rows.append(("Quick Ratio", f"{quick_ratio:.2f}"))
            if interest_coverage:
                metrics_rows.append(("Interest Coverage", f"{interest_coverage:.2f}x"))

            # Output as markdown table
            if metrics_rows:
                output_lines.append("| Metric | Value |")
                output_lines.append("|--------|-------|")
                for metric, value in metrics_rows:
                    output_lines.append(f"| {metric} | {value} |")
            else:
                output_lines.append("*No financial metrics available*")

            output_lines.append("")

        # === SEC FILING DATES ===
        has_filing_data = bool(filings_10q or filings_10k)

        if has_filing_data:
            output_lines.append("### SEC Filing Dates")
            output_lines.append("")

            output_lines.append("| Filing Type | Filing Date | Fiscal Period |")
            output_lines.append("|-------------|-------------|---------------|")

            # Show latest 10-K (annual report that includes Q4)
            if filings_10k:
                for filing in filings_10k[:1]:  # Just the latest
                    filing_date = filing.get("fillingDate", "N/A")
                    if filing_date and " " in filing_date:
                        filing_date = filing_date.split(" ")[0]  # Remove time part

                    # For 10-K, find Q4 fiscal period (10-K includes Q4)
                    fiscal_period = "Annual"
                    if fiscal_period_lookup:
                        # Find Q4 entries to determine fiscal year
                        for date_key, period_name in sorted(
                            fiscal_period_lookup.items(), reverse=True
                        ):
                            if period_name.startswith("Q4"):
                                # Extract FY from "Q4 FY2025" and show as "Q4 FY2025 (Annual)"
                                fiscal_period = f"{period_name} (Annual)"
                                break

                    output_lines.append(
                        f"| **10-K** | {filing_date} | {fiscal_period} |"
                    )

            # Show latest 10-Q filings
            if filings_10q:
                for filing in filings_10q[:3]:  # Last 3 quarterly reports
                    filing_date = filing.get("fillingDate", "N/A")
                    if filing_date and " " in filing_date:
                        filing_date = filing_date.split(" ")[0]

                    # Match filing to fiscal period using helper
                    fiscal_period = _match_filing_to_fiscal_period(
                        filing_date, earnings_calendar, fiscal_period_lookup
                    )
                    output_lines.append(
                        f"| **10-Q** (Quarterly) | {filing_date} | {fiscal_period} |"
                    )

            output_lines.append("")

            # Add tip for US stocks about get_sec_filing tool
            # US stocks don't have exchange suffix (.SS, .SZ, .HK, etc.)
            is_us_stock = "." not in symbol or symbol.endswith(".US")
            if is_us_stock:
                output_lines.append(
                    "*Tip: Use `get_sec_filing` tool to fetch complete earnings call transcripts and SEC filings.*"
                )
                output_lines.append("")

        # === NEXT EARNINGS REPORT ===
        if earnings_calendar:
            # Find upcoming reports (eps is None) and pick the earliest one
            upcoming_reports = [
                cal
                for cal in earnings_calendar
                if cal.get("eps") is None and cal.get("date")
            ]

            if upcoming_reports:
                upcoming_reports.sort(key=lambda x: x.get("date", "9999-99-99"))
                next_report = upcoming_reports[0]

                output_lines.append("### Next Earnings Report")
                output_lines.append("")

                report_date = next_report.get("date", "N/A")
                fiscal_ending = next_report.get("fiscalDateEnding", "N/A")
                time_slot = next_report.get("time", "")
                eps_estimate = next_report.get("epsEstimated")
                rev_estimate = next_report.get("revenueEstimated")

                # Determine fiscal period name (lookup first, then infer)
                fiscal_period_name = fiscal_period_lookup.get(fiscal_ending)
                if not fiscal_period_name and fiscal_ending != "N/A":
                    fiscal_period_name = _infer_fiscal_period(
                        fiscal_ending, fiscal_period_lookup
                    )
                fiscal_period_name = fiscal_period_name or "N/A"

                # Format time slot
                time_desc = {
                    "amc": " (After Market Close)",
                    "bmo": " (Before Market Open)",
                }.get(time_slot, "")

                output_lines.append(f"**Report Date:** {report_date}{time_desc}")
                output_lines.append(f"**Fiscal Period:** {fiscal_period_name}")
                output_lines.append(f"**Fiscal Period End:** {fiscal_ending}")

                if eps_estimate is not None:
                    output_lines.append(f"**EPS Estimate:** ${eps_estimate:.2f}")
                if rev_estimate is not None:
                    output_lines.append(
                        f"**Revenue Estimate:** {format_number(rev_estimate)}"
                    )

                output_lines.append("")

        # === EARNINGS PERFORMANCE ===
        # Filter to get reported quarters only (eps is not None means already reported)
        reported_earnings = [e for e in earnings_calendar if e.get("eps") is not None]

        if reported_earnings:
            output_lines.append("### Earnings Performance")
            output_lines.append("")

            # Show latest quarter in detail
            latest = reported_earnings[0]
            announce_date = latest.get("date", "N/A")
            fiscal_ending = latest.get("fiscalDateEnding")
            eps_actual = latest.get("eps")
            eps_estimate = latest.get("epsEstimated")
            revenue_actual = latest.get("revenue")
            revenue_estimate = latest.get("revenueEstimated")

            # Get fiscal period label
            fiscal_label = (
                fiscal_period_lookup.get(fiscal_ending, "") if fiscal_ending else ""
            )
            latest_label = (
                f"{announce_date} ({fiscal_label})" if fiscal_label else announce_date
            )

            output_lines.append(f"**Latest Quarter ({latest_label}):**")
            output_lines.append("")

            # EPS data
            if eps_actual is not None:
                if eps_estimate and eps_estimate != 0:
                    eps_surprise = (
                        (eps_actual - eps_estimate) / abs(eps_estimate)
                    ) * 100
                    output_lines.append(
                        f"- **EPS:** ${eps_actual:.2f} actual vs ${eps_estimate:.2f} estimate ({format_percentage(eps_surprise)} surprise)"
                    )
                else:
                    output_lines.append(
                        f"- **EPS:** ${eps_actual:.2f} (no estimate available)"
                    )

            # Revenue data
            if revenue_actual is not None:
                if revenue_estimate and revenue_estimate != 0:
                    rev_surprise = (
                        (revenue_actual - revenue_estimate) / abs(revenue_estimate)
                    ) * 100
                    output_lines.append(
                        f"- **Revenue:** {format_number(revenue_actual)} actual vs {format_number(revenue_estimate)} estimate ({format_percentage(rev_surprise)} surprise)"
                    )
                else:
                    output_lines.append(
                        f"- **Revenue:** {format_number(revenue_actual)} (no estimate available)"
                    )

            # Show earnings trend for last 4 quarters with fiscal period column
            if len(reported_earnings) > 1:
                output_lines.append("")
                output_lines.append("**Recent Earnings Trend:**")
                output_lines.append("")
                output_lines.append("| Date | Fiscal Period | EPS | Revenue |")
                output_lines.append("|------|---------------|-----|---------|")

                for quarter in reported_earnings[:4]:
                    q_date = quarter.get("date", "N/A")
                    q_fiscal_ending = quarter.get("fiscalDateEnding")
                    q_eps = quarter.get("eps")
                    q_revenue = quarter.get("revenue")

                    # Get fiscal period label
                    q_fiscal_label = (
                        fiscal_period_lookup.get(q_fiscal_ending, "N/A")
                        if q_fiscal_ending
                        else "N/A"
                    )
                    eps_str = f"${q_eps:.2f}" if q_eps is not None else "N/A"
                    revenue_str = (
                        format_number(q_revenue) if q_revenue is not None else "N/A"
                    )
                    output_lines.append(
                        f"| {q_date} | {q_fiscal_label} | {eps_str} | {revenue_str} |"
                    )

            output_lines.append("")

        # === CASH FLOW (QUARTERLY) ===
        if cash_flow_data:
            output_lines.append("### Cash Flow (Quarterly)")
            output_lines.append("")
            output_lines.append("| Period | Operating CF | CapEx | Free CF |")
            output_lines.append("|--------|-------------|-------|---------|")

            for cf in cash_flow_data[:8]:
                cf_date = cf.get("date", "N/A")
                cf_label = fiscal_period_lookup.get(cf_date, cf_date)
                op_cf = cf.get("operatingCashFlow")
                capex = cf.get("capitalExpenditure")
                fcf = cf.get("freeCashFlow")
                op_cf_str = format_number(op_cf) if op_cf is not None else "N/A"
                capex_str = format_number(capex) if capex is not None else "N/A"
                fcf_str = format_number(fcf) if fcf is not None else "N/A"
                output_lines.append(
                    f"| {cf_label} | {op_cf_str} | {capex_str} | {fcf_str} |"
                )

            output_lines.append("")

        # === ANALYST CONSENSUS & RATINGS ===
        output_lines.append("### Analyst Consensus & Ratings")
        output_lines.append("")

        # Price Targets Section
        if price_target_consensus:
            pt = price_target_consensus[0]
            median = pt.get("targetMedian")
            low = pt.get("targetLow")
            high = pt.get("targetHigh")
            consensus = pt.get("targetConsensus")

            output_lines.append("**Price Targets:**")
            output_lines.append("")
            pt_rows = []
            if median and price:
                upside = ((median - price) / price * 100) if price else 0
                upside_sign = "+" if upside >= 0 else ""
                pt_rows.append(
                    (
                        "Consensus Target",
                        f"${median:.2f} ({upside_sign}{upside:.1f}% from current)",
                    )
                )
            if low and high:
                pt_rows.append(("Target Range", f"${low:.2f} - ${high:.2f}"))
            if consensus:
                pt_rows.append(("Analyst Consensus", str(consensus)))

            if pt_rows:
                for label, value in pt_rows:
                    output_lines.append(f"- **{label}:** {value}")
                output_lines.append("")

        # Rating Distribution
        if grades_summary_data:
            gs = grades_summary_data[0]
            strong_buy = gs.get("strongBuy", 0)
            buy = gs.get("buy", 0)
            hold = gs.get("hold", 0)
            sell = gs.get("sell", 0)
            strong_sell = gs.get("strongSell", 0)
            consensus = gs.get("consensus", "N/A")

            total_ratings = strong_buy + buy + hold + sell + strong_sell
            if total_ratings > 0:
                output_lines.append("**Rating Distribution:**")
                output_lines.append("")
                output_lines.append("| Rating | Count | Percentage |")
                output_lines.append("|--------|-------|------------|")

                if strong_buy > 0:
                    pct = strong_buy / total_ratings * 100
                    output_lines.append(f"| Strong Buy | {strong_buy} | {pct:.1f}% |")
                if buy > 0:
                    pct = buy / total_ratings * 100
                    output_lines.append(f"| Buy | {buy} | {pct:.1f}% |")
                if hold > 0:
                    pct = hold / total_ratings * 100
                    output_lines.append(f"| Hold | {hold} | {pct:.1f}% |")
                if sell > 0:
                    pct = sell / total_ratings * 100
                    output_lines.append(f"| Sell | {sell} | {pct:.1f}% |")
                if strong_sell > 0:
                    pct = strong_sell / total_ratings * 100
                    output_lines.append(f"| Strong Sell | {strong_sell} | {pct:.1f}% |")

                output_lines.append("")
                output_lines.append(f"**Overall Consensus:** {consensus.upper()}")
                output_lines.append("")

        # Recent Analyst Actions
        if recent_grades:
            output_lines.append("**Recent Analyst Actions:**")
            output_lines.append("")
            output_lines.append("| Date | Firm | Action |")
            output_lines.append("|------|------|--------|")

            for grade in recent_grades[:5]:  # Show top 5 recent actions
                company = grade.get("gradingCompany", "N/A")
                new_grade = grade.get("newGrade", "N/A")
                previous_grade = grade.get("previousGrade", "")
                action = grade.get("action", "N/A")
                date = grade.get("date", "N/A")

                # Format action string
                if previous_grade and previous_grade != new_grade:
                    action_str = f"{action} to {new_grade} (from {previous_grade})"
                else:
                    action_str = f"{action} {new_grade}"

                output_lines.append(f"| {date} | {company} | {action_str} |")

            output_lines.append("")

        # Top Analyst Firms (from price target summary)
        if price_target_summary:
            output_lines.append("**Top Analyst Firms:**")
            output_lines.append("")
            output_lines.append("| Firm | Analyst | Price Target |")
            output_lines.append("|------|---------|--------------|")

            for firm_target in price_target_summary[:5]:
                analyst_company = firm_target.get("analystCompany", "N/A")
                target_price = firm_target.get("adjPriceTarget")
                analyst_name = firm_target.get("analystName", "-")

                target_str = f"${target_price:.2f}" if target_price else "N/A"
                output_lines.append(
                    f"| {analyst_company} | {analyst_name} | {target_str} |"
                )

            output_lines.append("")

        # === REVENUE BREAKDOWN ===
        has_product_data = False
        has_geo_data = False

        # Check if we have any data
        if product_data and len(product_data) > 0:
            latest_product_record = product_data[0]
            # Extract date and nested data (structure: {"2024-09-28": {"Mac": 123, ...}})
            if latest_product_record and isinstance(latest_product_record, dict):
                fiscal_date = list(latest_product_record.keys())[0]
                product_revenues = latest_product_record[fiscal_date]
                if (
                    product_revenues
                    and isinstance(product_revenues, dict)
                    and len(product_revenues) > 0
                ):
                    has_product_data = True

        if geo_data and len(geo_data) > 0:
            latest_geo_record = geo_data[0]
            # Extract date and nested data
            if latest_geo_record and isinstance(latest_geo_record, dict):
                geo_date = list(latest_geo_record.keys())[0]
                geo_revenues = latest_geo_record[geo_date]
                if (
                    geo_revenues
                    and isinstance(geo_revenues, dict)
                    and len(geo_revenues) > 0
                ):
                    has_geo_data = True

        # Only show section if we have data
        if has_product_data or has_geo_data:
            output_lines.append("### Revenue Breakdown (Latest Quarter)")
            output_lines.append("")

        # Product breakdown
        if has_product_data:
            latest_product_record = product_data[0]
            fiscal_date = list(latest_product_record.keys())[0]
            product_revenues = latest_product_record[fiscal_date]

            # Get fiscal period name from lookup
            period_label = fiscal_period_lookup.get(
                fiscal_date, f"Period ending {fiscal_date}"
            )
            output_lines.append(f"**By Product ({period_label}):**")
            output_lines.append(f"*Report Date: {fiscal_date}*")
            output_lines.append("")

            total_revenue = sum(product_revenues.values())

            # Sort by revenue (descending) and show top items
            sorted_products = sorted(
                product_revenues.items(), key=lambda x: x[1], reverse=True
            )
            output_lines.append("| Product | Revenue | Percentage |")
            output_lines.append("|---------|---------|------------|")
            for product, revenue in sorted_products[:5]:  # Top 5 products
                percentage = (revenue / total_revenue * 100) if total_revenue > 0 else 0
                output_lines.append(
                    f"| {product} | {format_number(revenue)} | {percentage:.1f}% |"
                )

            output_lines.append("")

        # Geographic breakdown
        if has_geo_data:
            latest_geo_record = geo_data[0]
            geo_date = list(latest_geo_record.keys())[0]
            geo_revenues = latest_geo_record[geo_date]

            # Get fiscal period name from lookup
            period_label = fiscal_period_lookup.get(
                geo_date, f"Period ending {geo_date}"
            )
            output_lines.append(f"**By Region ({period_label}):**")
            output_lines.append(f"*Report Date: {geo_date}*")
            output_lines.append("")

            total_revenue = sum(geo_revenues.values())

            # Sort by revenue (descending)
            sorted_regions = sorted(
                geo_revenues.items(), key=lambda x: x[1], reverse=True
            )
            output_lines.append("| Region | Revenue | Percentage |")
            output_lines.append("|--------|---------|------------|")
            for region, revenue in sorted_regions:
                percentage = (revenue / total_revenue * 100) if total_revenue > 0 else 0
                output_lines.append(
                    f"| {region} | {format_number(revenue)} | {percentage:.1f}% |"
                )

            output_lines.append("")

        result = "\n".join(output_lines)
        logger.debug(f"Retrieved comprehensive investment overview for {symbol}")

        # Build artifact with structured data for frontend charts
        artifact: Dict[str, Any] = {
            "type": "company_overview",
            "symbol": symbol,
            "name": company_name,
        }

        # Quote data for artifact — prefer snapshot for extended-hours detail
        if _has_snapshot:
            snap = snapshot_data
            fmp_quote = quote_data[0] if _has_fmp_quote else {}
            artifact["quote"] = {
                "regularClose": snap.get("price"),
                "lastTradePrice": snap.get("last_trade_price"),
                "marketStatus": snap.get("market_status"),
                "change": snap.get("change"),
                "changePct": snap.get("change_percent"),
                "regularChange": snap.get("regular_trading_change"),
                "regularChangePct": snap.get("regular_trading_change_percent"),
                "earlyTradingChangePct": snap.get("early_trading_change_percent"),
                "lateTradingChangePct": snap.get("late_trading_change_percent"),
                "dayHigh": snap.get("high"),
                "dayLow": snap.get("low"),
                "yearHigh": fmp_quote.get("yearHigh"),
                "yearLow": fmp_quote.get("yearLow"),
                "open": snap.get("open"),
                "previousClose": snap.get("previous_close"),
                "volume": snap.get("volume"),
                "avgVolume": fmp_quote.get("avgVolume"),
                "marketCap": fmp_quote.get("marketCap"),
            }
        elif _has_fmp_quote:
            quote = quote_data[0]
            artifact["quote"] = {
                "price": quote.get("price"),
                "change": quote.get("change"),
                "changePct": quote.get("changesPercentage"),
                "dayHigh": quote.get("dayHigh"),
                "dayLow": quote.get("dayLow"),
                "yearHigh": quote.get("yearHigh"),
                "yearLow": quote.get("yearLow"),
                "open": quote.get("open"),
                "previousClose": quote.get("previousClose"),
                "volume": quote.get("volume"),
                "avgVolume": quote.get("avgVolume"),
                "marketCap": quote.get("marketCap"),
            }

        # Float & short data for artifact (single latest records, not full arrays)
        if _has_float:
            artifact["float"] = float_data
        if _has_si:
            artifact["shortInterest"] = short_interest_data
        if _has_sv:
            artifact["shortVolume"] = short_volume_data

        # Performance data
        if price_change_data:
            changes = price_change_data[0]
            artifact["performance"] = {
                k: changes.get(k)
                for k in ["1D", "5D", "1M", "3M", "6M", "ytd", "1Y", "3Y", "5Y"]
                if changes.get(k) is not None
            }

        # Analyst ratings
        if grades_summary_data:
            gs = grades_summary_data[0]
            artifact["analystRatings"] = {
                "strongBuy": gs.get("strongBuy", 0),
                "buy": gs.get("buy", 0),
                "hold": gs.get("hold", 0),
                "sell": gs.get("sell", 0),
                "strongSell": gs.get("strongSell", 0),
                "consensus": gs.get("consensus", "N/A"),
            }

        # Revenue by product
        if has_product_data:
            latest_product_record = product_data[0]
            fiscal_date = list(latest_product_record.keys())[0]
            artifact["revenueByProduct"] = latest_product_record[fiscal_date]

        # Revenue by geography
        if has_geo_data:
            latest_geo_record = geo_data[0]
            geo_date = list(latest_geo_record.keys())[0]
            artifact["revenueByGeo"] = latest_geo_record[geo_date]

        # Quarterly fundamentals from income statement (oldest-first for charting)
        if income_stmt:
            artifact["quarterlyFundamentals"] = [
                {
                    "period": fiscal_period_lookup.get(stmt.get("date"), stmt.get("date", "")),
                    "date": stmt.get("date"),
                    "revenue": stmt.get("revenue"),
                    "netIncome": stmt.get("netIncome"),
                    "grossProfit": stmt.get("grossProfit"),
                    "operatingIncome": stmt.get("operatingIncome"),
                    "ebitda": stmt.get("ebitda"),
                    "epsDiluted": stmt.get("epsdiluted"),
                    "grossMargin": stmt.get("grossProfitRatio"),
                    "operatingMargin": stmt.get("operatingIncomeRatio"),
                    "netMargin": stmt.get("netIncomeRatio"),
                }
                for stmt in reversed(income_stmt)
            ]

        # Earnings surprises from earnings calendar (reported only, oldest-first)
        reported_for_artifact = [
            e for e in earnings_calendar if e.get("eps") is not None
        ]
        if reported_for_artifact:
            artifact["earningsSurprises"] = [
                {
                    "period": fiscal_period_lookup.get(
                        e.get("fiscalDateEnding"), e.get("date", "")
                    ),
                    "date": e.get("date"),
                    "epsActual": e.get("eps"),
                    "epsEstimate": e.get("epsEstimated"),
                    "revenueActual": e.get("revenue"),
                    "revenueEstimate": e.get("revenueEstimated"),
                }
                for e in reversed(reported_for_artifact)
            ]

        # Cash flow (oldest-first for charting)
        if cash_flow_data:
            artifact["cashFlow"] = [
                {
                    "period": fiscal_period_lookup.get(cf.get("date"), cf.get("date", "")),
                    "date": cf.get("date"),
                    "operatingCashFlow": cf.get("operatingCashFlow"),
                    "capitalExpenditure": cf.get("capitalExpenditure"),
                    "freeCashFlow": cf.get("freeCashFlow"),
                }
                for cf in reversed(cash_flow_data)
            ]

        return result, artifact

    except Exception as e:
        logger.error(f"Error retrieving company overview for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        content = f"""## Company Overview: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving company overview: {str(e)}"""
        return content, {"type": "company_overview", "symbol": symbol, "error": str(e)}


async def fetch_market_indices(
    indices: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 60,
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch market indices data (S&P 500, NASDAQ, Dow Jones).

    For periods < 14 trading days: Returns markdown tables with OHLCV data per index
    For periods >= 14 trading days: Returns formatted summary with sections per index

    Args:
        indices: List of index symbols, default is major US indices
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Number of records per index (default 60)
        config: LangChain RunnableConfig (injected by @tool decorator)

    Returns:
        Tuple of (content string, artifact dict with structured data for charts)
    """
    try:
        provider = await get_market_data_provider()
        user_id = _get_user_id(config)

        # Default indices if not specified
        if indices is None:
            indices = ["^GSPC", "^IXIC", "^DJI", "^RUT"]

        # Calculate date range once (outside the loop)
        if start_date or end_date:
            fetch_start = start_date
            fetch_end = end_date
            apply_limit = False
        else:
            end = datetime.now().date()
            days_back = int(limit * 1.5)  # Buffer for weekends/holidays
            start = end - timedelta(days=days_back)
            fetch_start = start.isoformat()
            fetch_end = end.isoformat()
            apply_limit = True

        async def fetch_single_index(index_symbol: str):
            """Fetch data for a single index with error handling."""
            try:
                raw_bars = await provider.get_daily(
                    index_symbol, from_date=fetch_start, to_date=fetch_end,
                    is_index=True, user_id=user_id,
                )
                index_data = _normalize_market_bars(raw_bars, index_symbol)
                # Apply limit if not using explicit date range (newest-first)
                if apply_limit and index_data and len(index_data) > limit:
                    index_data = index_data[:limit]
                return (index_symbol, index_data)
            except Exception as e:
                logger.warning(f"Error fetching data for index {index_symbol}: {e}")
                return (index_symbol, None)

        # Fetch all indices in parallel
        results = await asyncio.gather(*[fetch_single_index(sym) for sym in indices])

        # Process results
        indices_data = {}
        all_results = []
        for index_symbol, index_data in results:
            if index_data:
                indices_data[index_symbol] = index_data
                all_results.extend(index_data)

        if not all_results:
            logger.warning(f"No index data found for {indices}")
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            indices_str = (
                ", ".join(indices[:3])
                if len(indices) <= 3
                else f"{', '.join(indices[:3])} and {len(indices) - 3} more"
            )
            content = f"""## Market Indices: {indices_str}
**Retrieved:** {timestamp}
**Status:** No data available

No index data available for the specified period."""
            return content, {"type": "market_indices", "indices": {}}

        # Determine if we should normalize based on limit/date range
        # For date ranges, estimate number of days
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            calendar_days = (end_dt - start_dt).days
            # Rough estimate: 252 trading days per 365 calendar days
            estimated_trading_days = int(calendar_days * 252 / 365)
            should_normalize = estimated_trading_days >= 14
        else:
            # Use limit directly
            should_normalize = limit >= 14

        # Find actual date range from data
        all_dates = [d.get("date") for d in all_results if d.get("date")]
        if all_dates:
            all_dates_sorted = sorted(all_dates)
            actual_start = all_dates_sorted[0]
            actual_end = all_dates_sorted[-1]
            # Count unique trading days
            unique_days = len(set(all_dates))
        else:
            actual_start = start_date or "N/A"
            actual_end = end_date or "N/A"
            unique_days = limit

        # Generate file-ready header
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        indices_str = (
            ", ".join(indices[:3])
            if len(indices) <= 3
            else f"{', '.join(indices[:3])} and {len(indices) - 3} more"
        )

        if start_date and end_date:
            title = f"Market Indices: {indices_str} ({start_date} to {end_date})"
        elif actual_start != "N/A" and actual_end != "N/A":
            title = f"Market Indices: {indices_str} ({actual_start} to {actual_end})"
        else:
            title = f"Market Indices: {indices_str}"

        header = f"""## {title}
**Retrieved:** {timestamp}
**Market:** US Stock Indices
**Period:** {actual_start} to {actual_end}
**Data Points:** {unique_days} trading days
**Indices:** {len(indices_data)} indices

"""

        # Determine appropriate intraday interval based on period length
        intraday_interval = None
        if unique_days <= 5:
            intraday_interval = "5min"
        elif unique_days <= 20:
            intraday_interval = "1hour"
        elif unique_days <= 60:
            intraday_interval = "4hour"

        # Fetch intraday data for all indices in parallel if applicable
        intraday_map = {}
        if intraday_interval and actual_start != "N/A" and actual_end != "N/A":
            async def fetch_intraday(sym):
                try:
                    raw = await provider.get_intraday(
                        sym,
                        interval=intraday_interval,
                        from_date=actual_start,
                        to_date=actual_end,
                        is_index=True,
                        user_id=user_id,
                    )
                    return (sym, _normalize_market_bars(raw, sym, datetime_format=True))
                except Exception as e:
                    logger.warning(f"Failed to fetch intraday for index {sym}: {e}")
                    return (sym, None)

            intraday_results = await asyncio.gather(
                *[fetch_intraday(sym) for sym in indices_data.keys()]
            )
            for sym, idata in intraday_results:
                if idata and len(idata) > 5:
                    intraday_map[sym] = idata

        # Build artifact with structured data per index
        artifact_indices = {}
        for idx_symbol, idx_data in indices_data.items():
            sorted_for_chart = sorted(
                idx_data, key=lambda x: x.get("date", ""), reverse=False
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

            # Build chart_ohlcv from intraday if available
            chart_ohlcv = ohlcv
            chart_interval = "daily"
            if idx_symbol in intraday_map:
                intraday_sorted = sorted(
                    intraday_map[idx_symbol],
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

            idx_stats = _calculate_price_statistics(idx_data)
            artifact_indices[idx_symbol] = {
                "name": _get_index_name(idx_symbol),
                "ohlcv": ohlcv,
                "chart_ohlcv": chart_ohlcv,
                "chart_interval": chart_interval,
                "stats": {
                    "period_change_pct": idx_stats.get("period_change_pct"),
                    "ma_20": idx_stats.get("ma_20"),
                    "ma_50": idx_stats.get("ma_50"),
                    "volatility": idx_stats.get("volatility"),
                },
            }

        artifact = {"type": "market_indices", "indices": artifact_indices}

        if should_normalize and indices_data:
            # Return normalized summary
            period_info = {
                "num_days": unique_days,
                "start_date": actual_start,
                "end_date": actual_end,
            }

            logger.debug(
                f"Retrieved {len(all_results)} records for {len(indices)} indices, returning normalized summary"
            )
            return header + _format_indices_summary(indices_data, period_info), artifact
        else:
            # Return markdown tables for short periods
            logger.debug(
                f"Retrieved {len(all_results)} records for {len(indices)} indices, returning markdown tables"
            )
            return header + _format_indices_data_as_table(indices_data), artifact

    except Exception as e:
        logger.error(f"Error retrieving market indices: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        indices_str = (
            ", ".join(indices[:3])
            if len(indices) <= 3
            else f"{', '.join(indices[:3])} and {len(indices) - 3} more"
        )
        content = f"""## Market Indices: {indices_str}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving index data: {str(e)}"""
        return content, {"type": "market_indices", "indices": {}, "error": str(e)}


async def fetch_sector_performance(
    date: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch market sector performance.

    Args:
        date: Analysis date in YYYY-MM-DD format (default: latest available)

    Returns:
        Tuple of (content string, artifact dict with structured data for charts)
    """

    def _build_sector_artifact(
        raw_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build structured artifact from raw sector results."""
        sectors = []
        for sector in raw_results:
            sector_name = sector.get("sector", "N/A")
            change_str = sector.get("changesPercentage", "0%")
            try:
                change_val = float(change_str.replace("%", "").replace("+", ""))
            except (ValueError, AttributeError):
                change_val = 0.0
            sectors.append({"sector": sector_name, "changesPercentage": change_val})
        # Sort descending by performance
        sectors.sort(key=lambda x: x["changesPercentage"], reverse=True)
        return {"type": "sector_performance", "sectors": sectors}

    try:
        provider = await get_financial_data_provider()

        # Generate file-ready header
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        date_str = f" ({date})" if date else ""
        header = f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Market:** US Stock Market

"""

        # Try protocol-based provider first
        if provider.financial is not None:
            try:
                results = await provider.financial.get_sector_performance()
                if results:
                    logger.debug(f"Retrieved performance data for {len(results)} sectors")
                    content = header + _format_sectors_as_table(results)
                    return content, _build_sector_artifact(results)
            except Exception:
                pass

        # Fallback to stable version with date if provided
        if date:
            try:
                results = await _fmp_request(
                    "_make_request",
                    "sector-performance-snapshot",
                    params={"date": date},
                    version="stable",
                )
                if results:
                    logger.debug(f"Retrieved performance data for {len(results)} sectors")
                    content = header + _format_sectors_as_table(results)
                    return content, _build_sector_artifact(results)
            except Exception:
                pass

        logger.warning(
            "No sector performance data found - endpoint may not be available on this FMP plan"
        )
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        content = f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Status:** No data available

No sector performance data available for the specified period."""
        return content, {"type": "sector_performance", "sectors": []}

    except Exception as e:
        logger.error(f"Error retrieving sector performance: {e}")
        logger.warning("Sector performance endpoint may require a higher FMP API tier")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        date_str = f" ({date})" if date else ""
        content = f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving sector performance data: {str(e)}"""
        return content, {
            "type": "sector_performance",
            "sectors": [],
            "error": str(e),
        }


async def fetch_earnings_transcript(symbol: str, year: int, quarter: int) -> str:
    """
    Fetch earnings call transcript.

    Retrieves the full transcript of a company's earnings call, formatted for
    easy reading and analysis of management's communication about financial
    performance, future plans, and strategy.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")
        year: Fiscal year (e.g., 2020) - REQUIRED
        quarter: Fiscal quarter (1, 2, 3, or 4) - REQUIRED

    Returns:
        Formatted string with earnings call transcript
    """
    try:
        output_lines = []

        # Fetch transcript data (FMP-specific, not in generic protocol)
        transcript_data = await _fmp_request(
            "get_earnings_call_transcript", symbol=symbol, year=year, quarter=quarter
        )

        if not transcript_data or len(transcript_data) == 0:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            return f"""## Earnings Transcript: {symbol} Q{quarter} {year}
**Retrieved:** {timestamp}
**Status:** No data available

No earnings transcript found for {symbol} Q{quarter} {year}"""

        transcript = transcript_data[0]

        # Extract metadata
        company_symbol = transcript.get("symbol", symbol)
        period = transcript.get("period", "N/A")
        fiscal_year = transcript.get("year", "N/A")
        call_date = transcript.get("date", "N/A")
        content = transcript.get("content", "")

        # Add file-ready header
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        output_lines.append(f"## Earnings Transcript: {symbol} Q{quarter} {year}")
        output_lines.append(f"**Retrieved:** {timestamp}")
        output_lines.append(f"**Fiscal Period:** {period} {fiscal_year}")
        output_lines.append(f"**Call Date:** {call_date}")
        output_lines.append("")

        # Header section
        output_lines.append(f"Earnings Call Transcript: {company_symbol}")
        output_lines.append("═" * 70)
        output_lines.append(f"Fiscal Period: {period} {fiscal_year}")
        output_lines.append(f"Call Date: {call_date}")
        output_lines.append("═" * 70)
        output_lines.append("")

        # Add transcript content
        if content:
            # Split content into lines for better formatting
            content_lines = content.split("\n")

            # If content is very long, provide full transcript
            # (LLMs can handle large context, and users want full analysis capability)
            output_lines.append("Transcript Content:")
            output_lines.append("")
            output_lines.append("```text")
            output_lines.extend(content_lines)
            output_lines.append("```")
            output_lines.append("")

            # Add transcript stats
            word_count = len(content.split())
            char_count = len(content)
            output_lines.append("Transcript Statistics:")
            output_lines.append(f"├─ Words: {word_count:,}")
            output_lines.append(f"└─ Characters: {char_count:,}")
        else:
            output_lines.append("Note: Transcript content is empty or not available.")

        result = "\n".join(output_lines)
        logger.debug(
            f"Retrieved earnings transcript for {symbol} {period} {fiscal_year}"
        )
        return result

    except Exception as e:
        logger.error(f"Error retrieving earnings transcript for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return f"""## Earnings Transcript: {symbol} Q{quarter} {year}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving earnings transcript: {str(e)}"""


# ─── Stock Screener ──────────────────────────────────────────────────

# Mapping from snake_case parameter names to FMP camelCase API params
_SCREENER_PARAM_MAP = {
    "market_cap_more_than": "marketCapMoreThan",
    "market_cap_lower_than": "marketCapLowerThan",
    "price_more_than": "priceMoreThan",
    "price_lower_than": "priceLowerThan",
    "volume_more_than": "volumeMoreThan",
    "volume_lower_than": "volumeLowerThan",
    "beta_more_than": "betaMoreThan",
    "beta_lower_than": "betaLowerThan",
    "dividend_more_than": "dividendMoreThan",
    "dividend_lower_than": "dividendLowerThan",
    "is_etf": "isEtf",
    "is_fund": "isFund",
    "is_actively_trading": "isActivelyTrading",
}


async def fetch_stock_screener(
    market_cap_more_than: Optional[float] = None,
    market_cap_lower_than: Optional[float] = None,
    price_more_than: Optional[float] = None,
    price_lower_than: Optional[float] = None,
    volume_more_than: Optional[float] = None,
    volume_lower_than: Optional[float] = None,
    beta_more_than: Optional[float] = None,
    beta_lower_than: Optional[float] = None,
    dividend_more_than: Optional[float] = None,
    dividend_lower_than: Optional[float] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    exchange: Optional[str] = None,
    country: Optional[str] = None,
    is_etf: Optional[bool] = None,
    is_fund: Optional[bool] = None,
    is_actively_trading: Optional[bool] = None,
    limit: int = 50,
) -> Tuple[str, Dict[str, Any]]:
    """
    Screen stocks using FMP company screener API.

    Returns:
        Tuple of (markdown content, artifact dict for frontend rendering)
    """
    try:
        provider = await get_financial_data_provider()
        financial = provider.financial

        # Build API params with camelCase conversion
        local_params = {
            "market_cap_more_than": market_cap_more_than,
            "market_cap_lower_than": market_cap_lower_than,
            "price_more_than": price_more_than,
            "price_lower_than": price_lower_than,
            "volume_more_than": volume_more_than,
            "volume_lower_than": volume_lower_than,
            "beta_more_than": beta_more_than,
            "beta_lower_than": beta_lower_than,
            "dividend_more_than": dividend_more_than,
            "dividend_lower_than": dividend_lower_than,
            "is_etf": is_etf,
            "is_fund": is_fund,
            "is_actively_trading": is_actively_trading,
        }
        api_params = {}
        for snake_key, value in local_params.items():
            if value is not None:
                camel_key = _SCREENER_PARAM_MAP.get(snake_key, snake_key)
                api_params[camel_key] = value

        # String params pass through directly
        if sector:
            api_params["sector"] = sector
        if industry:
            api_params["industry"] = industry
        if exchange:
            api_params["exchange"] = exchange
        if country:
            api_params["country"] = country
        if limit:
            api_params["limit"] = limit

        if financial is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            return (
                f"## Stock Screener\n**Retrieved:** {timestamp}\n\nNo financial data source configured.",
                {"type": "stock_screener", "results": [], "filters": {}, "count": 0},
            )

        results = await financial.screen_stocks(**api_params)

        if not results or not isinstance(results, list):
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            return (
                f"## Stock Screener Results\n**Retrieved:** {timestamp}\n\nNo stocks matched the given criteria.",
                {"type": "stock_screener", "results": [], "filters": api_params, "count": 0},
            )

        # Build active filters summary for display
        active_filters = {}
        if sector:
            active_filters["Sector"] = sector
        if industry:
            active_filters["Industry"] = industry
        if exchange:
            active_filters["Exchange"] = exchange
        if country:
            active_filters["Country"] = country
        if market_cap_more_than is not None:
            active_filters["Mkt Cap >"] = format_number(market_cap_more_than)
        if market_cap_lower_than is not None:
            active_filters["Mkt Cap <"] = format_number(market_cap_lower_than)
        if price_more_than is not None:
            active_filters["Price >"] = f"${price_more_than:.2f}"
        if price_lower_than is not None:
            active_filters["Price <"] = f"${price_lower_than:.2f}"
        if volume_more_than is not None:
            active_filters["Vol >"] = format_number(volume_more_than).replace("$", "")
        if volume_lower_than is not None:
            active_filters["Vol <"] = format_number(volume_lower_than).replace("$", "")
        if beta_more_than is not None:
            active_filters["Beta >"] = f"{beta_more_than:.2f}"
        if beta_lower_than is not None:
            active_filters["Beta <"] = f"{beta_lower_than:.2f}"
        if dividend_more_than is not None:
            active_filters["Dividend >"] = f"{dividend_more_than:.2f}%"
        if dividend_lower_than is not None:
            active_filters["Dividend <"] = f"{dividend_lower_than:.2f}%"

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = []
        lines.append(f"## Stock Screener Results ({len(results)} stocks)")
        lines.append(f"**Retrieved:** {timestamp}")
        lines.append("")

        if active_filters:
            filter_parts = [f"{k}: {v}" for k, v in active_filters.items()]
            lines.append(f"**Filters:** {' | '.join(filter_parts)}")
            lines.append("")

        # Results table
        lines.append("| Symbol | Name | Price | Mkt Cap | Sector | Beta | Volume | Change% |")
        lines.append("|--------|------|-------|---------|--------|------|--------|---------|")

        for stock in results:
            sym = stock.get("symbol", "N/A")
            name = stock.get("companyName", "N/A")
            if len(name) > 25:
                name = name[:22] + "..."
            price = stock.get("price")
            mkt_cap = stock.get("marketCap")
            sect = stock.get("sector", "N/A")
            beta = stock.get("beta")
            volume = stock.get("volume")
            change = stock.get("changes")

            price_str = f"${price:.2f}" if price is not None else "N/A"
            cap_str = format_number(mkt_cap) if mkt_cap is not None else "N/A"
            beta_str = f"{beta:.2f}" if beta is not None else "N/A"
            vol_str = format_number(volume).replace("$", "") if volume is not None else "N/A"
            if change is not None:
                sign = "+" if change >= 0 else ""
                change_str = f"{sign}{change:.2f}%"
            else:
                change_str = "N/A"

            lines.append(f"| {sym} | {name} | {price_str} | {cap_str} | {sect} | {beta_str} | {vol_str} | {change_str} |")

        content = "\n".join(lines)

        artifact = {
            "type": "stock_screener",
            "results": results,
            "filters": active_filters,
            "count": len(results),
        }

        return content, artifact

    except Exception as e:
        logger.error(f"Error in stock screener: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        error_content = f"## Stock Screener\n**Retrieved:** {timestamp}\n**Status:** Error\n\nError screening stocks: {str(e)}"
        return error_content, {"type": "stock_screener", "results": [], "filters": {}, "count": 0}


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
                strike_str = f"${strike:.2f}" if strike is not None else "N/A"
                expiry = c.get("expiration_date", "N/A")

                # Merge snapshot session data
                snap = snapshot_map.get(ticker, {})
                session = snap.get("session", {})
                close_val = session.get("close")
                close_str = f"${close_val:.2f}" if close_val is not None else "—"
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
                price_str = f"${price:.2f}" if price is not None else "N/A"
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
